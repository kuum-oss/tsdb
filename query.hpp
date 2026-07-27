#pragma once
#include <string>
#include <vector>
#include <variant>
#include <chrono>
#include <algorithm>
#include <cmath>
#include <concepts>
#include <sstream>
#include <iostream>
#include <iomanip>

enum class AggregationType {
    AVG,
    MAX,
    MIN,
    RATE,
    PERCENTILE
};

struct Aggregation {
    AggregationType type;
    double percentile_val = 0.0;
};

struct QueryAST {
    Aggregation agg;
    std::string metric;
    std::string host_filter; // "*" or specific host name
    bool is_relative = true;
    uint64_t last_seconds = 0;
    uint64_t start_time = 0;
    uint64_t end_time = 0;
    uint64_t step_seconds = 0; // 0 if none
};

// Result pattern wrapping parsed AST or error string
struct ParseError {
    std::string message;
};

using ParseResult = std::variant<QueryAST, ParseError>;

// Concept checking that a range has arithmetic elements
template<typename T>
concept NumericRange = requires(T t) {
    typename T::value_type;
    { t.begin() } -> std::input_or_output_iterator;
    { t.end() } -> std::input_or_output_iterator;
} && std::is_arithmetic_v<typename T::value_type>;

template<NumericRange R>
double aggregate_avg(const R& range) {
    if (range.empty()) return 0.0;
    double sum = 0.0;
    for (auto val : range) sum += val;
    return sum / range.size();
}

template<NumericRange R>
double aggregate_max(const R& range) {
    if (range.empty()) return 0.0;
    double m = range[0];
    for (auto val : range) {
        if (val > m) m = val;
    }
    return m;
}

template<NumericRange R>
double aggregate_min(const R& range) {
    if (range.empty()) return 0.0;
    double m = range[0];
    for (auto val : range) {
        if (val < m) m = val;
    }
    return m;
}

template<NumericRange R>
double aggregate_percentile(const R& range, double p) {
    if (range.empty()) return 0.0;
    std::vector<double> copy(range.begin(), range.end());
    std::sort(copy.begin(), copy.end());
    double rank = (p / 100.0) * (copy.size() - 1);
    size_t low = static_cast<size_t>(std::floor(rank));
    size_t high = static_cast<size_t>(std::ceil(rank));
    if (high >= copy.size()) high = copy.size() - 1;
    if (low == high) return copy[low];
    double weight = rank - low;
    return copy[low] * (1.0 - weight) + copy[high] * weight;
}

template<NumericRange R>
double aggregate_rate(const R& range) {
    if (range.size() < 2) return 0.0;
    return range.back() - range.front();
}

inline uint64_t parse_date(const std::string& s) {
    int y, m, d;
    if (sscanf(s.c_str(), "%d-%d-%d", &y, &m, &d) == 3) {
        std::tm tm = {};
        tm.tm_year = y - 1900;
        tm.tm_mon = m - 1;
        tm.tm_mday = d;
        tm.tm_isdst = -1;
        std::time_t t = std::mktime(&tm);
        if (t == -1) return 0;
        return static_cast<uint64_t>(t);
    }
    return 0;
}

inline uint64_t parse_duration(const std::string& s) {
    if (s.empty()) return 0;
    char unit = s.back();
    uint64_t val = 0;
    try {
        val = std::stoull(s.substr(0, s.size() - 1));
    } catch (...) {
        return 0;
    }
    if (unit == 's') return val;
    if (unit == 'm') return val * 60;
    if (unit == 'h') return val * 3600;
    if (unit == 'd') return val * 86400;
    return val;
}

// Handwritten parser for TSDB query language
class QueryParser {
public:
    static ParseResult parse(const std::string& query_str) {
        std::vector<std::string> tokens = tokenize(query_str);
        if (tokens.empty()) return ParseError{"Empty query"};
        
        size_t idx = 0;
        if (tokens[idx] != "SELECT") return ParseError{"Expected SELECT at start"};
        idx++;

        if (idx >= tokens.size()) return ParseError{"Expected aggregation function"};
        
        std::string agg_fn = tokens[idx];
        Aggregation agg;
        if (agg_fn.rfind("avg", 0) == 0) {
            agg.type = AggregationType::AVG;
        } else if (agg_fn.rfind("max", 0) == 0) {
            agg.type = AggregationType::MAX;
        } else if (agg_fn.rfind("min", 0) == 0) {
            agg.type = AggregationType::MIN;
        } else if (agg_fn.rfind("rate", 0) == 0) {
            agg.type = AggregationType::RATE;
        } else if (agg_fn.rfind("percentile", 0) == 0) {
            agg.type = AggregationType::PERCENTILE;
            // parse percentile(95)
            size_t op = agg_fn.find('(');
            size_t cl = agg_fn.find(')');
            if (op == std::string::npos || cl == std::string::npos || cl <= op + 1) {
                return ParseError{"Invalid percentile definition"};
            }
            try {
                agg.percentile_val = std::stod(agg_fn.substr(op + 1, cl - op - 1));
            } catch (...) {
                return ParseError{"Invalid percentile value"};
            }
        } else {
            return ParseError{"Unknown aggregation function: " + agg_fn};
        }

        // Now extract metric name from aggregation argument (or it could be separate, but task says SELECT avg(cpu.usage))
        // So we look for the parentheses
        size_t op = agg_fn.find('(');
        size_t cl = agg_fn.find(')');
        std::string metric;
        if (op != std::string::npos && cl != std::string::npos) {
            metric = agg_fn.substr(op + 1, cl - op - 1);
        } else {
            // Check next tokens
            if (idx + 1 < tokens.size() && tokens[idx+1] == "(") {
                idx += 2;
                if (idx >= tokens.size()) return ParseError{"Expected metric name"};
                metric = tokens[idx];
                idx++;
                if (idx >= tokens.size() || tokens[idx] != ")") return ParseError{"Expected ')'"};
            } else {
                return ParseError{"Expected metric name in parentheses"};
            }
        }
        idx++;

        if (idx >= tokens.size() || tokens[idx] != "WHERE") return ParseError{"Expected WHERE"};
        idx++;

        if (idx >= tokens.size() || tokens[idx] != "host") return ParseError{"Expected host filter"};
        idx++;

        if (idx >= tokens.size() || tokens[idx] != "=") return ParseError{"Expected '=' after host"};
        idx++;

        if (idx >= tokens.size()) return ParseError{"Expected host filter value"};
        std::string host_filter = tokens[idx];
        if (host_filter.size() >= 2 && host_filter.front() == '"' && host_filter.back() == '"') {
            host_filter = host_filter.substr(1, host_filter.size() - 2);
        }
        idx++;

        if (idx >= tokens.size() || tokens[idx] != "OVER") return ParseError{"Expected OVER"};
        idx++;

        QueryAST ast;
        ast.agg = agg;
        ast.metric = metric;
        ast.host_filter = host_filter;

        if (idx >= tokens.size()) return ParseError{"Expected range or last duration"};
        if (tokens[idx] == "last") {
            ast.is_relative = true;
            idx++;
            if (idx >= tokens.size()) return ParseError{"Expected duration after 'last'"};
            ast.last_seconds = parse_duration(tokens[idx]);
            idx++;
        } else if (tokens[idx] == "range") {
            ast.is_relative = false;
            idx++;
            if (idx >= tokens.size()) return ParseError{"Expected start date"};
            std::string r = tokens[idx];
            size_t dots = r.find("..");
            if (dots == std::string::npos) {
                return ParseError{"Expected '..' in range"};
            }
            ast.start_time = parse_date(r.substr(0, dots));
            ast.end_time = parse_date(r.substr(dots + 2));
            idx++;
        } else {
            return ParseError{"Expected 'last' or 'range'"};
        }

        if (idx < tokens.size() && tokens[idx] == "STEP") {
            idx++;
            if (idx >= tokens.size()) return ParseError{"Expected duration after STEP"};
            ast.step_seconds = parse_duration(tokens[idx]);
            idx++;
        }

        return ast;
    }

private:
    static std::vector<std::string> tokenize(const std::string& query) {
        std::vector<std::string> tokens;
        std::string current;
        bool in_quotes = false;
        
        for (size_t i = 0; i < query.size(); ++i) {
            char c = query[i];
            if (c == '"') {
                in_quotes = !in_quotes;
                current += c;
            } else if (in_quotes) {
                current += c;
            } else if (std::isspace(c)) {
                if (!current.empty()) {
                    tokens.push_back(current);
                    current.clear();
                }
            } else if (c == '(' || c == ')' || c == '=') {
                if (!current.empty()) {
                    tokens.push_back(current);
                    current.clear();
                }
                tokens.push_back(std::string(1, c));
            } else {
                current += c;
            }
        }
        if (!current.empty()) {
            tokens.push_back(current);
        }
        return tokens;
    }
};
