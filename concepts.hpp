#pragma once
#include <concepts>
#include <type_traits>
#include <vector>
#include <cstdint>
#include <cstddef>

template<typename T>
concept Aggregatable = std::is_arithmetic_v<T>;

template<typename T>
concept Serializable = requires(T t, std::vector<uint8_t>& buf, const uint8_t* ptr) {
    { t.serialize(buf) } -> std::same_as<void>;
    { t.deserialize(ptr, std::declval<size_t>()) } -> std::same_as<size_t>;
};
