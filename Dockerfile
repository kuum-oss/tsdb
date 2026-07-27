FROM ubuntu:24.04 AS builder
RUN apt-get update && apt-get install -y \
    gcc-14 g++-14 liburing-dev

WORKDIR /app
COPY . .

RUN mkdir -p build && \
    g++-14 -std=c++20 -O3 -DTSDB_HAS_URING=1 main.cpp storage.cpp network.cpp -luring -lpthread -o build/tsdb && \
    g++-14 -std=c++20 -O3 cli.cpp -lpthread -o build/tsdb-cli

FROM ubuntu:24.04
RUN apt-get update && apt-get install -y liburing2
COPY --from=builder /app/build/tsdb /app/build/tsdb-cli ./

