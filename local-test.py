#!/usr/bin/env python3

from tasks import redis_benchmark

redis_benchmark(6379, "stock-arch", req=1000)
