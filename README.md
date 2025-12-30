# HopperKV

![Build & Test](https://github.com/chenhao-ye/hopperkv/actions/workflows/cmake.yml/badge.svg)

HopperKV is a multi-tenant key-value store that extends Redis to cache data from DynamoDB. It features the *HARE* allocation algorithm, which holistically allocates cache capacity, DynamoDB read/write units, and network bandwidth to maximize throughput while guaranteeing fairness across tenants.

Please refer to [`ARTIFACT.md`](./ARTIFACT.md) for instructions to reproduce experiments.
