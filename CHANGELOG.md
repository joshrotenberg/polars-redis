# Changelog

## [0.1.6](https://github.com/joshrotenberg/polars-redis/compare/polars-redis-v0.1.5...polars-redis-v0.1.6) (2026-01-06)


### Features

* add DataFrame caching decorator ([#119](https://github.com/joshrotenberg/polars-redis/issues/119)) ([#125](https://github.com/joshrotenberg/polars-redis/issues/125)) ([8a54423](https://github.com/joshrotenberg/polars-redis/commit/8a5442320acf0a54e6a5bfb89a66139b2180656e))
* add DataFrame caching with Arrow IPC and Parquet support ([#123](https://github.com/joshrotenberg/polars-redis/issues/123)) ([ad6cf7c](https://github.com/joshrotenberg/polars-redis/commit/ad6cf7c461d3598164ddd448de3ee7cd7942c42e))
* add Pub/Sub DataFrame streaming ([#121](https://github.com/joshrotenberg/polars-redis/issues/121)) ([#126](https://github.com/joshrotenberg/polars-redis/issues/126)) ([09f0cf2](https://github.com/joshrotenberg/polars-redis/commit/09f0cf27b8f6eafa590c9e2261ce745ea7e394e1))
* add Redis Streams streaming source ([#120](https://github.com/joshrotenberg/polars-redis/issues/120)) ([#127](https://github.com/joshrotenberg/polars-redis/issues/127)) ([e06a725](https://github.com/joshrotenberg/polars-redis/commit/e06a725a4a65426bf9a4bcf94396e74f11773bd2))

## [0.1.5](https://github.com/joshrotenberg/polars-redis/compare/polars-redis-v0.1.4...polars-redis-v0.1.5) (2026-01-06)


### Features

* add cluster support for Stream and TimeSeries types ([#94](https://github.com/joshrotenberg/polars-redis/issues/94)) ([13150e8](https://github.com/joshrotenberg/polars-redis/commit/13150e894a4ab3e97c798921ad14fba354a9026b))
* add docker-wrapper ContainerGuard for CI integration tests ([#114](https://github.com/joshrotenberg/polars-redis/issues/114)) ([94060e5](https://github.com/joshrotenberg/polars-redis/commit/94060e5722d4ab9c5ccfbe4bce725292fcafca4c))
* add missing RediSearch query features to query builder ([#96](https://github.com/joshrotenberg/polars-redis/issues/96)) ([eca251e](https://github.com/joshrotenberg/polars-redis/commit/eca251e3a4c2d40e00c004847ef0438587d18d05)), closes [#93](https://github.com/joshrotenberg/polars-redis/issues/93)
* add per-key error reporting for write operations ([#73](https://github.com/joshrotenberg/polars-redis/issues/73)) ([03d73f0](https://github.com/joshrotenberg/polars-redis/commit/03d73f05eca794b018e841edefa55c295c2e8893))
* add Python API for Set, List, ZSet, Stream, and TimeSeries types ([#95](https://github.com/joshrotenberg/polars-redis/issues/95)) ([0b61b72](https://github.com/joshrotenberg/polars-redis/commit/0b61b72ab57a00c4991d431523892e6da06dc7de))
* add Redis Cluster support ([#65](https://github.com/joshrotenberg/polars-redis/issues/65)) ([f8f79c1](https://github.com/joshrotenberg/polars-redis/commit/f8f79c13c5c4f83140413f3446d81ce11798ac33))
* add schema inference confidence scores ([#74](https://github.com/joshrotenberg/polars-redis/issues/74)) ([bb90c02](https://github.com/joshrotenberg/polars-redis/commit/bb90c0280852d4cf079b560bb574d92dac0df20e))
* add search_json() and aggregate_json() for RediSearch on JSON documents ([#92](https://github.com/joshrotenberg/polars-redis/issues/92)) ([2f69e63](https://github.com/joshrotenberg/polars-redis/commit/2f69e633263d5d2d21d9c7c334693e803799b00d)), closes [#86](https://github.com/joshrotenberg/polars-redis/issues/86)
* add TTL support for String type reads ([#88](https://github.com/joshrotenberg/polars-redis/issues/88)) ([#90](https://github.com/joshrotenberg/polars-redis/issues/90)) ([e8d14c4](https://github.com/joshrotenberg/polars-redis/commit/e8d14c40d5b47897cf58ea00f8b0af21111f4e9b))
* add write support for Set, List, and Sorted Set types ([#87](https://github.com/joshrotenberg/polars-redis/issues/87)) ([#91](https://github.com/joshrotenberg/polars-redis/issues/91)) ([edd8e04](https://github.com/joshrotenberg/polars-redis/commit/edd8e041798a0a12751b3f1a05decc91f6ffc58e))

## [0.1.4](https://github.com/joshrotenberg/polars-redis/compare/polars-redis-v0.1.3...polars-redis-v0.1.4) (2026-01-04)


### Features

* add connection pooling with ConnectionManager ([#37](https://github.com/joshrotenberg/polars-redis/issues/37)) ([78c820f](https://github.com/joshrotenberg/polars-redis/commit/78c820f17e0db872bbc261a29467d06a7c426649))
* add FT.AGGREGATE support for RediSearch ([#53](https://github.com/joshrotenberg/polars-redis/issues/53)) ([41367c3](https://github.com/joshrotenberg/polars-redis/commit/41367c37e270e642058bef231b6b8328e38f6899)), closes [#41](https://github.com/joshrotenberg/polars-redis/issues/41)
* Add ParallelStrategy for concurrent batch processing ([#63](https://github.com/joshrotenberg/polars-redis/issues/63)) ([4611e41](https://github.com/joshrotenberg/polars-redis/commit/4611e414842b044ad15aebe4c8ef71ccfcbd0221))
* add Python options classes and update scan functions ([#48](https://github.com/joshrotenberg/polars-redis/issues/48)) ([ae8db8a](https://github.com/joshrotenberg/polars-redis/commit/ae8db8a995f038ab7ff4057e9ee96f258f2f9bad))
* add RediSearch predicate pushdown support ([#39](https://github.com/joshrotenberg/polars-redis/issues/39)) ([e02992a](https://github.com/joshrotenberg/polars-redis/commit/e02992aa08f508f9a75bef1b42432b1a29c5a643))
* add schema_overwrite support for inference functions ([#51](https://github.com/joshrotenberg/polars-redis/issues/51)) ([e300f56](https://github.com/joshrotenberg/polars-redis/commit/e300f5698e381dd1d98898b8a60e8cfdfc926724))
* Polars-like query builder for RediSearch predicate pushdown ([#62](https://github.com/joshrotenberg/polars-redis/issues/62)) ([4b6ef46](https://github.com/joshrotenberg/polars-redis/commit/4b6ef46272581345c18cbd48373d8403dbe65a71))

## [0.1.3](https://github.com/joshrotenberg/polars-redis/compare/polars-redis-v0.1.2...polars-redis-v0.1.3) (2026-01-04)


### Bug Fixes

* update macOS runner and simplify wheel builds ([#34](https://github.com/joshrotenberg/polars-redis/issues/34)) ([9aa9e0d](https://github.com/joshrotenberg/polars-redis/commit/9aa9e0d41f888b44346ea1f061f4f1d0ecabc794))
* use macos-14 for Intel cross-compile and make crates.io resilient ([#36](https://github.com/joshrotenberg/polars-redis/issues/36)) ([2fa0bea](https://github.com/joshrotenberg/polars-redis/commit/2fa0bea77618e74cc495d757d82bef1984e3ff9b))

## [0.1.2](https://github.com/joshrotenberg/polars-redis/compare/polars-redis-v0.1.1...polars-redis-v0.1.2) (2026-01-03)


### Bug Fixes

* add workflow_dispatch trigger to publish workflow ([#31](https://github.com/joshrotenberg/polars-redis/issues/31)) ([7380f8d](https://github.com/joshrotenberg/polars-redis/commit/7380f8d473a6ae722fdef4a6de77a432a6d92e65))

## [0.1.1](https://github.com/joshrotenberg/polars-redis/compare/polars-redis-v0.1.0...polars-redis-v0.1.1) (2026-01-03)


### Features

* add Date and Datetime Polars type support ([0d49064](https://github.com/joshrotenberg/polars-redis/commit/0d490641f7b2ec04c61308dd8a966f6ef15322d3))
* add key_prefix parameter to write functions ([#14](https://github.com/joshrotenberg/polars-redis/issues/14)) ([58c58b8](https://github.com/joshrotenberg/polars-redis/commit/58c58b8bd2889a14bcb30efa799802b6ffae0098))
* add Phase 2 enhancements - TTL, row index, and better errors ([b7b1a08](https://github.com/joshrotenberg/polars-redis/commit/b7b1a081ec29452191eae89ab0b3f1e85a909193))
* add read_hashes() and read_json() eager functions ([5a02d3d](https://github.com/joshrotenberg/polars-redis/commit/5a02d3dec2e102232fad9bcede487ec2f4b5fb23))
* add Redis Sets, Lists, and Sorted Sets support ([#23](https://github.com/joshrotenberg/polars-redis/issues/23)) ([902048b](https://github.com/joshrotenberg/polars-redis/commit/902048b407ef9215a94d380bdd257096767d0d12))
* add Redis Streams and RedisTimeSeries support ([#24](https://github.com/joshrotenberg/polars-redis/issues/24)) ([4ef38fb](https://github.com/joshrotenberg/polars-redis/commit/4ef38fbabe5a7e2f5978f91f06da7df779bb8543))
* add row index key generation for write functions ([#17](https://github.com/joshrotenberg/polars-redis/issues/17)) ([13efcce](https://github.com/joshrotenberg/polars-redis/commit/13efccec48f651cd9a2269e93b11b4bd69934ffc))
* add scan_strings for reading Redis string values ([#9](https://github.com/joshrotenberg/polars-redis/issues/9)) ([6612d8c](https://github.com/joshrotenberg/polars-redis/commit/6612d8c314a96e10025ad415b658066ed2a6f106))
* add schema inference functions ([ea0cde2](https://github.com/joshrotenberg/polars-redis/commit/ea0cde22a9b5ce130706fc8ed6eb91009eb4ba46))
* add schema inference functions ([84c5a90](https://github.com/joshrotenberg/polars-redis/commit/84c5a90964a92c2b42d7b669fcd60f8ed5785f55))
* add TTL support to write operations ([#12](https://github.com/joshrotenberg/polars-redis/issues/12)) ([7c62772](https://github.com/joshrotenberg/polars-redis/commit/7c62772fc720c7a15d2ff437e82835295aa15abb))
* add write mode support (fail, replace, append) ([#13](https://github.com/joshrotenberg/polars-redis/issues/13)) ([53b377b](https://github.com/joshrotenberg/polars-redis/commit/53b377b6d976ef2d95548760756a8c1710b62112))
* add write support for hashes and JSON ([2914650](https://github.com/joshrotenberg/polars-redis/commit/29146508629ba65db43b7c02cc1280d0d1cc2057))
* add write support for hashes and JSON ([1109bf5](https://github.com/joshrotenberg/polars-redis/commit/1109bf587fb127766de635b3441eca723d6b7df1))
* add write_strings for writing Redis string values ([5260213](https://github.com/joshrotenberg/polars-redis/commit/526021367e2c508c39db3979904aad50ad3ee1f1))
* Phase 2 enhancements - TTL, row index, and better errors ([fcb1d11](https://github.com/joshrotenberg/polars-redis/commit/fcb1d11fc83f8ab905290ed2fe45190494dd1fab))


### Bug Fixes

* install redis-tools for redis-cli in CI ([a07976d](https://github.com/joshrotenberg/polars-redis/commit/a07976d64c35010bc3c5de86c9549fafc937649a))
* use config file for release-please ([#30](https://github.com/joshrotenberg/polars-redis/issues/30)) ([1e03ecd](https://github.com/joshrotenberg/polars-redis/commit/1e03ecd11312e57a426b8261c39351647ac30832))
* use correct dtolnay/rust-toolchain action name ([3e1d1fb](https://github.com/joshrotenberg/polars-redis/commit/3e1d1fb924633db40d44853b4eadd0403fdcb485))
* use maturin build instead of develop in CI ([2814ec3](https://github.com/joshrotenberg/polars-redis/commit/2814ec3a2ebf925fee9926fc94e78f4ad6166697))


### Performance Improvements

* add batch pipelining to write operations ([#16](https://github.com/joshrotenberg/polars-redis/issues/16)) ([b871432](https://github.com/joshrotenberg/polars-redis/commit/b87143281e088936281150a3da19c31e38ed2534))
