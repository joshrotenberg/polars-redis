# Changelog

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
