window.BENCHMARK_DATA = {
  "lastUpdate": 1767820142695,
  "repoUrl": "https://github.com/joshrotenberg/polars-redis",
  "entries": {
    "Rust Benchmarks": [
      {
        "commit": {
          "author": {
            "email": "joshrotenberg@gmail.com",
            "name": "Josh Rotenberg",
            "username": "joshrotenberg"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "4b6ef46272581345c18cbd48373d8403dbe65a71",
          "message": "feat: Polars-like query builder for RediSearch predicate pushdown (#62)\n\n* feat: add Polars-like query builder for RediSearch predicate pushdown\n\nAdd a col() interface that translates Polars-like filter expressions\nto RediSearch query syntax, enabling automatic predicate pushdown.\n\nPython API:\n  from polars_redis import col, search_hashes\n\n  # Instead of raw RediSearch syntax\n  lf = search_hashes(\n      url, index='users_idx',\n      query=(col('age') > 30) & (col('status') == 'active'),\n      schema={...}\n  )\n\nSupported operations:\n- Comparisons: >, >=, <, <=, ==, !=\n- Combinators: & (AND), | (OR)\n- Range: col('x').is_between(a, b)\n- Membership: col('x').is_in([a, b, c])\n- Escape hatch: raw('@field:query')\n\nNew files:\n- src/query_builder.rs: Rust Predicate/PredicateBuilder (15 tests)\n- python/polars_redis/query.py: Python col()/raw() interface\n\nWIP for #49\n\n* feat: expand query builder with full RediSearch syntax support\n\n- Add negation support (negate() method)\n- Add full-text search (TextSearch, contains)\n- Add prefix/suffix matching (Prefix, Suffix)\n- Add wildcard matching (Wildcard, matches)\n- Add fuzzy matching with configurable distance (Fuzzy, fuzzy)\n- Add phrase search (Phrase, phrase)\n- Add tag operations (Tag, TagOr, has_tag, has_any_tag)\n- Add geo radius queries (GeoRadius, within_radius)\n- Add null checks (IsMissing, IsNotMissing, is_null, is_not_null)\n- Add score boosting (Boost, boost)\n- Add escape functions for TAG and TEXT values\n- Comprehensive tests for all operations (27 Rust tests)\n- Python query builder with Polars-like syntax\n\n* chore: re-trigger CI after gh-pages creation",
          "timestamp": "2026-01-04T14:49:24-08:00",
          "tree_id": "f146411aa68b7f6daa68841c54abc9b454434ad4",
          "url": "https://github.com/joshrotenberg/polars-redis/commit/4b6ef46272581345c18cbd48373d8403dbe65a71"
        },
        "date": 1767567575567,
        "tool": "cargo",
        "benches": [
          {
            "name": "schema_creation/small_3_fields",
            "value": 324,
            "range": "± 11",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/medium_10_fields",
            "value": 1084,
            "range": "± 14",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/large_50_fields",
            "value": 7018,
            "range": "± 17",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/default",
            "value": 27,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/with_options",
            "value": 27,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/100",
            "value": 251,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/100",
            "value": 1215,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/100",
            "value": 84,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/1000",
            "value": 2778,
            "range": "± 146",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/1000",
            "value": 12778,
            "range": "± 32",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/1000",
            "value": 787,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/10000",
            "value": 34031,
            "range": "± 196",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/10000",
            "value": 134446,
            "range": "± 2636",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/10000",
            "value": 7582,
            "range": "± 232",
            "unit": "ns/iter"
          },
          {
            "name": "arrow_schema/to_arrow_schema",
            "value": 755,
            "range": "± 17",
            "unit": "ns/iter"
          },
          {
            "name": "projection/no_filter",
            "value": 19,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "projection/5_of_50_fields",
            "value": 901,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "projection/25_of_50_fields",
            "value": 1109,
            "range": "± 8",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "joshrotenberg@gmail.com",
            "name": "Josh Rotenberg",
            "username": "joshrotenberg"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "4611e414842b044ad15aebe4c8ef71ccfcbd0221",
          "message": "feat: Add ParallelStrategy for concurrent batch processing (#63)",
          "timestamp": "2026-01-04T15:20:31-08:00",
          "tree_id": "7385ef5b293dca5ce1ea111063e61999b6375adb",
          "url": "https://github.com/joshrotenberg/polars-redis/commit/4611e414842b044ad15aebe4c8ef71ccfcbd0221"
        },
        "date": 1767569054103,
        "tool": "cargo",
        "benches": [
          {
            "name": "schema_creation/small_3_fields",
            "value": 321,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/medium_10_fields",
            "value": 1089,
            "range": "± 25",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/large_50_fields",
            "value": 7015,
            "range": "± 31",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/default",
            "value": 28,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/with_options",
            "value": 29,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/100",
            "value": 248,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/100",
            "value": 1191,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/100",
            "value": 98,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/1000",
            "value": 2773,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/1000",
            "value": 12786,
            "range": "± 289",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/1000",
            "value": 939,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/10000",
            "value": 31291,
            "range": "± 258",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/10000",
            "value": 129152,
            "range": "± 1829",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/10000",
            "value": 9487,
            "range": "± 552",
            "unit": "ns/iter"
          },
          {
            "name": "arrow_schema/to_arrow_schema",
            "value": 803,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "projection/no_filter",
            "value": 20,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "projection/5_of_50_fields",
            "value": 940,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "projection/25_of_50_fields",
            "value": 1132,
            "range": "± 3",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "41898282+github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "113900d012991a1e5bde63b20a9660b4de0d080a",
          "message": "chore(master): release polars-redis 0.1.4 (#38)\n\nCo-authored-by: github-actions[bot] <41898282+github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2026-01-04T15:36:52-08:00",
          "tree_id": "65ccc1f4398b2d6df6490d71c53ea4cf6296640c",
          "url": "https://github.com/joshrotenberg/polars-redis/commit/113900d012991a1e5bde63b20a9660b4de0d080a"
        },
        "date": 1767570029648,
        "tool": "cargo",
        "benches": [
          {
            "name": "schema_creation/small_3_fields",
            "value": 326,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/medium_10_fields",
            "value": 1097,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/large_50_fields",
            "value": 7047,
            "range": "± 54",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/default",
            "value": 28,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/with_options",
            "value": 28,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/100",
            "value": 249,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/100",
            "value": 1187,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/100",
            "value": 98,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/1000",
            "value": 3057,
            "range": "± 12",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/1000",
            "value": 12520,
            "range": "± 112",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/1000",
            "value": 785,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/10000",
            "value": 36920,
            "range": "± 160",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/10000",
            "value": 128614,
            "range": "± 651",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/10000",
            "value": 7999,
            "range": "± 726",
            "unit": "ns/iter"
          },
          {
            "name": "arrow_schema/to_arrow_schema",
            "value": 748,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "projection/no_filter",
            "value": 19,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "projection/5_of_50_fields",
            "value": 949,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "projection/25_of_50_fields",
            "value": 1125,
            "range": "± 5",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "joshrotenberg@gmail.com",
            "name": "Josh Rotenberg",
            "username": "joshrotenberg"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "f8f79c13c5c4f83140413f3446d81ce11798ac33",
          "message": "feat: add Redis Cluster support (#65)\n\n* docs: Update documentation for RediSearch and parallel features\n\n- Add RediSearch guide with search_hashes, aggregate_hashes, query builder\n- Add parallel parameter documentation to scanning and configuration guides\n- Update README and index with new features\n- Add search_example.py to Python examples\n- Fix slides.md link and add direct URL fallback\n- Update mkdocs.yml navigation\n\n* feat: add Redis Cluster support\n\nAdd comprehensive Redis Cluster support with cluster-aware iterators\nfor all data types (Hash, JSON, String, Set, List, ZSet).\n\nKey changes:\n- Add cluster.rs with DirectClusterKeyScanner for cluster-wide SCAN\n- Extend connection.rs with RedisConn enum and ConnectionConfig\n- Add ClusterXxxBatchIterator for each data type\n- Add cluster fetch functions to each reader module\n- Add Python bindings for cluster hash, json, string iterators\n- Support redis+cluster:// URL scheme for auto-detection\n\nThe implementation uses DirectClusterKeyScanner to iterate SCAN across\nall master nodes (since cluster SCAN only returns keys from one node),\nand ClusterConnection for data fetching (which routes automatically).\n\nAll cluster code is behind the 'cluster' feature flag.\n\nCloses #40\n\n* test: add cluster integration tests using docker-wrapper\n\n- Add template-redis-cluster feature to docker-wrapper dev dependency\n- Create integration_cluster.rs with comprehensive cluster tests\n- Test hash scanning across cluster nodes\n- Test string scanning across cluster nodes\n- Tests use RedisClusterTemplate for Docker-based cluster setup",
          "timestamp": "2026-01-04T20:10:17-08:00",
          "tree_id": "97d133055960193b8760a08983860e3050c52225",
          "url": "https://github.com/joshrotenberg/polars-redis/commit/f8f79c13c5c4f83140413f3446d81ce11798ac33"
        },
        "date": 1767586439170,
        "tool": "cargo",
        "benches": [
          {
            "name": "schema_creation/small_3_fields",
            "value": 326,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/medium_10_fields",
            "value": 1097,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/large_50_fields",
            "value": 6921,
            "range": "± 29",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/default",
            "value": 27,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/with_options",
            "value": 29,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/100",
            "value": 248,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/100",
            "value": 1216,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/100",
            "value": 71,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/1000",
            "value": 2778,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/1000",
            "value": 12790,
            "range": "± 65",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/1000",
            "value": 672,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/10000",
            "value": 31251,
            "range": "± 205",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/10000",
            "value": 134811,
            "range": "± 349",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/10000",
            "value": 7326,
            "range": "± 49",
            "unit": "ns/iter"
          },
          {
            "name": "arrow_schema/to_arrow_schema",
            "value": 766,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "projection/no_filter",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "projection/5_of_50_fields",
            "value": 934,
            "range": "± 11",
            "unit": "ns/iter"
          },
          {
            "name": "projection/25_of_50_fields",
            "value": 1100,
            "range": "± 3",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "joshrotenberg@gmail.com",
            "name": "Josh Rotenberg",
            "username": "joshrotenberg"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "03d73f05eca794b018e841edefa55c295c2e8893",
          "message": "feat: add per-key error reporting for write operations (#73)",
          "timestamp": "2026-01-04T22:08:02-08:00",
          "tree_id": "7722272855847fc894762c5c93fdd5b6af08146d",
          "url": "https://github.com/joshrotenberg/polars-redis/commit/03d73f05eca794b018e841edefa55c295c2e8893"
        },
        "date": 1767593505818,
        "tool": "cargo",
        "benches": [
          {
            "name": "schema_creation/small_3_fields",
            "value": 343,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/medium_10_fields",
            "value": 1127,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/large_50_fields",
            "value": 6978,
            "range": "± 20",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/default",
            "value": 28,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/with_options",
            "value": 29,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/100",
            "value": 249,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/100",
            "value": 1160,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/100",
            "value": 71,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/1000",
            "value": 2775,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/1000",
            "value": 12268,
            "range": "± 56",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/1000",
            "value": 674,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/10000",
            "value": 31285,
            "range": "± 189",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/10000",
            "value": 125725,
            "range": "± 378",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/10000",
            "value": 7246,
            "range": "± 39",
            "unit": "ns/iter"
          },
          {
            "name": "arrow_schema/to_arrow_schema",
            "value": 765,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "projection/no_filter",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "projection/5_of_50_fields",
            "value": 918,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "projection/25_of_50_fields",
            "value": 1135,
            "range": "± 4",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "joshrotenberg@gmail.com",
            "name": "Josh Rotenberg",
            "username": "joshrotenberg"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "bb90c0280852d4cf079b560bb574d92dac0df20e",
          "message": "feat: add schema inference confidence scores (#74)",
          "timestamp": "2026-01-04T22:09:06-08:00",
          "tree_id": "d48151491368deea0a4171e812680f1b4afe2fd1",
          "url": "https://github.com/joshrotenberg/polars-redis/commit/bb90c0280852d4cf079b560bb574d92dac0df20e"
        },
        "date": 1767593563019,
        "tool": "cargo",
        "benches": [
          {
            "name": "schema_creation/small_3_fields",
            "value": 323,
            "range": "± 9",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/medium_10_fields",
            "value": 1279,
            "range": "± 10",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/large_50_fields",
            "value": 8010,
            "range": "± 25",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/default",
            "value": 27,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/with_options",
            "value": 28,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/100",
            "value": 249,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/100",
            "value": 1181,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/100",
            "value": 64,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/1000",
            "value": 2776,
            "range": "± 36",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/1000",
            "value": 12792,
            "range": "± 207",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/1000",
            "value": 722,
            "range": "± 25",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/10000",
            "value": 31183,
            "range": "± 138",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/10000",
            "value": 134668,
            "range": "± 2931",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/10000",
            "value": 6907,
            "range": "± 202",
            "unit": "ns/iter"
          },
          {
            "name": "arrow_schema/to_arrow_schema",
            "value": 734,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "projection/no_filter",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "projection/5_of_50_fields",
            "value": 928,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "projection/25_of_50_fields",
            "value": 1147,
            "range": "± 3",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "joshrotenberg@gmail.com",
            "name": "Josh Rotenberg",
            "username": "joshrotenberg"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "e8d14c40d5b47897cf58ea00f8b0af21111f4e9b",
          "message": "feat: add TTL support for String type reads (#88) (#90)\n\n- Add ttl field to StringData struct\n- Add fetch_ttls function for pipelined TTL fetching\n- Update fetch_strings to support include_ttl parameter\n- Add include_ttl and ttl_column_name to StringSchema\n- Update strings_to_record_batch for TTL column\n- Update StringBatchIterator and ClusterStringBatchIterator\n- Add include_ttl and ttl_column_name to Python bindings\n- Update scan_strings, read_strings, and StringScanOptions\n- Add Python tests for string TTL reads",
          "timestamp": "2026-01-05T14:03:21-08:00",
          "tree_id": "68af5e3d9de37d439ec0d328bf53acd6c293e503",
          "url": "https://github.com/joshrotenberg/polars-redis/commit/e8d14c40d5b47897cf58ea00f8b0af21111f4e9b"
        },
        "date": 1767650842156,
        "tool": "cargo",
        "benches": [
          {
            "name": "schema_creation/small_3_fields",
            "value": 307,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/medium_10_fields",
            "value": 1211,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/large_50_fields",
            "value": 7138,
            "range": "± 20",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/default",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/with_options",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/100",
            "value": 231,
            "range": "± 11",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/100",
            "value": 1118,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/100",
            "value": 95,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/1000",
            "value": 2857,
            "range": "± 79",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/1000",
            "value": 11646,
            "range": "± 134",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/1000",
            "value": 970,
            "range": "± 10",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/10000",
            "value": 36699,
            "range": "± 106",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/10000",
            "value": 122439,
            "range": "± 514",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/10000",
            "value": 9637,
            "range": "± 63",
            "unit": "ns/iter"
          },
          {
            "name": "arrow_schema/to_arrow_schema",
            "value": 845,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "projection/no_filter",
            "value": 18,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "projection/5_of_50_fields",
            "value": 870,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "projection/25_of_50_fields",
            "value": 1008,
            "range": "± 5",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "joshrotenberg@gmail.com",
            "name": "Josh Rotenberg",
            "username": "joshrotenberg"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "13150e894a4ab3e97c798921ad14fba354a9026b",
          "message": "feat: add cluster support for Stream and TimeSeries types (#94)\n\n* feat: add cluster support for Stream and TimeSeries types\n\n- Add ClusterStreamBatchIterator for scanning Redis Streams across cluster nodes\n- Add ClusterTimeSeriesBatchIterator for scanning RedisTimeSeries across cluster nodes\n- Both iterators follow the same pattern as existing cluster iterators\n- Support all existing options (start/end IDs, count, aggregation for timeseries)\n\nCloses #89\n\n* chore: update criterion to 0.8 and docker-wrapper to 0.10\n\n* fix: use std::hint::black_box instead of deprecated criterion::black_box",
          "timestamp": "2026-01-05T15:41:56-08:00",
          "tree_id": "0f93b77e1e917c5dcc4d297afc026fd42dbfdef3",
          "url": "https://github.com/joshrotenberg/polars-redis/commit/13150e894a4ab3e97c798921ad14fba354a9026b"
        },
        "date": 1767657109138,
        "tool": "cargo",
        "benches": [
          {
            "name": "schema_creation/small_3_fields",
            "value": 318,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/medium_10_fields",
            "value": 1087,
            "range": "± 14",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/large_50_fields",
            "value": 7007,
            "range": "± 28",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/default",
            "value": 26,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/with_options",
            "value": 27,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/100",
            "value": 252,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/100",
            "value": 1211,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/100",
            "value": 76,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/1000",
            "value": 3192,
            "range": "± 33",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/1000",
            "value": 12503,
            "range": "± 67",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/1000",
            "value": 713,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/10000",
            "value": 40039,
            "range": "± 171",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/10000",
            "value": 137958,
            "range": "± 1587",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/10000",
            "value": 7358,
            "range": "± 61",
            "unit": "ns/iter"
          },
          {
            "name": "arrow_schema/to_arrow_schema",
            "value": 757,
            "range": "± 8",
            "unit": "ns/iter"
          },
          {
            "name": "projection/no_filter",
            "value": 20,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "projection/5_of_50_fields",
            "value": 930,
            "range": "± 11",
            "unit": "ns/iter"
          },
          {
            "name": "projection/25_of_50_fields",
            "value": 1095,
            "range": "± 3",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "joshrotenberg@gmail.com",
            "name": "Josh Rotenberg",
            "username": "joshrotenberg"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "eca251e3a4c2d40e00c004847ef0438587d18d05",
          "message": "feat: add missing RediSearch query features to query builder (#96)\n\nRust query_builder.rs:\n- Add optional terms support (~query syntax)\n- Add infix/substring matching (*substring*)\n- Add exact wildcard matching (w'pattern')\n- Add phrase with slop and inorder attributes\n- Add multi-field search (@field1|field2:term)\n- Add geo polygon search (WITHIN $poly)\n- Add vector KNN search (*=>[KNN k @field $vec])\n- Add vector range search (VECTOR_RANGE)\n- Add get_params() method for PARAMS extraction\n\nPython query.py:\n- Add contains_substring() for infix matching\n- Add matches_exact() for exact wildcard\n- Add slop/inorder params to phrase()\n- Add optional() method for optional terms\n- Add within_polygon() for geo polygon search\n- Add knn() and vector_range() for vector search\n- Add cols() function for multi-field search\n- Add MultiFieldExpr class\n\nCloses #93",
          "timestamp": "2026-01-05T16:25:40-08:00",
          "tree_id": "6b46afdf04cebd979d97ddd45b531572b6830e1b",
          "url": "https://github.com/joshrotenberg/polars-redis/commit/eca251e3a4c2d40e00c004847ef0438587d18d05"
        },
        "date": 1767659717352,
        "tool": "cargo",
        "benches": [
          {
            "name": "schema_creation/small_3_fields",
            "value": 326,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/medium_10_fields",
            "value": 1079,
            "range": "± 8",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/large_50_fields",
            "value": 7048,
            "range": "± 35",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/default",
            "value": 27,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/with_options",
            "value": 27,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/100",
            "value": 253,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/100",
            "value": 1180,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/100",
            "value": 79,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/1000",
            "value": 3359,
            "range": "± 72",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/1000",
            "value": 12585,
            "range": "± 335",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/1000",
            "value": 785,
            "range": "± 9",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/10000",
            "value": 37204,
            "range": "± 558",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/10000",
            "value": 128737,
            "range": "± 682",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/10000",
            "value": 8191,
            "range": "± 289",
            "unit": "ns/iter"
          },
          {
            "name": "arrow_schema/to_arrow_schema",
            "value": 733,
            "range": "± 13",
            "unit": "ns/iter"
          },
          {
            "name": "projection/no_filter",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "projection/5_of_50_fields",
            "value": 963,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "projection/25_of_50_fields",
            "value": 1105,
            "range": "± 6",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "joshrotenberg@gmail.com",
            "name": "Josh Rotenberg",
            "username": "joshrotenberg"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c677c329e0f48cafd4680005c0242ced650d5e2c",
          "message": "test: mark batch iterator tests as ignored (#98) (#109)\n\nThese tests require a running Redis instance and were failing in\nenvironments without Redis available. Mark them with #[ignore] so\nthey can be run explicitly with 'cargo test -- --ignored' when\nRedis is available.\n\nAffected tests:\n- test_hash_batch_iterator_creation\n- test_json_batch_iterator_creation\n- test_string_batch_iterator_creation\n- test_string_batch_iterator_with_int64\n- test_set_batch_iterator_creation\n- test_set_batch_iterator_with_options\n- test_list_batch_iterator_creation\n- test_list_batch_iterator_with_options\n- test_zset_batch_iterator_creation\n- test_zset_batch_iterator_with_options\n- test_stream_batch_iterator_creation\n- test_stream_batch_iterator_with_options\n- test_timeseries_batch_iterator_creation\n- test_timeseries_batch_iterator_with_options\n- test_hash_search_iterator_creation\n\nCloses #98",
          "timestamp": "2026-01-05T17:47:53-08:00",
          "tree_id": "5ebb001b08f2b6138d30baad75e5c586cfd8d804",
          "url": "https://github.com/joshrotenberg/polars-redis/commit/c677c329e0f48cafd4680005c0242ced650d5e2c"
        },
        "date": 1767664654819,
        "tool": "cargo",
        "benches": [
          {
            "name": "schema_creation/small_3_fields",
            "value": 314,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/medium_10_fields",
            "value": 1086,
            "range": "± 15",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/large_50_fields",
            "value": 7152,
            "range": "± 48",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/default",
            "value": 27,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/with_options",
            "value": 27,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/100",
            "value": 249,
            "range": "± 11",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/100",
            "value": 1191,
            "range": "± 8",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/100",
            "value": 99,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/1000",
            "value": 3120,
            "range": "± 75",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/1000",
            "value": 12465,
            "range": "± 102",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/1000",
            "value": 938,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/10000",
            "value": 38352,
            "range": "± 267",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/10000",
            "value": 129210,
            "range": "± 422",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/10000",
            "value": 9489,
            "range": "± 59",
            "unit": "ns/iter"
          },
          {
            "name": "arrow_schema/to_arrow_schema",
            "value": 771,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "projection/no_filter",
            "value": 20,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "projection/5_of_50_fields",
            "value": 933,
            "range": "± 19",
            "unit": "ns/iter"
          },
          {
            "name": "projection/25_of_50_fields",
            "value": 1145,
            "range": "± 4",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "joshrotenberg@gmail.com",
            "name": "Josh Rotenberg",
            "username": "joshrotenberg"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "94060e5722d4ab9c5ccfbe4bce725292fcafca4c",
          "message": "feat: add docker-wrapper ContainerGuard for CI integration tests (#114)\n\n- Add testing feature to docker-wrapper dependency\n- Add redis_guard() async function using ContainerGuard for automatic\n  Redis container lifecycle management\n- Create integration_with_container.rs with 14 async tests that run\n  without #[ignore] by auto-starting Redis containers\n- Use spawn_blocking to handle runtime nesting with sync polars-redis APIs\n- Tests cover: scanning, projection, max_rows, TTL, row index, type\n  conversion, rows_yielded tracking, sparse data, batch iteration,\n  and write operations (basic, TTL, append, fail modes)",
          "timestamp": "2026-01-05T18:41:13-08:00",
          "tree_id": "ad993ebbc5a1bef7dcb27e38d71b6c613c7a8279",
          "url": "https://github.com/joshrotenberg/polars-redis/commit/94060e5722d4ab9c5ccfbe4bce725292fcafca4c"
        },
        "date": 1767667858621,
        "tool": "cargo",
        "benches": [
          {
            "name": "schema_creation/small_3_fields",
            "value": 315,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/medium_10_fields",
            "value": 1081,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/large_50_fields",
            "value": 6960,
            "range": "± 112",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/default",
            "value": 27,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/with_options",
            "value": 27,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/100",
            "value": 250,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/100",
            "value": 1181,
            "range": "± 12",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/100",
            "value": 77,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/1000",
            "value": 3363,
            "range": "± 9",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/1000",
            "value": 12465,
            "range": "± 29",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/1000",
            "value": 714,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/10000",
            "value": 38392,
            "range": "± 3487",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/10000",
            "value": 129262,
            "range": "± 335",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/10000",
            "value": 7601,
            "range": "± 312",
            "unit": "ns/iter"
          },
          {
            "name": "arrow_schema/to_arrow_schema",
            "value": 733,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "projection/no_filter",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "projection/5_of_50_fields",
            "value": 919,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "projection/25_of_50_fields",
            "value": 1153,
            "range": "± 4",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "41898282+github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c2e97df2fd3e66e4b6588521287b7665460dba9c",
          "message": "chore(master): release polars-redis 0.1.5 (#66)",
          "timestamp": "2026-01-05T19:02:30-08:00",
          "tree_id": "7e3bb14cec106ad7e596e747aaa40d97c384179d",
          "url": "https://github.com/joshrotenberg/polars-redis/commit/c2e97df2fd3e66e4b6588521287b7665460dba9c"
        },
        "date": 1767669125859,
        "tool": "cargo",
        "benches": [
          {
            "name": "schema_creation/small_3_fields",
            "value": 317,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/medium_10_fields",
            "value": 1091,
            "range": "± 9",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/large_50_fields",
            "value": 6929,
            "range": "± 35",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/default",
            "value": 26,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/with_options",
            "value": 27,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/100",
            "value": 249,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/100",
            "value": 1190,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/100",
            "value": 76,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/1000",
            "value": 3154,
            "range": "± 35",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/1000",
            "value": 12476,
            "range": "± 21",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/1000",
            "value": 716,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/10000",
            "value": 38667,
            "range": "± 421",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/10000",
            "value": 129285,
            "range": "± 1069",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/10000",
            "value": 7521,
            "range": "± 97",
            "unit": "ns/iter"
          },
          {
            "name": "arrow_schema/to_arrow_schema",
            "value": 753,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "projection/no_filter",
            "value": 20,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "projection/5_of_50_fields",
            "value": 966,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "projection/25_of_50_fields",
            "value": 1139,
            "range": "± 6",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "joshrotenberg@gmail.com",
            "name": "Josh Rotenberg",
            "username": "joshrotenberg"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "6da0bc77f863f564bd9526258d357b0dad34f456",
          "message": "docs: add API documentation for issues #99-103 (#116)",
          "timestamp": "2026-01-05T22:50:21-08:00",
          "tree_id": "34f37df15f2e11b076ffb22e2040b65321aa9406",
          "url": "https://github.com/joshrotenberg/polars-redis/commit/6da0bc77f863f564bd9526258d357b0dad34f456"
        },
        "date": 1767682798536,
        "tool": "cargo",
        "benches": [
          {
            "name": "schema_creation/small_3_fields",
            "value": 319,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/medium_10_fields",
            "value": 1077,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/large_50_fields",
            "value": 6985,
            "range": "± 26",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/default",
            "value": 26,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/with_options",
            "value": 27,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/100",
            "value": 249,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/100",
            "value": 1189,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/100",
            "value": 76,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/1000",
            "value": 3120,
            "range": "± 96",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/1000",
            "value": 12457,
            "range": "± 43",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/1000",
            "value": 713,
            "range": "± 16",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/10000",
            "value": 37184,
            "range": "± 948",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/10000",
            "value": 129612,
            "range": "± 1184",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/10000",
            "value": 7658,
            "range": "± 496",
            "unit": "ns/iter"
          },
          {
            "name": "arrow_schema/to_arrow_schema",
            "value": 858,
            "range": "± 10",
            "unit": "ns/iter"
          },
          {
            "name": "projection/no_filter",
            "value": 20,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "projection/5_of_50_fields",
            "value": 936,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "projection/25_of_50_fields",
            "value": 1129,
            "range": "± 22",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "joshrotenberg@gmail.com",
            "name": "Josh Rotenberg",
            "username": "joshrotenberg"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "ad6cf7c461d3598164ddd448de3ee7cd7942c42e",
          "message": "feat: add DataFrame caching with Arrow IPC and Parquet support (#123)\n\n* feat: add DataFrame caching with Arrow IPC and Parquet support\n\nAdd functions for caching entire DataFrames in Redis using Arrow IPC\nor Parquet format. This enables using Redis as a high-performance\ndistributed cache for intermediate computation results.\n\nNew functions:\n- cache_dataframe(): Store DataFrame with optional compression and TTL\n- get_cached_dataframe(): Retrieve cached DataFrame\n- scan_cached(): Retrieve as LazyFrame\n- delete_cached(): Remove cached DataFrame\n- cache_exists(): Check if cache key exists\n- cache_ttl(): Get remaining TTL\n\nSupports two formats:\n- Arrow IPC: Fast serialization, zero-copy potential (default)\n- Parquet: Better compression, storage efficiency\n\nCloses #118, closes #122\n\n* feat: add chunked storage and comprehensive tests for DataFrame caching\n\n- Add chunked storage for large DataFrames (default 100MB chunks)\n- Add cache_info() function to inspect cached data\n- Add chunk_size_mb parameter to control chunking behavior\n- Fix chunk size calculation to use integer bytes\n- Add comprehensive test suite (33 tests) covering:\n  - Basic IPC and Parquet caching\n  - All compression options (lz4, zstd, snappy, gzip)\n  - TTL functionality\n  - Chunked storage and retrieval\n  - Cache operations (exists, delete, info, scan)\n  - Various data types (ints, floats, bools, strings, dates, lists, nulls)\n  - Parquet projection pushdown (columns, n_rows)\n  - Error handling for invalid inputs\n- Update documentation with chunking section\n\n* feat: add Rust cache module for RecordBatch caching parity\n\n- Add src/cache.rs with full Arrow IPC and Parquet support\n- Support chunked storage for large datasets (default 100MB chunks)\n- Add compression options: IPC (lz4, zstd), Parquet (snappy, lz4, zstd)\n- Add TTL support for cache expiration\n- Add cache_info() for inspecting cached data\n- Add parquet and bytes dependencies to Cargo.toml\n- Enable ipc_compression feature in arrow\n- Update documentation with Rust API examples\n- Ensures feature parity between Python and Rust APIs",
          "timestamp": "2026-01-06T10:14:35-08:00",
          "tree_id": "d59250e6f2f2c53f1bb17c30dd2b49f2dd9c7dfc",
          "url": "https://github.com/joshrotenberg/polars-redis/commit/ad6cf7c461d3598164ddd448de3ee7cd7942c42e"
        },
        "date": 1767723881156,
        "tool": "cargo",
        "benches": [
          {
            "name": "schema_creation/small_3_fields",
            "value": 321,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/medium_10_fields",
            "value": 1085,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/large_50_fields",
            "value": 6867,
            "range": "± 29",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/default",
            "value": 26,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/with_options",
            "value": 27,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/100",
            "value": 250,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/100",
            "value": 1220,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/100",
            "value": 76,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/1000",
            "value": 3099,
            "range": "± 39",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/1000",
            "value": 12533,
            "range": "± 104",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/1000",
            "value": 712,
            "range": "± 13",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/10000",
            "value": 40130,
            "range": "± 573",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/10000",
            "value": 138202,
            "range": "± 4213",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/10000",
            "value": 7420,
            "range": "± 86",
            "unit": "ns/iter"
          },
          {
            "name": "arrow_schema/to_arrow_schema",
            "value": 769,
            "range": "± 9",
            "unit": "ns/iter"
          },
          {
            "name": "projection/no_filter",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "projection/5_of_50_fields",
            "value": 918,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "projection/25_of_50_fields",
            "value": 1123,
            "range": "± 17",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "joshrotenberg@gmail.com",
            "name": "Josh Rotenberg",
            "username": "joshrotenberg"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "09f0cf27b8f6eafa590c9e2261ce745ea7e394e1",
          "message": "feat: add Pub/Sub DataFrame streaming (#121) (#126)\n\n* feat: add Pub/Sub DataFrame streaming (#121)\n\n- Add Rust pubsub module with message collection into RecordBatch\n- Add Python pubsub functions: collect_pubsub, subscribe_batches, iter_batches\n- Support termination by count, timeout, or time window\n- Support JSON message format with schema casting\n- Support custom message parsers\n- Support pattern subscriptions (PSUBSCRIBE)\n- Add metadata columns for channel and timestamp\n- Add comprehensive tests for pubsub functionality\n- Add documentation for Pub/Sub streaming\n\n* fix: add redis dependency and fix pubsub tests for CI\n\n- Add redis>=5.0 as a dependency in pyproject.toml\n- Add asyncio_mode=auto to pytest config for async tests\n- Simplify unit test to use real Redis with short timeout\n- Remove unused mock imports\n\n* fix: add pytest-asyncio to CI dependencies for async tests",
          "timestamp": "2026-01-06T13:22:05-08:00",
          "tree_id": "dafe048d9c60503fd72814f972694a9dec4fd7c6",
          "url": "https://github.com/joshrotenberg/polars-redis/commit/09f0cf27b8f6eafa590c9e2261ce745ea7e394e1"
        },
        "date": 1767735154274,
        "tool": "cargo",
        "benches": [
          {
            "name": "schema_creation/small_3_fields",
            "value": 321,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/medium_10_fields",
            "value": 1089,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/large_50_fields",
            "value": 6863,
            "range": "± 76",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/default",
            "value": 28,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/with_options",
            "value": 27,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/100",
            "value": 249,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/100",
            "value": 1187,
            "range": "± 43",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/100",
            "value": 76,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/1000",
            "value": 3116,
            "range": "± 30",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/1000",
            "value": 12498,
            "range": "± 231",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/1000",
            "value": 714,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/10000",
            "value": 38167,
            "range": "± 287",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/10000",
            "value": 134181,
            "range": "± 2807",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/10000",
            "value": 7485,
            "range": "± 48",
            "unit": "ns/iter"
          },
          {
            "name": "arrow_schema/to_arrow_schema",
            "value": 758,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "projection/no_filter",
            "value": 19,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "projection/5_of_50_fields",
            "value": 901,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "projection/25_of_50_fields",
            "value": 1106,
            "range": "± 5",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "41898282+github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c80dd81279a51c17a9d5d4196582e483d5a5f8f7",
          "message": "chore(main): release polars-redis 0.1.6 (#130)\n\nCo-authored-by: github-actions[bot] <41898282+github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2026-01-06T14:19:31-08:00",
          "tree_id": "51ada589062e53dbacef15d9c0914c366d2ec4fd",
          "url": "https://github.com/joshrotenberg/polars-redis/commit/c80dd81279a51c17a9d5d4196582e483d5a5f8f7"
        },
        "date": 1767738621208,
        "tool": "cargo",
        "benches": [
          {
            "name": "schema_creation/small_3_fields",
            "value": 323,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/medium_10_fields",
            "value": 1089,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/large_50_fields",
            "value": 6837,
            "range": "± 30",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/default",
            "value": 26,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/with_options",
            "value": 27,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/100",
            "value": 249,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/100",
            "value": 1211,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/100",
            "value": 79,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/1000",
            "value": 3124,
            "range": "± 27",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/1000",
            "value": 12546,
            "range": "± 20",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/1000",
            "value": 937,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/10000",
            "value": 38661,
            "range": "± 403",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/10000",
            "value": 135415,
            "range": "± 1350",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/10000",
            "value": 9428,
            "range": "± 309",
            "unit": "ns/iter"
          },
          {
            "name": "arrow_schema/to_arrow_schema",
            "value": 750,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "projection/no_filter",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "projection/5_of_50_fields",
            "value": 933,
            "range": "± 9",
            "unit": "ns/iter"
          },
          {
            "name": "projection/25_of_50_fields",
            "value": 1170,
            "range": "± 22",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "joshrotenberg@gmail.com",
            "name": "Josh Rotenberg",
            "username": "joshrotenberg"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "422086d745e6875f3099aefc8381eb3d07b8db48",
          "message": "feat: add RediSearch index management helpers (#141)",
          "timestamp": "2026-01-06T20:20:09-08:00",
          "tree_id": "fda76cc4d8e5bf113617a8127055dbb16c66c184",
          "url": "https://github.com/joshrotenberg/polars-redis/commit/422086d745e6875f3099aefc8381eb3d07b8db48"
        },
        "date": 1767760196953,
        "tool": "cargo",
        "benches": [
          {
            "name": "schema_creation/small_3_fields",
            "value": 314,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/medium_10_fields",
            "value": 1095,
            "range": "± 22",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/large_50_fields",
            "value": 7102,
            "range": "± 27",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/default",
            "value": 26,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/with_options",
            "value": 27,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/100",
            "value": 249,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/100",
            "value": 1195,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/100",
            "value": 79,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/1000",
            "value": 3099,
            "range": "± 24",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/1000",
            "value": 12492,
            "range": "± 39",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/1000",
            "value": 938,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/10000",
            "value": 38223,
            "range": "± 229",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/10000",
            "value": 128453,
            "range": "± 446",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/10000",
            "value": 9540,
            "range": "± 67",
            "unit": "ns/iter"
          },
          {
            "name": "arrow_schema/to_arrow_schema",
            "value": 771,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "projection/no_filter",
            "value": 20,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "projection/5_of_50_fields",
            "value": 936,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "projection/25_of_50_fields",
            "value": 1127,
            "range": "± 6",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "joshrotenberg@gmail.com",
            "name": "Josh Rotenberg",
            "username": "joshrotenberg"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "12abe96a40805d170b15af224776a009a5f3b2d8",
          "message": "feat: complete RediSearch coverage with enhanced search features (#143)",
          "timestamp": "2026-01-06T22:01:16-08:00",
          "tree_id": "21246791556f2ce2855ab47c993196ec26ae61d9",
          "url": "https://github.com/joshrotenberg/polars-redis/commit/12abe96a40805d170b15af224776a009a5f3b2d8"
        },
        "date": 1767766263349,
        "tool": "cargo",
        "benches": [
          {
            "name": "schema_creation/small_3_fields",
            "value": 314,
            "range": "± 12",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/medium_10_fields",
            "value": 1063,
            "range": "± 29",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/large_50_fields",
            "value": 6701,
            "range": "± 196",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/default",
            "value": 26,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/with_options",
            "value": 26,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/100",
            "value": 236,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/100",
            "value": 1145,
            "range": "± 24",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/100",
            "value": 72,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/1000",
            "value": 3025,
            "range": "± 73",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/1000",
            "value": 12091,
            "range": "± 275",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/1000",
            "value": 674,
            "range": "± 15",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/10000",
            "value": 36001,
            "range": "± 1005",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/10000",
            "value": 126750,
            "range": "± 2956",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/10000",
            "value": 7665,
            "range": "± 189",
            "unit": "ns/iter"
          },
          {
            "name": "arrow_schema/to_arrow_schema",
            "value": 713,
            "range": "± 17",
            "unit": "ns/iter"
          },
          {
            "name": "projection/no_filter",
            "value": 20,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "projection/5_of_50_fields",
            "value": 896,
            "range": "± 20",
            "unit": "ns/iter"
          },
          {
            "name": "projection/25_of_50_fields",
            "value": 1068,
            "range": "± 12",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "joshrotenberg@gmail.com",
            "name": "Josh Rotenberg",
            "username": "joshrotenberg"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "354ba0bbe264cf1f7370e35c0bf9d37b02db40f1",
          "message": "feat: add smart_scan with automatic index detection (#144)\n\n* feat: add smart_scan with automatic index detection (#133)\n\n- Add smart_scan() that auto-detects RediSearch indexes for key patterns\n- Add explain_scan() to show query execution plan before running\n- Add find_index_for_pattern() and list_indexes() for index discovery\n- Add ExecutionStrategy enum (SEARCH, SCAN, HYBRID)\n- Add QueryPlan with explain() method for transparency\n- Add DetectedIndex dataclass for index metadata\n- Implement graceful degradation when RediSearch unavailable\n- Add Rust parity with smart module\n- Add comprehensive unit tests\n- Update documentation with Smart Scan section\n\n* fix: add warning for no index and avoid predicate pushdown in filter test\n\n* chore: register integration pytest marker",
          "timestamp": "2026-01-07T08:06:30-08:00",
          "tree_id": "484f7863655e9bd11b1f510255d3449581e392fe",
          "url": "https://github.com/joshrotenberg/polars-redis/commit/354ba0bbe264cf1f7370e35c0bf9d37b02db40f1"
        },
        "date": 1767802574736,
        "tool": "cargo",
        "benches": [
          {
            "name": "schema_creation/small_3_fields",
            "value": 314,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/medium_10_fields",
            "value": 1079,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/large_50_fields",
            "value": 6880,
            "range": "± 34",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/default",
            "value": 26,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/with_options",
            "value": 27,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/100",
            "value": 249,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/100",
            "value": 1208,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/100",
            "value": 76,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/1000",
            "value": 3149,
            "range": "± 37",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/1000",
            "value": 12770,
            "range": "± 40",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/1000",
            "value": 714,
            "range": "± 8",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/10000",
            "value": 37858,
            "range": "± 125",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/10000",
            "value": 134321,
            "range": "± 1642",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/10000",
            "value": 7466,
            "range": "± 172",
            "unit": "ns/iter"
          },
          {
            "name": "arrow_schema/to_arrow_schema",
            "value": 758,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "projection/no_filter",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "projection/5_of_50_fields",
            "value": 977,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "projection/25_of_50_fields",
            "value": 1149,
            "range": "± 5",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "joshrotenberg@gmail.com",
            "name": "Josh Rotenberg",
            "username": "joshrotenberg"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "d1d8b1a15b027fe2bdd261345c03eaceb9dbe307",
          "message": "test: add pushdown equivalence tests (#136) (#145)\n\n* test: add pushdown equivalence tests (#136)\n\nAdd comprehensive integration tests verifying that predicate pushdown\nto RediSearch produces identical results as client-side filtering.\n\nTest coverage includes:\n- Simple predicates (>, >=, <, <=, ==, !=, between)\n- Compound predicates (AND, OR, nested)\n- Result edge cases (no matches, all match, single match)\n- Numeric edge cases (boundary values, float precision, zero)\n- Text/tag edge cases\n- Projection tests\n- Data edge cases (nulls, unicode, special characters)\n\n* test: add property tests for query_builder and schema modules\n\nAdd comprehensive property-based tests using proptest for:\n\nquery_builder module (20 property tests):\n- Numeric equality, greater-than, less-than predicate formats\n- Between predicates containing both bounds\n- Tag escaping for special characters\n- AND/OR predicate composition\n- NOT wrapping in negation\n- Float value preservation\n- Fuzzy distance clamping (1-3)\n- PredicateBuilder joining behavior\n- Geo radius format validation\n- Vector KNN format validation\n- TagOr pipe separation\n- Boost weight wrapping\n- Multi-field search format\n\nschema module (17 property tests):\n- Int64 and Float64 round-trip parsing\n- Utf8 always succeeds\n- Boolean true/false variant parsing\n- Date format parsing (YYYY-MM-DD and epoch days)\n- DateTime Unix timestamp parsing (seconds and millis)\n- days_since_epoch monotonicity\n- HashSchema field order preservation\n- Arrow schema field count calculation\n- Projection subset behavior\n- RedisType to Arrow type determinism\n- TypedValue preservation\n\n* fix: correct datetime milliseconds property test range\n\nThe property test was using values that fell into the 'seconds' range\nof the parse_datetime heuristic. Values must be >= 10^10 to be detected\nas milliseconds. Updated the test to generate values in the valid\nmillisecond range (10^10 to 2*10^12).\n\n* fix: wait for RediSearch index to finish indexing in tests\n\nThe pushdown equivalence tests were failing because RediSearch indexes\ndocuments asynchronously. After creating an index, we need to wait for\nall documents to be indexed before running queries.\n\nAdded polling loop that checks FT.INFO for 'indexing' status to ensure\nindex is fully built before proceeding with tests.\n\n* fix: wrap OR clauses in parentheses for RediSearch parsing\n\n- Fix Python query builder to wrap OR expressions in parentheses\n- RediSearch requires parentheses around OR clauses for proper parsing\n- Fix test_single_match assertion: both user:0 and user:50 have\n  age=20 and level=0, so 2 matches is correct not 1\n\n* fix: optimize tag OR queries to use RediSearch @field:{tag1|tag2} syntax\n\nRediSearch requires @field:{tag1|tag2} syntax for OR operations on tag\nfields. The general OR syntax (query1 | query2) causes syntax errors.\n\nAdded _try_optimize_tag_or() to detect and optimize OR trees of tag\nequality checks on the same field into the proper RediSearch syntax.\n\nExamples:\n- (col('status') == 'a') | (col('status') == 'b') -> @status:{a|b}\n- Three or more values also work: @status:{a|b|c}\n- Different fields still use parenthesized OR\n- Numeric ORs correctly use range syntax",
          "timestamp": "2026-01-07T10:17:35-08:00",
          "tree_id": "37bb681f5df5554650003ce1c7a620f807e91b14",
          "url": "https://github.com/joshrotenberg/polars-redis/commit/d1d8b1a15b027fe2bdd261345c03eaceb9dbe307"
        },
        "date": 1767810441288,
        "tool": "cargo",
        "benches": [
          {
            "name": "schema_creation/small_3_fields",
            "value": 319,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/medium_10_fields",
            "value": 1084,
            "range": "± 11",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/large_50_fields",
            "value": 6916,
            "range": "± 19",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/default",
            "value": 27,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/with_options",
            "value": 27,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/100",
            "value": 249,
            "range": "± 15",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/100",
            "value": 1216,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/100",
            "value": 76,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/1000",
            "value": 3093,
            "range": "± 31",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/1000",
            "value": 12761,
            "range": "± 42",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/1000",
            "value": 785,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/10000",
            "value": 38767,
            "range": "± 501",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/10000",
            "value": 134422,
            "range": "± 279",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/10000",
            "value": 8061,
            "range": "± 99",
            "unit": "ns/iter"
          },
          {
            "name": "arrow_schema/to_arrow_schema",
            "value": 733,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "projection/no_filter",
            "value": 19,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "projection/5_of_50_fields",
            "value": 956,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "projection/25_of_50_fields",
            "value": 1085,
            "range": "± 2",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "joshrotenberg@gmail.com",
            "name": "Josh Rotenberg",
            "username": "joshrotenberg"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "4a658660ae5ef812c74eb74be07d31f49afb2696",
          "message": "perf: expand benchmark coverage for caching and streaming (#160)\n\n* test: add streaming semantics tests\n\nAdd comprehensive tests for Redis Streams and Pub/Sub semantics:\n- Message ordering verification (3 tests)\n- Delivery semantics (at-least-once, at-most-once) (3 tests)\n- Timeout behavior testing (3 tests)\n- Consumer group semantics (XGROUP, XREADGROUP, XCLAIM) (4 tests)\n- Multi-channel Pub/Sub with pattern subscriptions (2 tests)\n- Streaming edge cases (empty streams, large messages) (5 tests)\n\nCloses #152\n\n* perf: expand benchmark coverage for caching and streaming\n\nAdd comprehensive benchmarks for new features:\n\nCache Operations:\n- IPC write with varying row counts (1K, 10K, 100K)\n- IPC compression comparison (uncompressed, LZ4, Zstd)\n- Parquet compression comparison (uncompressed, Snappy, Zstd)\n- Cache read throughput by row count\n- Format comparison (IPC vs Parquet)\n- Chunked caching with varying chunk sizes\n\nStreaming Operations:\n- Stream write (XADD) throughput\n- Stream read (XRANGE) throughput\n\nLatency Tracking:\n- Individual operation latencies for p50/p95/p99 analysis\n- Small and medium batch write latencies\n- Cache read latencies\n- Single XADD latencies\n\nAlso includes baseline performance expectations documentation\nand updates CI workflow with notes about Redis-dependent benchmarks.\n\nCloses #150",
          "timestamp": "2026-01-07T12:00:56-08:00",
          "tree_id": "15d6056751a543806ec7861ec1d50184d885b789",
          "url": "https://github.com/joshrotenberg/polars-redis/commit/4a658660ae5ef812c74eb74be07d31f49afb2696"
        },
        "date": 1767816637941,
        "tool": "cargo",
        "benches": [
          {
            "name": "schema_creation/small_3_fields",
            "value": 320,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/medium_10_fields",
            "value": 1079,
            "range": "± 9",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/large_50_fields",
            "value": 6796,
            "range": "± 23",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/default",
            "value": 26,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/with_options",
            "value": 27,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/100",
            "value": 248,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/100",
            "value": 1207,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/100",
            "value": 76,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/1000",
            "value": 3103,
            "range": "± 29",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/1000",
            "value": 12775,
            "range": "± 45",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/1000",
            "value": 722,
            "range": "± 35",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/10000",
            "value": 38495,
            "range": "± 225",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/10000",
            "value": 134936,
            "range": "± 410",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/10000",
            "value": 8159,
            "range": "± 27",
            "unit": "ns/iter"
          },
          {
            "name": "arrow_schema/to_arrow_schema",
            "value": 753,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "projection/no_filter",
            "value": 19,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "projection/5_of_50_fields",
            "value": 933,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "projection/25_of_50_fields",
            "value": 1113,
            "range": "± 6",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "joshrotenberg@gmail.com",
            "name": "Josh Rotenberg",
            "username": "joshrotenberg"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "344506ae24a690ec23a27a29ae8e988aaf8e605b",
          "message": "chore: align with Polars conventions (#166)\n\n- Add rustfmt.toml with Polars-compatible settings\n  - match_block_trailing_comma = true\n  - use_field_init_shorthand = true\n  - Document nightly-only settings (group_imports, imports_granularity)\n\n- Add Clippy lint configuration\n  - Allow collapsible_if to match Polars workspace settings\n\n- Apply rustfmt changes across codebase\n\nCloses #163\nCloses #165",
          "timestamp": "2026-01-07T12:29:42-08:00",
          "tree_id": "b14276aab5162c9ef9dc2567af37dbf9e105d883",
          "url": "https://github.com/joshrotenberg/polars-redis/commit/344506ae24a690ec23a27a29ae8e988aaf8e605b"
        },
        "date": 1767818372196,
        "tool": "cargo",
        "benches": [
          {
            "name": "schema_creation/small_3_fields",
            "value": 316,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/medium_10_fields",
            "value": 1090,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/large_50_fields",
            "value": 7042,
            "range": "± 162",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/default",
            "value": 26,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/with_options",
            "value": 27,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/100",
            "value": 249,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/100",
            "value": 1216,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/100",
            "value": 76,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/1000",
            "value": 3095,
            "range": "± 63",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/1000",
            "value": 12777,
            "range": "± 20",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/1000",
            "value": 716,
            "range": "± 34",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/10000",
            "value": 38242,
            "range": "± 161",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/10000",
            "value": 135182,
            "range": "± 1428",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/10000",
            "value": 7991,
            "range": "± 298",
            "unit": "ns/iter"
          },
          {
            "name": "arrow_schema/to_arrow_schema",
            "value": 760,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "projection/no_filter",
            "value": 19,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "projection/5_of_50_fields",
            "value": 933,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "projection/25_of_50_fields",
            "value": 1161,
            "range": "± 2",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "joshrotenberg@gmail.com",
            "name": "Josh Rotenberg",
            "username": "joshrotenberg"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "637f742bea90b490dac50ebed59bf8d5be95142e",
          "message": "feat: add pipeline and transaction support (#174)\n\nAdd Pipeline and Transaction types for efficient batched Redis operations\nwith DataFrame ergonomics and Rust/Python API parity.\n\n## Rust API\n\n- Pipeline: batch multiple commands, execute in single round-trip\n- Transaction: atomic operations with MULTI/EXEC\n- CommandResult: typed result enum for command responses\n- PipelineResult: aggregate results with success/failure counts\n\nCommands supported:\n- String: set, set_ex, get, mget, incr, incrby, decr\n- Hash: hset, hmset, hget, hgetall, hdel, hincrby\n- List: lpush, rpush, lrange, llen\n- Set: sadd, smembers, sismember, scard\n- Sorted Set: zadd, zrange, zscore, zcard\n- Key: del, exists, expire, ttl, rename, type\n- Raw: execute any command with raw()\n\n## Python API\n\n- Pipeline class with fluent interface\n- Transaction class for atomic operations\n- PipelineResult and CommandResult wrappers\n- Full parity with Rust API\n\nCloses #168",
          "timestamp": "2026-01-07T12:58:49-08:00",
          "tree_id": "cfba03bc2c346b760c631c2a4b38aeedfb29ee8c",
          "url": "https://github.com/joshrotenberg/polars-redis/commit/637f742bea90b490dac50ebed59bf8d5be95142e"
        },
        "date": 1767820141781,
        "tool": "cargo",
        "benches": [
          {
            "name": "schema_creation/small_3_fields",
            "value": 320,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/medium_10_fields",
            "value": 1092,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "schema_creation/large_50_fields",
            "value": 6941,
            "range": "± 43",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/default",
            "value": 27,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "batch_config/with_options",
            "value": 27,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/100",
            "value": 249,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/100",
            "value": 1221,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/100",
            "value": 98,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/1000",
            "value": 3254,
            "range": "± 32",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/1000",
            "value": 12529,
            "range": "± 92",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/1000",
            "value": 808,
            "range": "± 77",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/int64/10000",
            "value": 38117,
            "range": "± 488",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/float64/10000",
            "value": 138146,
            "range": "± 1470",
            "unit": "ns/iter"
          },
          {
            "name": "type_parsing/boolean/10000",
            "value": 9435,
            "range": "± 32",
            "unit": "ns/iter"
          },
          {
            "name": "arrow_schema/to_arrow_schema",
            "value": 732,
            "range": "± 17",
            "unit": "ns/iter"
          },
          {
            "name": "projection/no_filter",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "projection/5_of_50_fields",
            "value": 953,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "projection/25_of_50_fields",
            "value": 1117,
            "range": "± 7",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}