window.BENCHMARK_DATA = {
  "lastUpdate": 1767657109974,
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
      }
    ]
  }
}