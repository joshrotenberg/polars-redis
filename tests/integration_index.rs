//! Integration tests for RediSearch index management.
//!
//! These tests require a running Redis instance with RediSearch.
//! Run with: `cargo test --test integration_index --all-features`

use polars_redis::RedisType;
use polars_redis::index::{
    DistanceMetric, Field, GeoField, GeoShapeField, Index, IndexType, NumericField, TagField,
    TextField, VectorAlgorithm, VectorField,
};

mod common;
use common::{cleanup_keys, redis_available, redis_cli, redis_cli_output, redis_url};

/// Test TextField builder.
#[test]
fn test_text_field_builder() {
    let field = TextField::new("title")
        .sortable()
        .weight(2.0)
        .nostem()
        .phonetic("dm:en")
        .withsuffixtrie();

    let args = field.to_args();
    assert!(args.contains(&"TEXT".to_string()));
    assert!(args.contains(&"SORTABLE".to_string()));
    assert!(args.contains(&"WEIGHT".to_string()));
    assert!(args.contains(&"2".to_string()));
    assert!(args.contains(&"NOSTEM".to_string()));
    assert!(args.contains(&"PHONETIC".to_string()));
    assert!(args.contains(&"dm:en".to_string()));
    assert!(args.contains(&"WITHSUFFIXTRIE".to_string()));
}

/// Test NumericField builder.
#[test]
fn test_numeric_field_builder() {
    let field = NumericField::new("price").sortable();

    let args = field.to_args();
    assert_eq!(args, vec!["price", "NUMERIC", "SORTABLE"]);
}

/// Test TagField builder.
#[test]
fn test_tag_field_builder() {
    let field = TagField::new("categories")
        .separator("|")
        .casesensitive()
        .sortable()
        .withsuffixtrie();

    let args = field.to_args();
    assert!(args.contains(&"TAG".to_string()));
    assert!(args.contains(&"SEPARATOR".to_string()));
    assert!(args.contains(&"|".to_string()));
    assert!(args.contains(&"CASESENSITIVE".to_string()));
    assert!(args.contains(&"SORTABLE".to_string()));
    assert!(args.contains(&"WITHSUFFIXTRIE".to_string()));
}

/// Test GeoField builder.
#[test]
fn test_geo_field_builder() {
    let field = GeoField::new("location");
    let args = field.to_args();
    assert_eq!(args, vec!["location", "GEO"]);

    let field_noindex = GeoField::new("location").noindex();
    let args = field_noindex.to_args();
    assert!(args.contains(&"NOINDEX".to_string()));
}

/// Test GeoShapeField builder.
#[test]
fn test_geoshape_field_builder() {
    let field = GeoShapeField::new("boundary");
    let args = field.to_args();
    assert_eq!(args, vec!["boundary", "GEOSHAPE"]);

    let field_flat = GeoShapeField::new("boundary").coord_system("FLAT");
    let args = field_flat.to_args();
    assert!(args.contains(&"COORD_SYSTEM".to_string()));
    assert!(args.contains(&"FLAT".to_string()));
}

/// Test VectorField builder with HNSW.
#[test]
fn test_vector_field_hnsw() {
    let field = VectorField::new("embedding")
        .algorithm(VectorAlgorithm::Hnsw)
        .dim(768)
        .distance_metric(DistanceMetric::Cosine)
        .initial_cap(1000)
        .m(16)
        .ef_construction(200)
        .ef_runtime(10);

    let args = field.to_args();
    assert!(args.contains(&"VECTOR".to_string()));
    assert!(args.contains(&"HNSW".to_string()));
    assert!(args.contains(&"DIM".to_string()));
    assert!(args.contains(&"768".to_string()));
    assert!(args.contains(&"COSINE".to_string()));
    assert!(args.contains(&"M".to_string()));
    assert!(args.contains(&"16".to_string()));
}

/// Test VectorField builder with FLAT.
#[test]
fn test_vector_field_flat() {
    let field = VectorField::new("embedding")
        .algorithm(VectorAlgorithm::Flat)
        .dim(128)
        .distance_metric(DistanceMetric::L2)
        .block_size(1024);

    let args = field.to_args();
    assert!(args.contains(&"FLAT".to_string()));
    assert!(args.contains(&"L2".to_string()));
    assert!(args.contains(&"BLOCK_SIZE".to_string()));
}

/// Test Index builder.
#[test]
fn test_index_builder() {
    let index = Index::new("products_idx")
        .with_prefix("product:")
        .with_field(TextField::new("name").sortable())
        .with_field(NumericField::new("price").sortable())
        .with_field(TagField::new("category"));

    let cmd = index.to_command_string();
    assert!(cmd.contains("FT.CREATE products_idx"));
    assert!(cmd.contains("ON HASH"));
    assert!(cmd.contains("PREFIX 1 product:"));
    assert!(cmd.contains("SCHEMA"));
    assert!(cmd.contains("name TEXT SORTABLE"));
    assert!(cmd.contains("price NUMERIC SORTABLE"));
    assert!(cmd.contains("category TAG"));
}

/// Test Index with multiple prefixes.
#[test]
fn test_index_multiple_prefixes() {
    let index = Index::new("multi_idx")
        .with_prefixes(vec!["product:".to_string(), "item:".to_string()])
        .with_field(TextField::new("name"));

    let cmd = index.to_command_string();
    assert!(cmd.contains("PREFIX 2 product: item:"));
}

/// Test Index with JSON type.
#[test]
fn test_index_json_type() {
    let index = Index::new("json_idx")
        .with_type(IndexType::Json)
        .with_prefix("doc:")
        .with_field(TextField::new("$.title"));

    let cmd = index.to_command_string();
    assert!(cmd.contains("ON JSON"));
}

/// Test Index with stopwords.
#[test]
fn test_index_stopwords() {
    let index = Index::new("nostop_idx")
        .with_prefix("doc:")
        .without_stopwords()
        .with_field(TextField::new("content"));

    let cmd = index.to_command_string();
    assert!(cmd.contains("STOPWORDS 0"));
}

/// Test Index with custom stopwords.
#[test]
fn test_index_custom_stopwords() {
    let index = Index::new("custom_idx")
        .with_prefix("doc:")
        .with_stopwords(vec!["the".to_string(), "a".to_string()])
        .with_field(TextField::new("content"));

    let cmd = index.to_command_string();
    assert!(cmd.contains("STOPWORDS 2 the a"));
}

/// Test Index with language settings.
#[test]
fn test_index_language() {
    let index = Index::new("lang_idx")
        .with_prefix("doc:")
        .with_language("german")
        .with_language_field("lang")
        .with_field(TextField::new("content"));

    let cmd = index.to_command_string();
    assert!(cmd.contains("LANGUAGE german"));
    assert!(cmd.contains("LANGUAGE_FIELD lang"));
}

/// Test Index with optimization flags.
#[test]
fn test_index_optimization_flags() {
    let index = Index::new("opt_idx")
        .with_prefix("doc:")
        .maxtextfields()
        .nooffsets()
        .nohl()
        .nofields()
        .nofreqs()
        .skipinitialscan()
        .with_field(TextField::new("content"));

    let cmd = index.to_command_string();
    assert!(cmd.contains("MAXTEXTFIELDS"));
    assert!(cmd.contains("NOOFFSETS"));
    assert!(cmd.contains("NOHL"));
    assert!(cmd.contains("NOFIELDS"));
    assert!(cmd.contains("NOFREQS"));
    assert!(cmd.contains("SKIPINITIALSCAN"));
}

/// Test Index::from_schema.
#[test]
fn test_index_from_schema() {
    let schema = vec![
        ("name".to_string(), RedisType::Utf8),
        ("description".to_string(), RedisType::Utf8),
        ("price".to_string(), RedisType::Float64),
        ("quantity".to_string(), RedisType::Int64),
        ("active".to_string(), RedisType::Boolean),
    ];

    let index = Index::from_schema(
        "schema_idx",
        "product:",
        &schema,
        &["name", "description"], // TEXT fields
        &["price"],               // sortable fields
    );

    let cmd = index.to_command_string();
    assert!(cmd.contains("name TEXT"));
    assert!(cmd.contains("description TEXT"));
    assert!(cmd.contains("price NUMERIC SORTABLE"));
    assert!(cmd.contains("quantity NUMERIC"));
    assert!(cmd.contains("active TAG"));
}

/// Test Index::create and Index::drop.
#[test]
#[ignore] // Requires Redis with RediSearch
fn test_index_create_drop() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    // First ensure index doesn't exist
    redis_cli(&["FT.DROPINDEX", "rust_idx_create_test"]);

    let index = Index::new("rust_idx_create_test")
        .with_prefix("rust:idx:create:")
        .with_field(TextField::new("name").sortable())
        .with_field(NumericField::new("value"));

    // Create the index
    if let Err(e) = index.create(&redis_url()) {
        let err = e.to_string();
        if err.contains("unknown command") {
            eprintln!("Skipping test: RediSearch not available");
            return;
        }
        panic!("Unexpected error: {}", err);
    }

    // Verify it exists
    let info = redis_cli_output(&["FT.INFO", "rust_idx_create_test"]);
    assert!(info.is_some());

    // Drop the index
    let result = index.drop(&redis_url());
    assert!(result.is_ok());

    // Verify it's gone
    let info = redis_cli_output(&["FT.INFO", "rust_idx_create_test"]);
    assert!(info.is_none() || info.unwrap().contains("Unknown index name"));
}

/// Test Index::exists.
#[test]
#[ignore] // Requires Redis with RediSearch
fn test_index_exists() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    redis_cli(&["FT.DROPINDEX", "rust_idx_exists_test"]);

    let index = Index::new("rust_idx_exists_test")
        .with_prefix("rust:idx:exists:")
        .with_field(TextField::new("name"));

    // Should not exist
    let exists = index.exists(&redis_url());
    match exists {
        Ok(val) => assert!(!val),
        Err(e) => {
            if e.to_string().contains("unknown command") {
                eprintln!("Skipping test: RediSearch not available");
                return;
            }
            panic!("Unexpected error: {}", e);
        },
    }

    // Create it
    index.create(&redis_url()).unwrap();

    // Should exist now
    assert!(index.exists(&redis_url()).unwrap());

    // Cleanup
    index.drop(&redis_url()).unwrap();
}

/// Test Index::create_if_not_exists.
#[test]
#[ignore] // Requires Redis with RediSearch
fn test_index_create_if_not_exists() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    redis_cli(&["FT.DROPINDEX", "rust_idx_idempotent"]);

    let index = Index::new("rust_idx_idempotent")
        .with_prefix("rust:idx:idempotent:")
        .with_field(TextField::new("name"));

    // First call creates
    if let Err(e) = index.create_if_not_exists(&redis_url()) {
        let err = e.to_string();
        if err.contains("unknown command") {
            eprintln!("Skipping test: RediSearch not available");
            return;
        }
        panic!("Unexpected error: {}", err);
    }

    // Second call is idempotent
    let result = index.create_if_not_exists(&redis_url());
    assert!(result.is_ok());

    // Cleanup
    index.drop(&redis_url()).unwrap();
}

/// Test Index::ensure_exists (alias for create_if_not_exists).
#[test]
#[ignore] // Requires Redis with RediSearch
fn test_index_ensure_exists() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    redis_cli(&["FT.DROPINDEX", "rust_idx_ensure"]);

    let index = Index::new("rust_idx_ensure")
        .with_prefix("rust:idx:ensure:")
        .with_field(TextField::new("name"));

    if let Err(e) = index.ensure_exists(&redis_url()) {
        let err = e.to_string();
        if err.contains("unknown command") {
            eprintln!("Skipping test: RediSearch not available");
            return;
        }
        panic!("Unexpected error: {}", err);
    }

    assert!(index.exists(&redis_url()).unwrap());

    // Cleanup
    index.drop(&redis_url()).unwrap();
}

/// Test Index::recreate.
#[test]
#[ignore] // Requires Redis with RediSearch
fn test_index_recreate() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    redis_cli(&["FT.DROPINDEX", "rust_idx_recreate"]);

    let index = Index::new("rust_idx_recreate")
        .with_prefix("rust:idx:recreate:")
        .with_field(TextField::new("name"));

    // Create initial index
    if let Err(e) = index.create(&redis_url()) {
        let err = e.to_string();
        if err.contains("unknown command") {
            eprintln!("Skipping test: RediSearch not available");
            return;
        }
        panic!("Unexpected error: {}", err);
    }

    // Recreate with different schema
    let new_index = Index::new("rust_idx_recreate")
        .with_prefix("rust:idx:recreate:")
        .with_field(TextField::new("title"))
        .with_field(NumericField::new("count"));

    let result = new_index.recreate(&redis_url());
    assert!(result.is_ok());

    // Verify new schema
    let info = redis_cli_output(&["FT.INFO", "rust_idx_recreate"]);
    assert!(info.is_some());
    let info_str = info.unwrap();
    assert!(info_str.contains("title"));

    // Cleanup
    new_index.drop(&redis_url()).unwrap();
}

/// Test Index::drop_with_docs.
#[test]
#[ignore] // Requires Redis with RediSearch
fn test_index_drop_with_docs() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:idx:dropdocs:*");
    redis_cli(&["FT.DROPINDEX", "rust_idx_dropdocs"]);

    let index = Index::new("rust_idx_dropdocs")
        .with_prefix("rust:idx:dropdocs:")
        .with_field(TextField::new("name"));

    if let Err(e) = index.create(&redis_url()) {
        let err = e.to_string();
        if err.contains("unknown command") {
            eprintln!("Skipping test: RediSearch not available");
            return;
        }
        panic!("Unexpected error: {}", err);
    }

    // Add some documents
    redis_cli(&["HSET", "rust:idx:dropdocs:1", "name", "Test1"]);
    redis_cli(&["HSET", "rust:idx:dropdocs:2", "name", "Test2"]);

    // Verify keys exist
    let exists = redis_cli_output(&["EXISTS", "rust:idx:dropdocs:1"]);
    assert_eq!(exists, Some("1".to_string()));

    // Drop index with documents
    let result = index.drop_with_docs(&redis_url());
    assert!(result.is_ok());

    // Verify keys are deleted
    let exists = redis_cli_output(&["EXISTS", "rust:idx:dropdocs:1"]);
    assert_eq!(exists, Some("0".to_string()));
}

/// Test dropping non-existent index doesn't error.
#[test]
#[ignore] // Requires Redis with RediSearch
fn test_drop_nonexistent_index() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    let index = Index::new("rust_idx_nonexistent")
        .with_prefix("rust:idx:none:")
        .with_field(TextField::new("name"));

    // Should not error even if index doesn't exist
    let result = index.drop(&redis_url());
    // This might error if RediSearch is not available
    if let Err(e) = &result {
        if e.to_string().contains("unknown command") {
            eprintln!("Skipping test: RediSearch not available");
            return;
        }
    }
    assert!(result.is_ok());
}

/// Test Index Display trait.
#[test]
fn test_index_display() {
    let index = Index::new("display_idx")
        .with_prefix("doc:")
        .with_field(TextField::new("content"));

    let display = format!("{}", index);
    assert!(display.starts_with("FT.CREATE display_idx"));
}
