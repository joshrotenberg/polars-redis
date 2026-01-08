//! Example: RediSearch integration with polars-redis.
//!
//! This example demonstrates how to use the Rust API for server-side
//! filtering and aggregation with RediSearch.
//!
//! Run with: cargo run --example search_example
//!
//! Prerequisites:
//!     - Redis Stack running on localhost:6379
//!     - RediSearch module loaded (comes with Redis Stack)

use polars_redis::{HashSchema, HashSearchIterator, RedisType, SearchBatchConfig};
use redis::Commands;
use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());

    println!("Connecting to: {}", url);
    println!();

    // Set up sample data and index
    setup_sample_data(&url)?;

    // =========================================================================
    // Example 1: Basic Search with Query String
    // =========================================================================
    println!("=== Example 1: Basic Search (age > 30) ===\n");

    let schema = HashSchema::new(vec![
        ("name".to_string(), RedisType::Utf8),
        ("age".to_string(), RedisType::Int64),
        ("department".to_string(), RedisType::Utf8),
        ("salary".to_string(), RedisType::Float64),
    ])
    .with_key(true)
    .with_key_column_name("_key");

    // Use RediSearch query syntax
    let config = SearchBatchConfig::new("employees_idx", "@age:[30 +inf]").with_batch_size(100);

    let mut iterator = HashSearchIterator::new(&url, schema.clone(), config, None)?;

    let mut total_rows = 0;
    while let Some(batch) = iterator.next_batch()? {
        total_rows += batch.num_rows();
        println!("Batch: {} rows", batch.num_rows());

        // Print column names
        if total_rows == batch.num_rows() {
            println!(
                "Columns: {:?}",
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name())
                    .collect::<Vec<_>>()
            );
        }
    }
    println!("Total matching: {} employees over 30\n", total_rows);

    // =========================================================================
    // Example 2: Tag Query (department filter)
    // =========================================================================
    println!("=== Example 2: Tag Query (engineering department) ===\n");

    let config =
        SearchBatchConfig::new("employees_idx", "@department:{engineering}").with_batch_size(100);

    let mut iterator = HashSearchIterator::new(&url, schema.clone(), config, None)?;

    let mut total_rows = 0;
    while let Some(batch) = iterator.next_batch()? {
        total_rows += batch.num_rows();
    }
    println!("Found {} engineers\n", total_rows);

    // =========================================================================
    // Example 3: Combined Query (age AND department)
    // =========================================================================
    println!("=== Example 3: Combined Query (age > 30 AND active) ===\n");

    // Combine conditions: age > 30 AND status is active
    let config = SearchBatchConfig::new("employees_idx", "@age:[30 +inf] @status:{active}")
        .with_batch_size(100);

    let mut iterator = HashSearchIterator::new(&url, schema.clone(), config, None)?;

    let mut total_rows = 0;
    while let Some(batch) = iterator.next_batch()? {
        total_rows += batch.num_rows();
    }
    println!("Found {} active employees over 30\n", total_rows);

    // =========================================================================
    // Example 4: Sorted Search
    // =========================================================================
    println!("=== Example 4: Sorted Search (by salary descending) ===\n");

    let config = SearchBatchConfig::new("employees_idx", "@status:{active}")
        .with_batch_size(100)
        .with_sort_by("salary", false); // false = descending

    let mut iterator = HashSearchIterator::new(&url, schema.clone(), config, None)?;

    let mut total_rows = 0;
    while let Some(batch) = iterator.next_batch()? {
        total_rows += batch.num_rows();
        println!("Batch: {} rows (sorted by salary DESC)", batch.num_rows());
    }
    println!("Total: {} active employees\n", total_rows);

    // =========================================================================
    // Example 5: Projection Pushdown (select specific fields)
    // =========================================================================
    println!("=== Example 5: Projection (name and salary only) ===\n");

    let config = SearchBatchConfig::new("employees_idx", "*").with_batch_size(100);

    // Only request specific fields
    let projection = Some(vec!["name".to_string(), "salary".to_string()]);

    let mut iterator = HashSearchIterator::new(&url, schema.clone(), config, projection)?;

    if let Some(batch) = iterator.next_batch()? {
        println!(
            "Returned columns: {:?}",
            batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>()
        );
        println!("Rows: {}\n", batch.num_rows());
    }

    // =========================================================================
    // Example 6: Negation Query
    // =========================================================================
    println!("=== Example 6: Negation (NOT inactive) ===\n");

    // Find employees who are NOT inactive
    let config =
        SearchBatchConfig::new("employees_idx", "-@status:{inactive}").with_batch_size(100);

    let mut iterator = HashSearchIterator::new(&url, schema.clone(), config, None)?;

    let mut total_rows = 0;
    while let Some(batch) = iterator.next_batch()? {
        total_rows += batch.num_rows();
    }
    println!("Found {} non-inactive employees\n", total_rows);

    // =========================================================================
    // Example 7: OR Query
    // =========================================================================
    println!("=== Example 7: OR Query (engineering OR product) ===\n");

    // Find employees in engineering OR product departments
    let config = SearchBatchConfig::new("employees_idx", "@department:{engineering|product}")
        .with_batch_size(100);

    let mut iterator = HashSearchIterator::new(&url, schema, config, None)?;

    let mut total_rows = 0;
    while let Some(batch) = iterator.next_batch()? {
        total_rows += batch.num_rows();
    }
    println!("Found {} employees in engineering or product\n", total_rows);

    println!("=== All search examples completed! ===");

    Ok(())
}

/// Set up sample data and RediSearch index for examples.
fn setup_sample_data(url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let client = redis::Client::open(url)?;
    let mut conn = client.get_connection()?;

    // Clear existing data
    let keys: Vec<String> = redis::cmd("KEYS").arg("employee:*").query(&mut conn)?;
    for key in keys {
        let _: () = conn.del(&key)?;
    }

    // Create sample employees
    let employees = vec![
        ("1", "Alice", "32", "engineering", "120000", "active"),
        ("2", "Bob", "28", "engineering", "95000", "active"),
        ("3", "Carol", "45", "product", "140000", "active"),
        ("4", "Dave", "35", "product", "110000", "inactive"),
        ("5", "Eve", "29", "marketing", "85000", "active"),
        ("6", "Frank", "52", "engineering", "150000", "active"),
        ("7", "Grace", "38", "marketing", "95000", "active"),
        ("8", "Henry", "41", "engineering", "130000", "inactive"),
    ];

    for (id, name, age, dept, salary, status) in employees {
        let _: () = redis::cmd("HSET")
            .arg(format!("employee:{}", id))
            .arg("name")
            .arg(name)
            .arg("age")
            .arg(age)
            .arg("department")
            .arg(dept)
            .arg("salary")
            .arg(salary)
            .arg("status")
            .arg(status)
            .query(&mut conn)?;
    }

    // Drop existing index if it exists
    let _: Result<(), redis::RedisError> = redis::cmd("FT.DROPINDEX")
        .arg("employees_idx")
        .query(&mut conn);

    // Create RediSearch index
    redis::cmd("FT.CREATE")
        .arg("employees_idx")
        .arg("ON")
        .arg("HASH")
        .arg("PREFIX")
        .arg("1")
        .arg("employee:")
        .arg("SCHEMA")
        .arg("name")
        .arg("TEXT")
        .arg("SORTABLE")
        .arg("age")
        .arg("NUMERIC")
        .arg("SORTABLE")
        .arg("department")
        .arg("TAG")
        .arg("salary")
        .arg("NUMERIC")
        .arg("SORTABLE")
        .arg("status")
        .arg("TAG")
        .query(&mut conn)?;

    // Wait briefly for index to be ready
    std::thread::sleep(std::time::Duration::from_millis(100));

    println!("Created 8 employees and RediSearch index\n");

    Ok(())
}
