//! Error types for polars-redis.
//!
//! This module provides error types with helpful, actionable messages
//! to help users diagnose and fix common issues.

use thiserror::Error;

/// Result type alias for polars-redis operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur in polars-redis operations.
#[derive(Error, Debug)]
pub enum Error {
    /// Redis connection error.
    #[error("{}", format_connection_error(.0))]
    Connection(#[from] redis::RedisError),

    /// Invalid connection URL.
    #[error(
        "Invalid Redis URL '{0}'

Expected format: redis://[user:password@]host[:port][/db]

Examples:
  redis://localhost:6379
  redis://user:password@redis.example.com:6379/0
  redis+sentinel://sentinel1:26379,sentinel2:26379/mymaster"
    )]
    InvalidUrl(String),

    /// Schema mismatch error.
    #[error(
        "Schema mismatch: {0}

Ensure the schema field types match the Redis data structure.
Use infer_hash_schema() or infer_json_schema() to auto-detect types."
    )]
    SchemaMismatch(String),

    /// Type conversion error with context.
    #[error("{}", format_type_conversion_error(.0))]
    TypeConversion(String),

    /// IO error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Tokio runtime error.
    #[error("Runtime error: {0}")]
    Runtime(String),

    /// Key not found error.
    #[error(
        "Key not found: '{0}'

The key does not exist in Redis. Check:
  - Key name spelling
  - Key prefix/pattern
  - Whether the key has expired (TTL)"
    )]
    KeyNotFound(String),

    /// RedisJSON module not available.
    #[error(
        "RedisJSON module not available

scan_json() and write_json() require the RedisJSON module.

To check if it's installed:
  $ redis-cli MODULE LIST

To install Redis Stack (includes RedisJSON):
  $ docker run -d -p 6379:6379 redis/redis-stack:latest

Or use scan_hashes() with string fields as an alternative."
    )]
    JsonModuleNotAvailable,

    /// RediSearch module not available.
    #[error(
        "RediSearch module not available

search_hashes() requires the RediSearch module.

To check if it's installed:
  $ redis-cli MODULE LIST

To install Redis Stack (includes RediSearch):
  $ docker run -d -p 6379:6379 redis/redis-stack:latest

Or use scan_hashes() which works with plain Redis:
  df = scan_hashes(url, \"prefix:*\", schema).collect()"
    )]
    SearchModuleNotAvailable,

    /// RediSearch index not found.
    #[error(
        "RediSearch index '{0}' not found

To list existing indexes:
  $ redis-cli FT._LIST

To create an index:
  from polars_redis import Index
  idx = Index(\"{0}\", prefix=\"your:prefix:\", schema=[...])
  idx.create(url)

Or use scan_hashes() which doesn't require an index."
    )]
    IndexNotFound(String),

    /// Invalid input parameter.
    #[error("Invalid input: {0}")]
    InvalidInput(String),

    /// Key already exists (when using WriteMode::Fail).
    #[error(
        "Key already exists: '{0}'

WriteMode::Fail prevents overwriting existing keys.

Options:
  - Use WriteMode::Replace to overwrite
  - Use WriteMode::Append to add new fields only
  - Delete the existing key first"
    )]
    KeyExists(String),

    /// Write type conflict.
    #[error(
        "Type conflict for key '{key}': expected {expected}, found {found}

The key exists but holds a different data type.

Options:
  - Delete the existing key: redis-cli DEL {key}
  - Use a different key name/prefix
  - Use WriteMode::Replace (will delete and recreate)"
    )]
    TypeConflict {
        key: String,
        expected: String,
        found: String,
    },

    /// Channel communication error.
    #[error("Channel error: {0}")]
    Channel(String),

    /// Query/filter not supported for pushdown.
    #[error(
        "Query operation not supported for RediSearch pushdown: {operation}

Supported operations:
  - Equality: col == value
  - Comparison: col > value, col < value, col >= value, col <= value
  - Text search: col.str.contains(\"term\")
  - Range: col.is_between(low, high)
  - Boolean: & (and), | (or), ~ (not)

For unsupported operations, filter after collection:
  df = scan_hashes(...).collect().filter(...)"
    )]
    UnsupportedPushdown { operation: String },

    /// Timeout during operation.
    #[error("Operation timed out after {timeout_secs}s{}", format_timeout_context(.context, .keys_processed))]
    Timeout {
        timeout_secs: u64,
        context: Option<String>,
        keys_processed: Option<usize>,
    },
}

/// Format connection errors with helpful suggestions.
fn format_connection_error(err: &redis::RedisError) -> String {
    let msg = err.to_string();
    let mut result = format!("Redis connection error: {}", msg);

    // Add specific suggestions based on error type
    if msg.contains("Connection refused") || msg.contains("connection refused") {
        result.push_str(
            "

Possible causes:
  - Redis server is not running
  - Wrong host or port in URL
  - Firewall blocking connection

To verify Redis is running:
  $ redis-cli ping

To start Redis with Docker:
  $ docker run -d -p 6379:6379 redis:latest",
        );
    } else if msg.contains("Name or service not known")
        || msg.contains("nodename nor servname provided")
    {
        result.push_str(
            "

The hostname could not be resolved. Check:
  - Hostname spelling
  - DNS configuration
  - Network connectivity",
        );
    } else if msg.contains("Authentication")
        || msg.contains("NOAUTH")
        || msg.contains("invalid password")
    {
        result.push_str(
            "

Authentication failed. Check:
  - Username and password in URL
  - Redis ACL configuration
  - URL format: redis://user:password@host:port",
        );
    } else if msg.contains("WRONGTYPE") {
        result.push_str(
            "

The key exists but holds a different data type.
Use DEL to remove it first, or use a different key name.",
        );
    }

    result
}

/// Format type conversion errors with context.
fn format_type_conversion_error(msg: &str) -> String {
    let mut result = format!("Type conversion error: {}", msg);

    if msg.contains("as i64") || msg.contains("as Int64") {
        result.push_str(
            "

The value cannot be parsed as an integer. Options:
  - Change schema type to Utf8 for this field
  - Fix the data in Redis
  - Use infer_hash_schema() to auto-detect types",
        );
    } else if msg.contains("as f64") || msg.contains("as Float64") {
        result.push_str(
            "

The value cannot be parsed as a float. Options:
  - Change schema type to Utf8 for this field
  - Fix the data in Redis
  - Use infer_hash_schema() to auto-detect types",
        );
    } else if msg.contains("as boolean") || msg.contains("as Boolean") {
        result.push_str(
            "

The value cannot be parsed as a boolean.
Accepted values: true, false, 1, 0, yes, no, on, off",
        );
    } else if msg.contains("as date") || msg.contains("as Date") {
        result.push_str(
            "

The value cannot be parsed as a date.
Expected format: YYYY-MM-DD (e.g., 2024-01-15)",
        );
    } else if msg.contains("as datetime") || msg.contains("as Datetime") {
        result.push_str(
            "

The value cannot be parsed as a datetime.
Expected formats:
  - ISO 8601: 2024-01-15T10:30:00Z
  - Unix timestamp (seconds): 1705312200
  - Unix timestamp (milliseconds): 1705312200000",
        );
    }

    result
}

/// Format timeout context message.
fn format_timeout_context(context: &Option<String>, keys_processed: &Option<usize>) -> String {
    let mut parts = Vec::new();

    if let Some(ctx) = context {
        parts.push(format!(" while {}", ctx));
    }

    if let Some(count) = keys_processed {
        parts.push(format!(" (processed {} keys)", count));
    }

    if !parts.is_empty() {
        let mut result = parts.join("");
        result.push_str(
            "

Options:
  - Increase timeout parameter
  - Reduce batch size
  - Use RediSearch for filtering (faster for large datasets)
  - Add an index for predicate pushdown",
        );
        result
    } else {
        String::new()
    }
}

impl Error {
    /// Create a type conversion error with field context.
    pub fn type_conversion_with_context(
        field: &str,
        expected_type: &str,
        value: &str,
        key: Option<&str>,
    ) -> Self {
        let key_info = key.map(|k| format!(" (key: {})", k)).unwrap_or_default();
        Error::TypeConversion(format!(
            "Field '{}'{}: expected {}, got '{}'",
            field, key_info, expected_type, value
        ))
    }

    /// Create a timeout error with context.
    pub fn timeout(
        timeout_secs: u64,
        context: Option<String>,
        keys_processed: Option<usize>,
    ) -> Self {
        Error::Timeout {
            timeout_secs,
            context,
            keys_processed,
        }
    }

    /// Check if this is a RediSearch module not available error.
    pub fn is_search_module_missing(err: &redis::RedisError) -> bool {
        let msg = err.to_string().to_lowercase();
        msg.contains("unknown command") && (msg.contains("ft.") || msg.contains("ft "))
    }

    /// Check if this is an index not found error.
    pub fn is_index_not_found(err: &redis::RedisError) -> bool {
        let msg = err.to_string();
        msg.contains("Unknown Index name") || msg.contains("no such index")
    }

    /// Check if this is a JSON module not available error.
    pub fn is_json_module_missing(err: &redis::RedisError) -> bool {
        let msg = err.to_string().to_lowercase();
        msg.contains("unknown command") && msg.contains("json.")
    }
}

#[cfg(feature = "python")]
impl From<Error> for pyo3::PyErr {
    fn from(err: Error) -> pyo3::PyErr {
        match err {
            Error::Connection(_) | Error::InvalidUrl(_) => {
                pyo3::exceptions::PyConnectionError::new_err(err.to_string())
            },
            Error::SchemaMismatch(_)
            | Error::TypeConversion(_)
            | Error::TypeConflict { .. }
            | Error::UnsupportedPushdown { .. } => {
                pyo3::exceptions::PyValueError::new_err(err.to_string())
            },
            Error::Io(_) => pyo3::exceptions::PyIOError::new_err(err.to_string()),
            Error::Runtime(_) | Error::Channel(_) => {
                pyo3::exceptions::PyRuntimeError::new_err(err.to_string())
            },
            Error::KeyNotFound(_) | Error::IndexNotFound(_) => {
                pyo3::exceptions::PyKeyError::new_err(err.to_string())
            },
            Error::JsonModuleNotAvailable | Error::SearchModuleNotAvailable => {
                pyo3::exceptions::PyRuntimeError::new_err(err.to_string())
            },
            Error::InvalidInput(_) | Error::KeyExists(_) => {
                pyo3::exceptions::PyValueError::new_err(err.to_string())
            },
            Error::Timeout { .. } => pyo3::exceptions::PyTimeoutError::new_err(err.to_string()),
        }
    }
}
