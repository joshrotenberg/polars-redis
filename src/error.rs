//! Error types for polars-redis.

use thiserror::Error;

/// Result type alias for polars-redis operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur in polars-redis operations.
#[derive(Error, Debug)]
pub enum Error {
    /// Redis connection error.
    #[error("Redis connection error: {0}. Check that Redis is running and accessible.")]
    Connection(#[from] redis::RedisError),

    /// Invalid connection URL.
    #[error("Invalid Redis URL '{0}'. Expected format: redis://[user:password@]host[:port][/db]")]
    InvalidUrl(String),

    /// Schema mismatch error.
    #[error("Schema mismatch: {0}. Ensure the schema matches the Redis data structure.")]
    SchemaMismatch(String),

    /// Type conversion error.
    #[error("Type conversion error: {0}. Check that the schema types match the actual data.")]
    TypeConversion(String),

    /// IO error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Tokio runtime error.
    #[error("Runtime error: {0}")]
    Runtime(String),

    /// Key not found error.
    #[error("Key not found: {0}")]
    KeyNotFound(String),

    /// RedisJSON module not available.
    #[error("RedisJSON module not available. Install redis-stack or load the ReJSON module.")]
    JsonModuleNotAvailable,
}

#[cfg(feature = "python")]
impl From<Error> for pyo3::PyErr {
    fn from(err: Error) -> pyo3::PyErr {
        match err {
            Error::Connection(_) | Error::InvalidUrl(_) => {
                pyo3::exceptions::PyConnectionError::new_err(err.to_string())
            }
            Error::SchemaMismatch(_) | Error::TypeConversion(_) => {
                pyo3::exceptions::PyValueError::new_err(err.to_string())
            }
            Error::Io(_) => pyo3::exceptions::PyIOError::new_err(err.to_string()),
            Error::Runtime(_) => pyo3::exceptions::PyRuntimeError::new_err(err.to_string()),
            Error::KeyNotFound(_) => pyo3::exceptions::PyKeyError::new_err(err.to_string()),
            Error::JsonModuleNotAvailable => {
                pyo3::exceptions::PyRuntimeError::new_err(err.to_string())
            }
        }
    }
}
