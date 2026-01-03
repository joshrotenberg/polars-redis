//! Error types for polars-redis.

use thiserror::Error;

/// Result type alias for polars-redis operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur in polars-redis operations.
#[derive(Error, Debug)]
pub enum Error {
    /// Redis connection error.
    #[error("Redis connection error: {0}")]
    Connection(#[from] redis::RedisError),

    /// Invalid connection URL.
    #[error("Invalid connection URL: {0}")]
    InvalidUrl(String),

    /// Schema mismatch error.
    #[error("Schema mismatch: {0}")]
    SchemaMismatch(String),

    /// Type conversion error.
    #[error("Type conversion error: {0}")]
    TypeConversion(String),

    /// IO error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Tokio runtime error.
    #[error("Runtime error: {0}")]
    Runtime(String),
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
        }
    }
}
