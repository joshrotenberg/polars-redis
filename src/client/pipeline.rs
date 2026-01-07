//! Pipeline and transaction support for batched Redis operations.
//!
//! This module provides [`Pipeline`] and [`Transaction`] types for efficient
//! batched Redis operations with DataFrame ergonomics.
//!
//! # Pipelines
//!
//! Pipelines batch multiple commands and send them in a single round-trip,
//! reducing network latency for bulk operations.
//!
//! ```ignore
//! use polars_redis::client::pipeline::Pipeline;
//!
//! let mut pipe = Pipeline::new("redis://localhost:6379")?;
//!
//! // Queue multiple operations
//! pipe.set("key1", "value1")?;
//! pipe.set("key2", "value2")?;
//! pipe.hset("hash1", "field", "value")?;
//!
//! // Execute all at once
//! let results = pipe.execute()?;
//! ```
//!
//! # Transactions
//!
//! Transactions wrap commands in MULTI/EXEC for atomic execution.
//! Either all commands succeed or none do.
//!
//! ```ignore
//! use polars_redis::client::pipeline::Transaction;
//!
//! let mut tx = Transaction::new("redis://localhost:6379")?;
//!
//! // Queue atomic operations
//! tx.set("counter", "0")?;
//! tx.incr("counter")?;
//! tx.hset("user:1", "last_login", "2024-01-01")?;
//!
//! // Execute atomically
//! let results = tx.execute()?;
//! ```

use redis::Value;
use tokio::runtime::Runtime;

use crate::connection::RedisConnection;
use crate::error::{Error, Result};

/// Result of a single command in a pipeline.
#[derive(Debug, Clone, PartialEq)]
pub enum CommandResult {
    /// Command returned OK/success.
    Ok,
    /// Command returned a string value.
    String(String),
    /// Command returned an integer value.
    Int(i64),
    /// Command returned a bulk string (bytes as string).
    Bulk(String),
    /// Command returned an array of values.
    Array(Vec<CommandResult>),
    /// Command returned nil/null.
    Nil,
    /// Command failed with an error.
    Error(String),
}

impl CommandResult {
    /// Convert a Redis Value to a CommandResult.
    fn from_redis_value(value: Value) -> Self {
        match value {
            Value::Nil => CommandResult::Nil,
            Value::Int(i) => CommandResult::Int(i),
            Value::BulkString(bytes) => {
                CommandResult::Bulk(String::from_utf8_lossy(&bytes).to_string())
            },
            Value::Array(arr) => CommandResult::Array(
                arr.into_iter()
                    .map(CommandResult::from_redis_value)
                    .collect(),
            ),
            Value::SimpleString(s) => CommandResult::String(s),
            Value::Okay => CommandResult::Ok,
            Value::Map(map) => {
                let arr: Vec<CommandResult> = map
                    .into_iter()
                    .flat_map(|(k, v)| {
                        vec![
                            CommandResult::from_redis_value(k),
                            CommandResult::from_redis_value(v),
                        ]
                    })
                    .collect();
                CommandResult::Array(arr)
            },
            Value::Set(set) => CommandResult::Array(
                set.into_iter()
                    .map(CommandResult::from_redis_value)
                    .collect(),
            ),
            Value::Double(d) => CommandResult::String(d.to_string()),
            Value::Boolean(b) => CommandResult::Int(if b { 1 } else { 0 }),
            Value::BigNumber(n) => CommandResult::String(n.to_string()),
            Value::VerbatimString { format: _, text } => CommandResult::String(text),
            Value::Push { kind: _, data } => CommandResult::Array(
                data.into_iter()
                    .map(CommandResult::from_redis_value)
                    .collect(),
            ),
            Value::ServerError(err) => CommandResult::Error(err.to_string()),
            _ => CommandResult::Error("Unknown value type".to_string()),
        }
    }

    /// Check if the result is an error.
    pub fn is_error(&self) -> bool {
        matches!(self, CommandResult::Error(_))
    }

    /// Check if the result is OK.
    pub fn is_ok(&self) -> bool {
        matches!(self, CommandResult::Ok)
    }

    /// Check if the result is nil.
    pub fn is_nil(&self) -> bool {
        matches!(self, CommandResult::Nil)
    }

    /// Get as integer if possible.
    pub fn as_int(&self) -> Option<i64> {
        match self {
            CommandResult::Int(i) => Some(*i),
            _ => None,
        }
    }

    /// Get as string if possible.
    pub fn as_string(&self) -> Option<&str> {
        match self {
            CommandResult::String(s) | CommandResult::Bulk(s) => Some(s),
            _ => None,
        }
    }
}

/// Result of executing a pipeline.
#[derive(Debug)]
pub struct PipelineResult {
    /// Results for each command in order.
    pub results: Vec<CommandResult>,
    /// Number of commands that succeeded.
    pub succeeded: usize,
    /// Number of commands that failed.
    pub failed: usize,
}

impl PipelineResult {
    /// Check if all commands succeeded.
    pub fn all_succeeded(&self) -> bool {
        self.failed == 0
    }

    /// Get the result at a specific index.
    pub fn get(&self, index: usize) -> Option<&CommandResult> {
        self.results.get(index)
    }

    /// Iterate over results.
    pub fn iter(&self) -> impl Iterator<Item = &CommandResult> {
        self.results.iter()
    }
}

/// A command queued in a pipeline.
#[derive(Clone)]
struct QueuedCommand {
    cmd: redis::Cmd,
}

/// Pipeline for batching Redis commands.
///
/// Pipelines reduce network round-trips by sending multiple commands
/// at once and receiving all responses together.
pub struct Pipeline {
    connection: RedisConnection,
    commands: Vec<QueuedCommand>,
    runtime: Runtime,
}

impl Pipeline {
    /// Create a new pipeline.
    ///
    /// # Arguments
    /// * `url` - Redis connection URL
    ///
    /// # Example
    /// ```ignore
    /// let mut pipe = Pipeline::new("redis://localhost:6379")?;
    /// ```
    pub fn new(url: &str) -> Result<Self> {
        let connection = RedisConnection::new(url)?;
        let runtime = Runtime::new()
            .map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

        Ok(Self {
            connection,
            commands: Vec::new(),
            runtime,
        })
    }

    /// Get the number of queued commands.
    pub fn len(&self) -> usize {
        self.commands.len()
    }

    /// Check if the pipeline is empty.
    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }

    /// Clear all queued commands.
    pub fn clear(&mut self) {
        self.commands.clear();
    }

    // ========================================================================
    // String commands
    // ========================================================================

    /// Queue a SET command.
    pub fn set(&mut self, key: &str, value: &str) -> &mut Self {
        let mut cmd = redis::cmd("SET");
        cmd.arg(key).arg(value);
        self.commands.push(QueuedCommand { cmd });
        self
    }

    /// Queue a SET command with expiration.
    pub fn set_ex(&mut self, key: &str, value: &str, seconds: i64) -> &mut Self {
        let mut cmd = redis::cmd("SETEX");
        cmd.arg(key).arg(seconds).arg(value);
        self.commands.push(QueuedCommand { cmd });
        self
    }

    /// Queue a GET command.
    pub fn get(&mut self, key: &str) -> &mut Self {
        let mut cmd = redis::cmd("GET");
        cmd.arg(key);
        self.commands.push(QueuedCommand { cmd });
        self
    }

    /// Queue a MGET command for multiple keys.
    pub fn mget(&mut self, keys: &[&str]) -> &mut Self {
        let mut cmd = redis::cmd("MGET");
        for key in keys {
            cmd.arg(*key);
        }
        self.commands.push(QueuedCommand { cmd });
        self
    }

    /// Queue an INCR command.
    pub fn incr(&mut self, key: &str) -> &mut Self {
        let mut cmd = redis::cmd("INCR");
        cmd.arg(key);
        self.commands.push(QueuedCommand { cmd });
        self
    }

    /// Queue an INCRBY command.
    pub fn incrby(&mut self, key: &str, increment: i64) -> &mut Self {
        let mut cmd = redis::cmd("INCRBY");
        cmd.arg(key).arg(increment);
        self.commands.push(QueuedCommand { cmd });
        self
    }

    /// Queue a DECR command.
    pub fn decr(&mut self, key: &str) -> &mut Self {
        let mut cmd = redis::cmd("DECR");
        cmd.arg(key);
        self.commands.push(QueuedCommand { cmd });
        self
    }

    // ========================================================================
    // Hash commands
    // ========================================================================

    /// Queue an HSET command.
    pub fn hset(&mut self, key: &str, field: &str, value: &str) -> &mut Self {
        let mut cmd = redis::cmd("HSET");
        cmd.arg(key).arg(field).arg(value);
        self.commands.push(QueuedCommand { cmd });
        self
    }

    /// Queue an HMSET command for multiple fields.
    pub fn hmset(&mut self, key: &str, fields: &[(&str, &str)]) -> &mut Self {
        let mut cmd = redis::cmd("HSET");
        cmd.arg(key);
        for (field, value) in fields {
            cmd.arg(*field).arg(*value);
        }
        self.commands.push(QueuedCommand { cmd });
        self
    }

    /// Queue an HGET command.
    pub fn hget(&mut self, key: &str, field: &str) -> &mut Self {
        let mut cmd = redis::cmd("HGET");
        cmd.arg(key).arg(field);
        self.commands.push(QueuedCommand { cmd });
        self
    }

    /// Queue an HGETALL command.
    pub fn hgetall(&mut self, key: &str) -> &mut Self {
        let mut cmd = redis::cmd("HGETALL");
        cmd.arg(key);
        self.commands.push(QueuedCommand { cmd });
        self
    }

    /// Queue an HDEL command.
    pub fn hdel(&mut self, key: &str, fields: &[&str]) -> &mut Self {
        let mut cmd = redis::cmd("HDEL");
        cmd.arg(key);
        for field in fields {
            cmd.arg(*field);
        }
        self.commands.push(QueuedCommand { cmd });
        self
    }

    /// Queue an HINCRBY command.
    pub fn hincrby(&mut self, key: &str, field: &str, increment: i64) -> &mut Self {
        let mut cmd = redis::cmd("HINCRBY");
        cmd.arg(key).arg(field).arg(increment);
        self.commands.push(QueuedCommand { cmd });
        self
    }

    // ========================================================================
    // List commands
    // ========================================================================

    /// Queue an LPUSH command.
    pub fn lpush(&mut self, key: &str, values: &[&str]) -> &mut Self {
        let mut cmd = redis::cmd("LPUSH");
        cmd.arg(key);
        for value in values {
            cmd.arg(*value);
        }
        self.commands.push(QueuedCommand { cmd });
        self
    }

    /// Queue an RPUSH command.
    pub fn rpush(&mut self, key: &str, values: &[&str]) -> &mut Self {
        let mut cmd = redis::cmd("RPUSH");
        cmd.arg(key);
        for value in values {
            cmd.arg(*value);
        }
        self.commands.push(QueuedCommand { cmd });
        self
    }

    /// Queue an LRANGE command.
    pub fn lrange(&mut self, key: &str, start: i64, stop: i64) -> &mut Self {
        let mut cmd = redis::cmd("LRANGE");
        cmd.arg(key).arg(start).arg(stop);
        self.commands.push(QueuedCommand { cmd });
        self
    }

    /// Queue an LLEN command.
    pub fn llen(&mut self, key: &str) -> &mut Self {
        let mut cmd = redis::cmd("LLEN");
        cmd.arg(key);
        self.commands.push(QueuedCommand { cmd });
        self
    }

    // ========================================================================
    // Set commands
    // ========================================================================

    /// Queue an SADD command.
    pub fn sadd(&mut self, key: &str, members: &[&str]) -> &mut Self {
        let mut cmd = redis::cmd("SADD");
        cmd.arg(key);
        for member in members {
            cmd.arg(*member);
        }
        self.commands.push(QueuedCommand { cmd });
        self
    }

    /// Queue an SMEMBERS command.
    pub fn smembers(&mut self, key: &str) -> &mut Self {
        let mut cmd = redis::cmd("SMEMBERS");
        cmd.arg(key);
        self.commands.push(QueuedCommand { cmd });
        self
    }

    /// Queue an SISMEMBER command.
    pub fn sismember(&mut self, key: &str, member: &str) -> &mut Self {
        let mut cmd = redis::cmd("SISMEMBER");
        cmd.arg(key).arg(member);
        self.commands.push(QueuedCommand { cmd });
        self
    }

    /// Queue an SCARD command.
    pub fn scard(&mut self, key: &str) -> &mut Self {
        let mut cmd = redis::cmd("SCARD");
        cmd.arg(key);
        self.commands.push(QueuedCommand { cmd });
        self
    }

    // ========================================================================
    // Sorted set commands
    // ========================================================================

    /// Queue a ZADD command.
    pub fn zadd(&mut self, key: &str, members: &[(f64, &str)]) -> &mut Self {
        let mut cmd = redis::cmd("ZADD");
        cmd.arg(key);
        for (score, member) in members {
            cmd.arg(*score).arg(*member);
        }
        self.commands.push(QueuedCommand { cmd });
        self
    }

    /// Queue a ZRANGE command.
    pub fn zrange(&mut self, key: &str, start: i64, stop: i64) -> &mut Self {
        let mut cmd = redis::cmd("ZRANGE");
        cmd.arg(key).arg(start).arg(stop);
        self.commands.push(QueuedCommand { cmd });
        self
    }

    /// Queue a ZSCORE command.
    pub fn zscore(&mut self, key: &str, member: &str) -> &mut Self {
        let mut cmd = redis::cmd("ZSCORE");
        cmd.arg(key).arg(member);
        self.commands.push(QueuedCommand { cmd });
        self
    }

    /// Queue a ZCARD command.
    pub fn zcard(&mut self, key: &str) -> &mut Self {
        let mut cmd = redis::cmd("ZCARD");
        cmd.arg(key);
        self.commands.push(QueuedCommand { cmd });
        self
    }

    // ========================================================================
    // Key commands
    // ========================================================================

    /// Queue a DEL command.
    pub fn del(&mut self, keys: &[&str]) -> &mut Self {
        let mut cmd = redis::cmd("DEL");
        for key in keys {
            cmd.arg(*key);
        }
        self.commands.push(QueuedCommand { cmd });
        self
    }

    /// Queue an EXISTS command.
    pub fn exists(&mut self, keys: &[&str]) -> &mut Self {
        let mut cmd = redis::cmd("EXISTS");
        for key in keys {
            cmd.arg(*key);
        }
        self.commands.push(QueuedCommand { cmd });
        self
    }

    /// Queue an EXPIRE command.
    pub fn expire(&mut self, key: &str, seconds: i64) -> &mut Self {
        let mut cmd = redis::cmd("EXPIRE");
        cmd.arg(key).arg(seconds);
        self.commands.push(QueuedCommand { cmd });
        self
    }

    /// Queue a TTL command.
    pub fn ttl(&mut self, key: &str) -> &mut Self {
        let mut cmd = redis::cmd("TTL");
        cmd.arg(key);
        self.commands.push(QueuedCommand { cmd });
        self
    }

    /// Queue a RENAME command.
    pub fn rename(&mut self, key: &str, new_key: &str) -> &mut Self {
        let mut cmd = redis::cmd("RENAME");
        cmd.arg(key).arg(new_key);
        self.commands.push(QueuedCommand { cmd });
        self
    }

    /// Queue a TYPE command.
    pub fn key_type(&mut self, key: &str) -> &mut Self {
        let mut cmd = redis::cmd("TYPE");
        cmd.arg(key);
        self.commands.push(QueuedCommand { cmd });
        self
    }

    // ========================================================================
    // Generic command
    // ========================================================================

    /// Queue a raw command with arguments.
    ///
    /// Use this for commands not covered by the convenience methods.
    ///
    /// # Example
    /// ```ignore
    /// pipe.raw("PFADD", &["hll_key", "element1", "element2"]);
    /// ```
    pub fn raw(&mut self, command: &str, args: &[&str]) -> &mut Self {
        let mut cmd = redis::cmd(command);
        for arg in args {
            cmd.arg(*arg);
        }
        self.commands.push(QueuedCommand { cmd });
        self
    }

    // ========================================================================
    // Execution
    // ========================================================================

    /// Execute all queued commands and return results.
    ///
    /// Commands are executed in order, and results are returned in the same order.
    pub fn execute(&mut self) -> Result<PipelineResult> {
        if self.commands.is_empty() {
            return Ok(PipelineResult {
                results: Vec::new(),
                succeeded: 0,
                failed: 0,
            });
        }

        let commands = std::mem::take(&mut self.commands);

        self.runtime.block_on(async {
            let mut conn = self.connection.get_async_connection().await?;

            let mut pipe = redis::pipe();
            for queued in &commands {
                pipe.add_command(queued.cmd.clone());
            }

            let values: Vec<Value> = pipe
                .query_async(&mut conn)
                .await
                .map_err(Error::Connection)?;

            let mut succeeded = 0;
            let mut failed = 0;
            let results: Vec<CommandResult> = values
                .into_iter()
                .map(|v| {
                    let result = CommandResult::from_redis_value(v);
                    if result.is_error() {
                        failed += 1;
                    } else {
                        succeeded += 1;
                    }
                    result
                })
                .collect();

            Ok(PipelineResult {
                results,
                succeeded,
                failed,
            })
        })
    }
}

/// Transaction for atomic Redis operations.
///
/// Transactions use MULTI/EXEC to execute commands atomically.
/// If any command fails during EXEC, the transaction is aborted.
pub struct Transaction {
    pipeline: Pipeline,
}

impl Transaction {
    /// Create a new transaction.
    ///
    /// # Arguments
    /// * `url` - Redis connection URL
    ///
    /// # Example
    /// ```ignore
    /// let mut tx = Transaction::new("redis://localhost:6379")?;
    /// ```
    pub fn new(url: &str) -> Result<Self> {
        Ok(Self {
            pipeline: Pipeline::new(url)?,
        })
    }

    /// Get the number of queued commands.
    pub fn len(&self) -> usize {
        self.pipeline.len()
    }

    /// Check if the transaction is empty.
    pub fn is_empty(&self) -> bool {
        self.pipeline.is_empty()
    }

    /// Clear all queued commands (discards the transaction).
    pub fn discard(&mut self) {
        self.pipeline.clear();
    }

    // Forward all command methods to the inner pipeline
    // String commands
    pub fn set(&mut self, key: &str, value: &str) -> &mut Self {
        self.pipeline.set(key, value);
        self
    }

    pub fn set_ex(&mut self, key: &str, value: &str, seconds: i64) -> &mut Self {
        self.pipeline.set_ex(key, value, seconds);
        self
    }

    pub fn get(&mut self, key: &str) -> &mut Self {
        self.pipeline.get(key);
        self
    }

    pub fn mget(&mut self, keys: &[&str]) -> &mut Self {
        self.pipeline.mget(keys);
        self
    }

    pub fn incr(&mut self, key: &str) -> &mut Self {
        self.pipeline.incr(key);
        self
    }

    pub fn incrby(&mut self, key: &str, increment: i64) -> &mut Self {
        self.pipeline.incrby(key, increment);
        self
    }

    pub fn decr(&mut self, key: &str) -> &mut Self {
        self.pipeline.decr(key);
        self
    }

    // Hash commands
    pub fn hset(&mut self, key: &str, field: &str, value: &str) -> &mut Self {
        self.pipeline.hset(key, field, value);
        self
    }

    pub fn hmset(&mut self, key: &str, fields: &[(&str, &str)]) -> &mut Self {
        self.pipeline.hmset(key, fields);
        self
    }

    pub fn hget(&mut self, key: &str, field: &str) -> &mut Self {
        self.pipeline.hget(key, field);
        self
    }

    pub fn hgetall(&mut self, key: &str) -> &mut Self {
        self.pipeline.hgetall(key);
        self
    }

    pub fn hdel(&mut self, key: &str, fields: &[&str]) -> &mut Self {
        self.pipeline.hdel(key, fields);
        self
    }

    pub fn hincrby(&mut self, key: &str, field: &str, increment: i64) -> &mut Self {
        self.pipeline.hincrby(key, field, increment);
        self
    }

    // List commands
    pub fn lpush(&mut self, key: &str, values: &[&str]) -> &mut Self {
        self.pipeline.lpush(key, values);
        self
    }

    pub fn rpush(&mut self, key: &str, values: &[&str]) -> &mut Self {
        self.pipeline.rpush(key, values);
        self
    }

    pub fn lrange(&mut self, key: &str, start: i64, stop: i64) -> &mut Self {
        self.pipeline.lrange(key, start, stop);
        self
    }

    pub fn llen(&mut self, key: &str) -> &mut Self {
        self.pipeline.llen(key);
        self
    }

    // Set commands
    pub fn sadd(&mut self, key: &str, members: &[&str]) -> &mut Self {
        self.pipeline.sadd(key, members);
        self
    }

    pub fn smembers(&mut self, key: &str) -> &mut Self {
        self.pipeline.smembers(key);
        self
    }

    pub fn sismember(&mut self, key: &str, member: &str) -> &mut Self {
        self.pipeline.sismember(key, member);
        self
    }

    pub fn scard(&mut self, key: &str) -> &mut Self {
        self.pipeline.scard(key);
        self
    }

    // Sorted set commands
    pub fn zadd(&mut self, key: &str, members: &[(f64, &str)]) -> &mut Self {
        self.pipeline.zadd(key, members);
        self
    }

    pub fn zrange(&mut self, key: &str, start: i64, stop: i64) -> &mut Self {
        self.pipeline.zrange(key, start, stop);
        self
    }

    pub fn zscore(&mut self, key: &str, member: &str) -> &mut Self {
        self.pipeline.zscore(key, member);
        self
    }

    pub fn zcard(&mut self, key: &str) -> &mut Self {
        self.pipeline.zcard(key);
        self
    }

    // Key commands
    pub fn del(&mut self, keys: &[&str]) -> &mut Self {
        self.pipeline.del(keys);
        self
    }

    pub fn exists(&mut self, keys: &[&str]) -> &mut Self {
        self.pipeline.exists(keys);
        self
    }

    pub fn expire(&mut self, key: &str, seconds: i64) -> &mut Self {
        self.pipeline.expire(key, seconds);
        self
    }

    pub fn ttl(&mut self, key: &str) -> &mut Self {
        self.pipeline.ttl(key);
        self
    }

    pub fn rename(&mut self, key: &str, new_key: &str) -> &mut Self {
        self.pipeline.rename(key, new_key);
        self
    }

    pub fn key_type(&mut self, key: &str) -> &mut Self {
        self.pipeline.key_type(key);
        self
    }

    pub fn raw(&mut self, command: &str, args: &[&str]) -> &mut Self {
        self.pipeline.raw(command, args);
        self
    }

    /// Execute the transaction atomically.
    ///
    /// All queued commands are wrapped in MULTI/EXEC and executed atomically.
    /// If any command fails, the entire transaction is aborted.
    pub fn execute(&mut self) -> Result<PipelineResult> {
        if self.pipeline.commands.is_empty() {
            return Ok(PipelineResult {
                results: Vec::new(),
                succeeded: 0,
                failed: 0,
            });
        }

        let commands = std::mem::take(&mut self.pipeline.commands);

        self.pipeline.runtime.block_on(async {
            let mut conn = self.pipeline.connection.get_async_connection().await?;

            // Use atomic pipeline (MULTI/EXEC)
            let mut pipe = redis::pipe();
            pipe.atomic();

            for queued in &commands {
                pipe.add_command(queued.cmd.clone());
            }

            let values: Vec<Value> = pipe
                .query_async(&mut conn)
                .await
                .map_err(Error::Connection)?;

            let mut succeeded = 0;
            let mut failed = 0;
            let results: Vec<CommandResult> = values
                .into_iter()
                .map(|v| {
                    let result = CommandResult::from_redis_value(v);
                    if result.is_error() {
                        failed += 1;
                    } else {
                        succeeded += 1;
                    }
                    result
                })
                .collect();

            Ok(PipelineResult {
                results,
                succeeded,
                failed,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_creation() {
        // Should work without a running Redis (just creates the client)
        let result = Pipeline::new("redis://localhost:6379");
        assert!(result.is_ok());
    }

    #[test]
    fn test_transaction_creation() {
        let result = Transaction::new("redis://localhost:6379");
        assert!(result.is_ok());
    }

    #[test]
    fn test_pipeline_len() {
        let mut pipe = Pipeline::new("redis://localhost:6379").unwrap();
        assert!(pipe.is_empty());
        assert_eq!(pipe.len(), 0);

        pipe.set("key1", "value1");
        assert!(!pipe.is_empty());
        assert_eq!(pipe.len(), 1);

        pipe.set("key2", "value2");
        assert_eq!(pipe.len(), 2);

        pipe.clear();
        assert!(pipe.is_empty());
    }

    #[test]
    fn test_command_result_helpers() {
        let ok = CommandResult::Ok;
        assert!(ok.is_ok());
        assert!(!ok.is_error());

        let int = CommandResult::Int(42);
        assert_eq!(int.as_int(), Some(42));
        assert!(int.as_string().is_none());

        let string = CommandResult::String("hello".to_string());
        assert_eq!(string.as_string(), Some("hello"));
        assert!(string.as_int().is_none());

        let nil = CommandResult::Nil;
        assert!(nil.is_nil());

        let err = CommandResult::Error("failed".to_string());
        assert!(err.is_error());
    }

    #[test]
    fn test_pipeline_result_helpers() {
        let result = PipelineResult {
            results: vec![CommandResult::Ok, CommandResult::Int(1)],
            succeeded: 2,
            failed: 0,
        };

        assert!(result.all_succeeded());
        assert_eq!(result.get(0), Some(&CommandResult::Ok));
        assert_eq!(result.iter().count(), 2);
    }
}
