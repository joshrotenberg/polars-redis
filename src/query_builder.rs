//! Query builder for translating filter expressions to RediSearch queries.
//!
//! This module provides utilities for converting simple filter predicates
//! into RediSearch query syntax, enabling automatic predicate pushdown.
//!
//! # Supported Translations
//!
//! | Expression | RediSearch |
//! |------------|------------|
//! | `col == value` | `@col:{value}` (TAG) or `@col:[value value]` (NUMERIC) |
//! | `col > value` | `@col:[(value +inf]` |
//! | `col >= value` | `@col:[value +inf]` |
//! | `col < value` | `@col:[-inf (value]` |
//! | `col <= value` | `@col:[-inf value]` |
//! | `col.is_between(a, b)` | `@col:[a b]` |
//! | `expr1 & expr2` | `(query1) (query2)` |
//! | `expr1 \| expr2` | `(query1) \| (query2)` |
//!
//! # Example
//!
//! ```ignore
//! use polars_redis::query_builder::{Predicate, PredicateBuilder};
//!
//! // Build: @age:[30 +inf] @status:{active}
//! let query = PredicateBuilder::new()
//!     .and(Predicate::gt("age", 30))
//!     .and(Predicate::eq("status", "active"))
//!     .build();
//! ```

use std::fmt;

/// A single predicate that can be translated to RediSearch.
#[derive(Debug, Clone)]
pub enum Predicate {
    /// Equality: `@field:{value}` for TAG, `@field:[value value]` for NUMERIC
    Eq(String, Value),
    /// Not equal: `-@field:{value}`
    Ne(String, Value),
    /// Greater than: `@field:[(value +inf]`
    Gt(String, Value),
    /// Greater than or equal: `@field:[value +inf]`
    Gte(String, Value),
    /// Less than: `@field:[-inf (value]`
    Lt(String, Value),
    /// Less than or equal: `@field:[-inf value]`
    Lte(String, Value),
    /// Between (inclusive): `@field:[min max]`
    Between(String, Value, Value),
    /// AND of multiple predicates
    And(Vec<Predicate>),
    /// OR of multiple predicates
    Or(Vec<Predicate>),
    /// Raw RediSearch query (escape hatch)
    Raw(String),
}

/// A value in a predicate.
#[derive(Debug, Clone)]
pub enum Value {
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Int(n) => write!(f, "{}", n),
            Value::Float(n) => write!(f, "{}", n),
            Value::String(s) => write!(f, "{}", s),
            Value::Bool(b) => write!(f, "{}", b),
        }
    }
}

impl Value {
    /// Check if this value should be treated as numeric.
    pub fn is_numeric(&self) -> bool {
        matches!(self, Value::Int(_) | Value::Float(_))
    }
}

// Convenience conversions
impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Int(v)
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::Int(v as i64)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Float(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::String(v.to_string())
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::String(v)
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}

impl Predicate {
    /// Create an equality predicate.
    pub fn eq(field: impl Into<String>, value: impl Into<Value>) -> Self {
        Predicate::Eq(field.into(), value.into())
    }

    /// Create a not-equal predicate.
    pub fn ne(field: impl Into<String>, value: impl Into<Value>) -> Self {
        Predicate::Ne(field.into(), value.into())
    }

    /// Create a greater-than predicate.
    pub fn gt(field: impl Into<String>, value: impl Into<Value>) -> Self {
        Predicate::Gt(field.into(), value.into())
    }

    /// Create a greater-than-or-equal predicate.
    pub fn gte(field: impl Into<String>, value: impl Into<Value>) -> Self {
        Predicate::Gte(field.into(), value.into())
    }

    /// Create a less-than predicate.
    pub fn lt(field: impl Into<String>, value: impl Into<Value>) -> Self {
        Predicate::Lt(field.into(), value.into())
    }

    /// Create a less-than-or-equal predicate.
    pub fn lte(field: impl Into<String>, value: impl Into<Value>) -> Self {
        Predicate::Lte(field.into(), value.into())
    }

    /// Create a between predicate (inclusive).
    pub fn between(field: impl Into<String>, min: impl Into<Value>, max: impl Into<Value>) -> Self {
        Predicate::Between(field.into(), min.into(), max.into())
    }

    /// Create a raw RediSearch query.
    pub fn raw(query: impl Into<String>) -> Self {
        Predicate::Raw(query.into())
    }

    /// Combine with AND.
    pub fn and(self, other: Predicate) -> Self {
        match self {
            Predicate::And(mut preds) => {
                preds.push(other);
                Predicate::And(preds)
            }
            _ => Predicate::And(vec![self, other]),
        }
    }

    /// Combine with OR.
    pub fn or(self, other: Predicate) -> Self {
        match self {
            Predicate::Or(mut preds) => {
                preds.push(other);
                Predicate::Or(preds)
            }
            _ => Predicate::Or(vec![self, other]),
        }
    }

    /// Convert to RediSearch query string.
    pub fn to_query(&self) -> String {
        match self {
            Predicate::Eq(field, value) => {
                if value.is_numeric() {
                    // Numeric equality: @field:[value value]
                    format!("@{}:[{} {}]", field, value, value)
                } else {
                    // TAG/TEXT equality: @field:{value}
                    format!("@{}:{{{}}}", field, escape_tag_value(&value.to_string()))
                }
            }
            Predicate::Ne(field, value) => {
                if value.is_numeric() {
                    format!("-@{}:[{} {}]", field, value, value)
                } else {
                    format!("-@{}:{{{}}}", field, escape_tag_value(&value.to_string()))
                }
            }
            Predicate::Gt(field, value) => {
                // Exclusive lower bound: (value
                format!("@{}:[({} +inf]", field, value)
            }
            Predicate::Gte(field, value) => {
                format!("@{}:[{} +inf]", field, value)
            }
            Predicate::Lt(field, value) => {
                // Exclusive upper bound: (value
                format!("@{}:[-inf ({}]", field, value)
            }
            Predicate::Lte(field, value) => {
                format!("@{}:[-inf {}]", field, value)
            }
            Predicate::Between(field, min, max) => {
                format!("@{}:[{} {}]", field, min, max)
            }
            Predicate::And(preds) => {
                if preds.is_empty() {
                    "*".to_string()
                } else {
                    preds
                        .iter()
                        .map(|p| {
                            let q = p.to_query();
                            if matches!(p, Predicate::Or(_)) {
                                format!("({})", q)
                            } else {
                                q
                            }
                        })
                        .collect::<Vec<_>>()
                        .join(" ")
                }
            }
            Predicate::Or(preds) => {
                if preds.is_empty() {
                    "*".to_string()
                } else {
                    preds
                        .iter()
                        .map(|p| {
                            let q = p.to_query();
                            if matches!(p, Predicate::And(_)) {
                                format!("({})", q)
                            } else {
                                q
                            }
                        })
                        .collect::<Vec<_>>()
                        .join(" | ")
                }
            }
            Predicate::Raw(query) => query.clone(),
        }
    }
}

/// Escape special characters in TAG values.
fn escape_tag_value(s: &str) -> String {
    // RediSearch TAG values need escaping for: , . < > { } [ ] " ' : ; ! @ # $ % ^ & * ( ) - + = ~
    let mut result = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            ',' | '.' | '<' | '>' | '{' | '}' | '[' | ']' | '"' | '\'' | ':' | ';' | '!' | '@'
            | '#' | '$' | '%' | '^' | '&' | '*' | '(' | ')' | '-' | '+' | '=' | '~' | ' ' => {
                result.push('\\');
                result.push(c);
            }
            _ => result.push(c),
        }
    }
    result
}

/// Builder for constructing predicates fluently.
#[derive(Debug, Clone, Default)]
pub struct PredicateBuilder {
    predicates: Vec<Predicate>,
}

impl PredicateBuilder {
    /// Create a new empty builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an AND predicate.
    pub fn and(mut self, predicate: Predicate) -> Self {
        self.predicates.push(predicate);
        self
    }

    /// Build the final query string.
    pub fn build(self) -> String {
        if self.predicates.is_empty() {
            "*".to_string()
        } else if self.predicates.len() == 1 {
            self.predicates[0].to_query()
        } else {
            Predicate::And(self.predicates).to_query()
        }
    }

    /// Build as a Predicate (for further composition).
    pub fn build_predicate(self) -> Predicate {
        if self.predicates.is_empty() {
            Predicate::Raw("*".to_string())
        } else if self.predicates.len() == 1 {
            self.predicates.into_iter().next().unwrap()
        } else {
            Predicate::And(self.predicates)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_eq_numeric() {
        let pred = Predicate::eq("age", 30);
        assert_eq!(pred.to_query(), "@age:[30 30]");
    }

    #[test]
    fn test_eq_string() {
        let pred = Predicate::eq("status", "active");
        assert_eq!(pred.to_query(), "@status:{active}");
    }

    #[test]
    fn test_gt() {
        let pred = Predicate::gt("age", 30);
        assert_eq!(pred.to_query(), "@age:[(30 +inf]");
    }

    #[test]
    fn test_gte() {
        let pred = Predicate::gte("age", 30);
        assert_eq!(pred.to_query(), "@age:[30 +inf]");
    }

    #[test]
    fn test_lt() {
        let pred = Predicate::lt("age", 30);
        assert_eq!(pred.to_query(), "@age:[-inf (30]");
    }

    #[test]
    fn test_lte() {
        let pred = Predicate::lte("age", 30);
        assert_eq!(pred.to_query(), "@age:[-inf 30]");
    }

    #[test]
    fn test_between() {
        let pred = Predicate::between("age", 20, 40);
        assert_eq!(pred.to_query(), "@age:[20 40]");
    }

    #[test]
    fn test_and() {
        let pred = Predicate::gt("age", 30).and(Predicate::eq("status", "active"));
        assert_eq!(pred.to_query(), "@age:[(30 +inf] @status:{active}");
    }

    #[test]
    fn test_or() {
        let pred = Predicate::eq("status", "active").or(Predicate::eq("status", "pending"));
        assert_eq!(pred.to_query(), "@status:{active} | @status:{pending}");
    }

    #[test]
    fn test_complex_and_or() {
        // (age > 30 AND status = active) OR (age < 20)
        let pred = Predicate::gt("age", 30)
            .and(Predicate::eq("status", "active"))
            .or(Predicate::lt("age", 20));

        // The AND creates: @age:[(30 +inf] @status:{active}
        // Then OR with lt: (@age:[(30 +inf] @status:{active}) | @age:[-inf (20]
        let query = pred.to_query();
        assert!(query.contains("@age:[(30 +inf]"));
        assert!(query.contains("@status:{active}"));
        assert!(query.contains("|"));
    }

    #[test]
    fn test_builder() {
        let query = PredicateBuilder::new()
            .and(Predicate::gt("age", 30))
            .and(Predicate::eq("status", "active"))
            .build();

        assert_eq!(query, "@age:[(30 +inf] @status:{active}");
    }

    #[test]
    fn test_builder_empty() {
        let query = PredicateBuilder::new().build();
        assert_eq!(query, "*");
    }

    #[test]
    fn test_escape_tag_value() {
        let pred = Predicate::eq("email", "user@example.com");
        assert_eq!(pred.to_query(), r"@email:{user\@example\.com}");
    }

    #[test]
    fn test_float_values() {
        let pred = Predicate::gt("score", 3.5);
        assert_eq!(pred.to_query(), "@score:[(3.5 +inf]");
    }

    #[test]
    fn test_ne() {
        let pred = Predicate::ne("status", "deleted");
        assert_eq!(pred.to_query(), "-@status:{deleted}");
    }
}
