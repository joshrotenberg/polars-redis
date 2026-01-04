//! Query builder for translating filter expressions to RediSearch queries.
//!
//! This module provides utilities for converting simple filter predicates
//! into RediSearch query syntax, enabling automatic predicate pushdown.
//!
//! # Supported Operations
//!
//! | Operation | RediSearch |
//! |-----------|------------|
//! | `Predicate::eq("age", 30)` | `@age:[30 30]` |
//! | `Predicate::gt("age", 30)` | `@age:[(30 +inf]` |
//! | `Predicate::between("age", 20, 40)` | `@age:[20 40]` |
//! | `Predicate::text_search("title", "python")` | `@title:python` |
//! | `Predicate::prefix("name", "jo")` | `@name:jo*` |
//! | `Predicate::tag("status", "active")` | `@status:{active}` |
//! | `Predicate::geo_radius("loc", -122.4, 37.7, 10.0, "km")` | `@loc:[-122.4 37.7 10 km]` |
//! | `pred1.and(pred2)` | `query1 query2` |
//! | `pred1.or(pred2)` | `query1 \| query2` |
//! | `pred.not()` | `-(query)` |
//!
//! # Example
//!
//! ```ignore
//! use polars_redis::query_builder::{Predicate, PredicateBuilder};
//!
//! // Build: @age:[(30 +inf] @status:{active}
//! let query = PredicateBuilder::new()
//!     .and(Predicate::gt("age", 30))
//!     .and(Predicate::tag("status", "active"))
//!     .build();
//! ```

use std::fmt;

/// A single predicate that can be translated to RediSearch.
#[derive(Debug, Clone)]
pub enum Predicate {
    // ========================================================================
    // Comparison operators
    // ========================================================================
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

    // ========================================================================
    // Logical operators
    // ========================================================================
    /// AND of multiple predicates
    And(Vec<Predicate>),
    /// OR of multiple predicates
    Or(Vec<Predicate>),
    /// NOT: `-(query)`
    Not(Box<Predicate>),

    // ========================================================================
    // Text search
    // ========================================================================
    /// Full-text search: `@field:term`
    TextSearch(String, String),
    /// Prefix match: `@field:prefix*`
    Prefix(String, String),
    /// Suffix match: `@field:*suffix`
    Suffix(String, String),
    /// Wildcard match: `@field:pattern`
    Wildcard(String, String),
    /// Fuzzy match: `@field:%term%` (distance 1), `@field:%%term%%` (distance 2)
    Fuzzy(String, String, u8),
    /// Phrase search: `@field:(word1 word2 word3)`
    Phrase(String, Vec<String>),

    // ========================================================================
    // Tag operations
    // ========================================================================
    /// Tag match: `@field:{tag}`
    Tag(String, String),
    /// Tag OR: `@field:{tag1|tag2|tag3}`
    TagOr(String, Vec<String>),

    // ========================================================================
    // Geo operations
    // ========================================================================
    /// Geo radius: `@field:[lon lat radius unit]`
    GeoRadius(String, f64, f64, f64, String),

    // ========================================================================
    // Null checks
    // ========================================================================
    /// Field is missing: `ismissing(@field)`
    IsMissing(String),
    /// Field exists: `-ismissing(@field)`
    IsNotMissing(String),

    // ========================================================================
    // Scoring
    // ========================================================================
    /// Boost: `(query) => { $weight: value; }`
    Boost(Box<Predicate>, f64),

    // ========================================================================
    // Raw/escape hatch
    // ========================================================================
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
    // ========================================================================
    // Comparison constructors
    // ========================================================================

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

    // ========================================================================
    // Text search constructors
    // ========================================================================

    /// Create a full-text search predicate.
    pub fn text_search(field: impl Into<String>, term: impl Into<String>) -> Self {
        Predicate::TextSearch(field.into(), term.into())
    }

    /// Create a prefix match predicate.
    pub fn prefix(field: impl Into<String>, prefix: impl Into<String>) -> Self {
        Predicate::Prefix(field.into(), prefix.into())
    }

    /// Create a suffix match predicate.
    pub fn suffix(field: impl Into<String>, suffix: impl Into<String>) -> Self {
        Predicate::Suffix(field.into(), suffix.into())
    }

    /// Create a wildcard match predicate.
    pub fn wildcard(field: impl Into<String>, pattern: impl Into<String>) -> Self {
        Predicate::Wildcard(field.into(), pattern.into())
    }

    /// Create a fuzzy match predicate.
    pub fn fuzzy(field: impl Into<String>, term: impl Into<String>, distance: u8) -> Self {
        Predicate::Fuzzy(field.into(), term.into(), distance.clamp(1, 3))
    }

    /// Create a phrase search predicate.
    pub fn phrase(field: impl Into<String>, words: Vec<impl Into<String>>) -> Self {
        Predicate::Phrase(field.into(), words.into_iter().map(|w| w.into()).collect())
    }

    // ========================================================================
    // Tag constructors
    // ========================================================================

    /// Create a tag match predicate.
    pub fn tag(field: impl Into<String>, tag: impl Into<String>) -> Self {
        Predicate::Tag(field.into(), tag.into())
    }

    /// Create a tag OR predicate.
    pub fn tag_or(field: impl Into<String>, tags: Vec<impl Into<String>>) -> Self {
        Predicate::TagOr(field.into(), tags.into_iter().map(|t| t.into()).collect())
    }

    // ========================================================================
    // Geo constructors
    // ========================================================================

    /// Create a geo radius predicate.
    pub fn geo_radius(
        field: impl Into<String>,
        lon: f64,
        lat: f64,
        radius: f64,
        unit: impl Into<String>,
    ) -> Self {
        Predicate::GeoRadius(field.into(), lon, lat, radius, unit.into())
    }

    // ========================================================================
    // Null check constructors
    // ========================================================================

    /// Check if field is missing.
    pub fn is_missing(field: impl Into<String>) -> Self {
        Predicate::IsMissing(field.into())
    }

    /// Check if field exists.
    pub fn is_not_missing(field: impl Into<String>) -> Self {
        Predicate::IsNotMissing(field.into())
    }

    // ========================================================================
    // Raw constructor
    // ========================================================================

    /// Create a raw RediSearch query.
    pub fn raw(query: impl Into<String>) -> Self {
        Predicate::Raw(query.into())
    }

    // ========================================================================
    // Combinators
    // ========================================================================

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

    /// Negate this predicate.
    pub fn negate(self) -> Self {
        Predicate::Not(Box::new(self))
    }

    /// Boost this predicate's relevance score.
    pub fn boost(self, weight: f64) -> Self {
        Predicate::Boost(Box::new(self), weight)
    }

    // ========================================================================
    // Query generation
    // ========================================================================

    /// Convert to RediSearch query string.
    pub fn to_query(&self) -> String {
        match self {
            // Comparisons
            Predicate::Eq(field, value) => {
                if value.is_numeric() {
                    format!("@{}:[{} {}]", field, value, value)
                } else {
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
                format!("@{}:[({} +inf]", field, value)
            }
            Predicate::Gte(field, value) => {
                format!("@{}:[{} +inf]", field, value)
            }
            Predicate::Lt(field, value) => {
                format!("@{}:[-inf ({}]", field, value)
            }
            Predicate::Lte(field, value) => {
                format!("@{}:[-inf {}]", field, value)
            }
            Predicate::Between(field, min, max) => {
                format!("@{}:[{} {}]", field, min, max)
            }

            // Logical
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
            Predicate::Not(inner) => {
                format!("-({})", inner.to_query())
            }

            // Text search
            Predicate::TextSearch(field, term) => {
                format!("@{}:{}", field, escape_text_value(term))
            }
            Predicate::Prefix(field, prefix) => {
                format!("@{}:{}*", field, escape_text_value(prefix))
            }
            Predicate::Suffix(field, suffix) => {
                format!("@{}:*{}", field, escape_text_value(suffix))
            }
            Predicate::Wildcard(field, pattern) => {
                format!("@{}:{}", field, pattern)
            }
            Predicate::Fuzzy(field, term, distance) => {
                let pct = "%".repeat(*distance as usize);
                format!("@{}:{}{}{}", field, pct, escape_text_value(term), pct)
            }
            Predicate::Phrase(field, words) => {
                format!("@{}:({})", field, words.join(" "))
            }

            // Tags
            Predicate::Tag(field, tag) => {
                format!("@{}:{{{}}}", field, escape_tag_value(tag))
            }
            Predicate::TagOr(field, tags) => {
                let escaped: Vec<String> = tags.iter().map(|t| escape_tag_value(t)).collect();
                format!("@{}:{{{}}}", field, escaped.join("|"))
            }

            // Geo
            Predicate::GeoRadius(field, lon, lat, radius, unit) => {
                format!("@{}:[{} {} {} {}]", field, lon, lat, radius, unit)
            }

            // Null checks
            Predicate::IsMissing(field) => {
                format!("ismissing(@{})", field)
            }
            Predicate::IsNotMissing(field) => {
                format!("-ismissing(@{})", field)
            }

            // Boost
            Predicate::Boost(inner, weight) => {
                format!("({}) => {{ $weight: {}; }}", inner.to_query(), weight)
            }

            // Raw
            Predicate::Raw(query) => query.clone(),
        }
    }
}

/// Escape special characters in TAG values.
fn escape_tag_value(s: &str) -> String {
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

/// Escape special characters in TEXT search values.
fn escape_text_value(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '@' | '{' | '}' | '[' | ']' | '(' | ')' | '|' | '-' | '~' => {
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

    // Comparison tests
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
    fn test_ne() {
        let pred = Predicate::ne("status", "deleted");
        assert_eq!(pred.to_query(), "-@status:{deleted}");
    }

    // Logical tests
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
    fn test_not() {
        let pred = Predicate::eq("status", "deleted").negate();
        assert_eq!(pred.to_query(), "-(@status:{deleted})");
    }

    #[test]
    fn test_complex_and_or() {
        let pred = Predicate::gt("age", 30)
            .and(Predicate::eq("status", "active"))
            .or(Predicate::lt("age", 20));
        let query = pred.to_query();
        assert!(query.contains("@age:[(30 +inf]"));
        assert!(query.contains("@status:{active}"));
        assert!(query.contains("|"));
    }

    // Text search tests
    #[test]
    fn test_text_search() {
        let pred = Predicate::text_search("title", "python");
        assert_eq!(pred.to_query(), "@title:python");
    }

    #[test]
    fn test_prefix() {
        let pred = Predicate::prefix("name", "jo");
        assert_eq!(pred.to_query(), "@name:jo*");
    }

    #[test]
    fn test_suffix() {
        let pred = Predicate::suffix("name", "son");
        assert_eq!(pred.to_query(), "@name:*son");
    }

    #[test]
    fn test_fuzzy() {
        let pred = Predicate::fuzzy("name", "john", 1);
        assert_eq!(pred.to_query(), "@name:%john%");

        let pred2 = Predicate::fuzzy("name", "john", 2);
        assert_eq!(pred2.to_query(), "@name:%%john%%");
    }

    #[test]
    fn test_phrase() {
        let pred = Predicate::phrase("title", vec!["hello", "world"]);
        assert_eq!(pred.to_query(), "@title:(hello world)");
    }

    // Tag tests
    #[test]
    fn test_tag() {
        let pred = Predicate::tag("category", "science");
        assert_eq!(pred.to_query(), "@category:{science}");
    }

    #[test]
    fn test_tag_or() {
        let pred = Predicate::tag_or("tags", vec!["urgent", "important"]);
        assert_eq!(pred.to_query(), "@tags:{urgent|important}");
    }

    // Geo tests
    #[test]
    fn test_geo_radius() {
        let pred = Predicate::geo_radius("location", -122.4, 37.7, 10.0, "km");
        assert_eq!(pred.to_query(), "@location:[-122.4 37.7 10 km]");
    }

    // Null tests
    #[test]
    fn test_is_missing() {
        let pred = Predicate::is_missing("email");
        assert_eq!(pred.to_query(), "ismissing(@email)");
    }

    #[test]
    fn test_is_not_missing() {
        let pred = Predicate::is_not_missing("email");
        assert_eq!(pred.to_query(), "-ismissing(@email)");
    }

    // Boost tests
    #[test]
    fn test_boost() {
        let pred = Predicate::text_search("title", "python").boost(2.0);
        assert_eq!(pred.to_query(), "(@title:python) => { $weight: 2; }");
    }

    // Builder tests
    #[test]
    fn test_builder() {
        let query = PredicateBuilder::new()
            .and(Predicate::gt("age", 30))
            .and(Predicate::tag("status", "active"))
            .build();
        assert_eq!(query, "@age:[(30 +inf] @status:{active}");
    }

    #[test]
    fn test_builder_empty() {
        let query = PredicateBuilder::new().build();
        assert_eq!(query, "*");
    }

    // Escaping tests
    #[test]
    fn test_escape_tag_value() {
        let pred = Predicate::tag("email", "user@example.com");
        assert_eq!(pred.to_query(), r"@email:{user\@example\.com}");
    }

    #[test]
    fn test_float_values() {
        let pred = Predicate::gt("score", 3.5);
        assert_eq!(pred.to_query(), "@score:[(3.5 +inf]");
    }
}
