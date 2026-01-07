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
    /// Infix/contains match: `@field:*substring*`
    Infix(String, String),
    /// Wildcard match: `@field:pattern`
    Wildcard(String, String),
    /// Exact wildcard match: `@field:"w'pattern'"`
    WildcardExact(String, String),
    /// Fuzzy match: `@field:%term%` (distance 1), `@field:%%term%%` (distance 2)
    Fuzzy(String, String, u8),
    /// Phrase search: `@field:(word1 word2 word3)`
    Phrase(String, Vec<String>),
    /// Phrase search with slop and inorder: `@field:(word1 word2)=>{$slop:2;$inorder:true}`
    PhraseWithOptions {
        field: String,
        words: Vec<String>,
        slop: Option<u32>,
        inorder: Option<bool>,
    },
    /// Optional term: `~@field:term` (boosts score but not required)
    Optional(Box<Predicate>),

    // ========================================================================
    // Tag operations
    // ========================================================================
    /// Tag match: `@field:{tag}`
    Tag(String, String),
    /// Tag OR: `@field:{tag1|tag2|tag3}`
    TagOr(String, Vec<String>),

    // ========================================================================
    // Multi-field search
    // ========================================================================
    /// Search across multiple fields: `@field1|field2|field3:term`
    MultiFieldSearch(Vec<String>, String),

    // ========================================================================
    // Geo operations
    // ========================================================================
    /// Geo radius: `@field:[lon lat radius unit]`
    GeoRadius(String, f64, f64, f64, String),
    /// Geo polygon filter using WITHIN
    GeoPolygon {
        field: String,
        /// Points as (lon, lat) pairs forming a closed polygon
        points: Vec<(f64, f64)>,
    },

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
    // Vector search (KNN)
    // ========================================================================
    /// KNN vector search: `*=>[KNN k @field $vec]`
    VectorKnn {
        field: String,
        k: usize,
        /// Vector as bytes (to be passed as PARAMS)
        vector_param: String,
        /// Optional pre-filter query
        pre_filter: Option<Box<Predicate>>,
    },
    /// Vector range search: `@field:[VECTOR_RANGE radius $vec]`
    VectorRange {
        field: String,
        radius: f64,
        vector_param: String,
    },

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

    /// Create an infix/contains match predicate: `*substring*`
    pub fn infix(field: impl Into<String>, substring: impl Into<String>) -> Self {
        Predicate::Infix(field.into(), substring.into())
    }

    /// Create a wildcard match predicate with simple wildcards.
    pub fn wildcard(field: impl Into<String>, pattern: impl Into<String>) -> Self {
        Predicate::Wildcard(field.into(), pattern.into())
    }

    /// Create an exact wildcard match predicate: `w'pattern'`
    /// Supports `*` (any chars) and `?` (single char) wildcards.
    pub fn wildcard_exact(field: impl Into<String>, pattern: impl Into<String>) -> Self {
        Predicate::WildcardExact(field.into(), pattern.into())
    }

    /// Create a fuzzy match predicate.
    pub fn fuzzy(field: impl Into<String>, term: impl Into<String>, distance: u8) -> Self {
        Predicate::Fuzzy(field.into(), term.into(), distance.clamp(1, 3))
    }

    /// Create a phrase search predicate.
    pub fn phrase(field: impl Into<String>, words: Vec<impl Into<String>>) -> Self {
        Predicate::Phrase(field.into(), words.into_iter().map(|w| w.into()).collect())
    }

    /// Create a phrase search predicate with slop and inorder options.
    ///
    /// # Arguments
    /// * `field` - The field to search
    /// * `words` - Words that should appear in the phrase
    /// * `slop` - Number of intervening terms allowed (None = exact match)
    /// * `inorder` - Whether words must appear in order (None = any order)
    pub fn phrase_with_options(
        field: impl Into<String>,
        words: Vec<impl Into<String>>,
        slop: Option<u32>,
        inorder: Option<bool>,
    ) -> Self {
        Predicate::PhraseWithOptions {
            field: field.into(),
            words: words.into_iter().map(|w| w.into()).collect(),
            slop,
            inorder,
        }
    }

    /// Mark a predicate as optional (boosts score but not required).
    /// Generates: `~(query)`
    pub fn optional(self) -> Self {
        Predicate::Optional(Box::new(self))
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
    // Multi-field constructors
    // ========================================================================

    /// Create a multi-field text search: `@field1|field2:term`
    pub fn multi_field_search(fields: Vec<impl Into<String>>, term: impl Into<String>) -> Self {
        Predicate::MultiFieldSearch(fields.into_iter().map(|f| f.into()).collect(), term.into())
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

    /// Create a geo polygon predicate.
    /// Points should form a closed polygon (first and last point should be the same).
    pub fn geo_polygon(field: impl Into<String>, points: Vec<(f64, f64)>) -> Self {
        Predicate::GeoPolygon {
            field: field.into(),
            points,
        }
    }

    // ========================================================================
    // Vector search constructors
    // ========================================================================

    /// Create a KNN vector search predicate.
    ///
    /// # Arguments
    /// * `field` - The vector field name
    /// * `k` - Number of nearest neighbors to return
    /// * `vector_param` - Parameter name for the vector (will be passed via PARAMS)
    pub fn vector_knn(field: impl Into<String>, k: usize, vector_param: impl Into<String>) -> Self {
        Predicate::VectorKnn {
            field: field.into(),
            k,
            vector_param: vector_param.into(),
            pre_filter: None,
        }
    }

    /// Create a KNN vector search with pre-filter.
    pub fn vector_knn_with_filter(
        field: impl Into<String>,
        k: usize,
        vector_param: impl Into<String>,
        pre_filter: Predicate,
    ) -> Self {
        Predicate::VectorKnn {
            field: field.into(),
            k,
            vector_param: vector_param.into(),
            pre_filter: Some(Box::new(pre_filter)),
        }
    }

    /// Create a vector range search predicate.
    pub fn vector_range(
        field: impl Into<String>,
        radius: f64,
        vector_param: impl Into<String>,
    ) -> Self {
        Predicate::VectorRange {
            field: field.into(),
            radius,
            vector_param: vector_param.into(),
        }
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

    /// Get parameters that need to be passed to FT.SEARCH via PARAMS.
    /// Returns a list of (name, value) pairs.
    pub fn get_params(&self) -> Vec<(String, String)> {
        let mut params = Vec::new();
        self.collect_params(&mut params);
        params
    }

    /// Internal helper to collect params recursively.
    fn collect_params(&self, params: &mut Vec<(String, String)>) {
        match self {
            Predicate::GeoPolygon { points, .. } => {
                let coords: Vec<String> = points
                    .iter()
                    .map(|(lon, lat)| format!("{} {}", lon, lat))
                    .collect();
                let wkt = format!("POLYGON(({}))", coords.join(", "));
                params.push(("poly".to_string(), wkt));
            }
            Predicate::VectorKnn {
                vector_param,
                pre_filter,
                ..
            } => {
                // Vector data needs to be provided externally
                // We just note the param name here
                params.push((vector_param.clone(), String::new()));
                if let Some(filter) = pre_filter {
                    filter.collect_params(params);
                }
            }
            Predicate::VectorRange { vector_param, .. } => {
                params.push((vector_param.clone(), String::new()));
            }
            Predicate::And(preds) | Predicate::Or(preds) => {
                for p in preds {
                    p.collect_params(params);
                }
            }
            Predicate::Not(inner) | Predicate::Optional(inner) | Predicate::Boost(inner, _) => {
                inner.collect_params(params);
            }
            _ => {}
        }
    }

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
            Predicate::Infix(field, substring) => {
                format!("@{}:*{}*", field, escape_text_value(substring))
            }
            Predicate::Wildcard(field, pattern) => {
                format!("@{}:{}", field, pattern)
            }
            Predicate::WildcardExact(field, pattern) => {
                format!("@{}:\"w'{}\"", field, pattern)
            }
            Predicate::Fuzzy(field, term, distance) => {
                let pct = "%".repeat(*distance as usize);
                format!("@{}:{}{}{}", field, pct, escape_text_value(term), pct)
            }
            Predicate::Phrase(field, words) => {
                format!("@{}:({})", field, words.join(" "))
            }
            Predicate::PhraseWithOptions {
                field,
                words,
                slop,
                inorder,
            } => {
                let phrase = words.join(" ");
                let mut attrs = Vec::new();
                if let Some(s) = slop {
                    attrs.push(format!("$slop: {}", s));
                }
                if let Some(io) = inorder {
                    attrs.push(format!("$inorder: {}", io));
                }
                if attrs.is_empty() {
                    format!("@{}:({})", field, phrase)
                } else {
                    format!("@{}:({}) => {{ {}; }}", field, phrase, attrs.join("; "))
                }
            }
            Predicate::Optional(inner) => {
                format!("~{}", inner.to_query())
            }

            // Tags
            Predicate::Tag(field, tag) => {
                format!("@{}:{{{}}}", field, escape_tag_value(tag))
            }
            Predicate::TagOr(field, tags) => {
                let escaped: Vec<String> = tags.iter().map(|t| escape_tag_value(t)).collect();
                format!("@{}:{{{}}}", field, escaped.join("|"))
            }

            // Multi-field search
            Predicate::MultiFieldSearch(fields, term) => {
                format!("@{}:{}", fields.join("|"), escape_text_value(term))
            }

            // Geo
            Predicate::GeoRadius(field, lon, lat, radius, unit) => {
                format!("@{}:[{} {} {} {}]", field, lon, lat, radius, unit)
            }
            Predicate::GeoPolygon { field, points } => {
                // Format: @field:[WITHIN $poly] with PARAMS containing WKT polygon
                // For query string, we output the WITHIN syntax
                // The actual polygon data needs to be passed via PARAMS
                // Points are (lon, lat) pairs
                let _coords: Vec<String> = points
                    .iter()
                    .map(|(lon, lat)| format!("{} {}", lon, lat))
                    .collect();
                // Note: The actual WKT polygon is passed via PARAMS 2 poly "POLYGON((...))""
                format!("@{}:[WITHIN $poly]", field)
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

            // Vector search
            Predicate::VectorKnn {
                field,
                k,
                vector_param,
                pre_filter,
            } => {
                let filter = pre_filter
                    .as_ref()
                    .map(|p| p.to_query())
                    .unwrap_or_else(|| "*".to_string());
                format!("{}=>[KNN {} @{} ${}]", filter, k, field, vector_param)
            }
            Predicate::VectorRange {
                field,
                radius,
                vector_param,
            } => {
                format!("@{}:[VECTOR_RANGE {} ${}]", field, radius, vector_param)
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

    // =========================================================================
    // New feature tests
    // =========================================================================

    // Infix/contains match
    #[test]
    fn test_infix() {
        let pred = Predicate::infix("name", "sun");
        assert_eq!(pred.to_query(), "@name:*sun*");
    }

    // Wildcard exact match
    #[test]
    fn test_wildcard_exact() {
        let pred = Predicate::wildcard_exact("name", "foo*bar?");
        assert_eq!(pred.to_query(), "@name:\"w'foo*bar?\"");
    }

    // Phrase with slop and inorder
    #[test]
    fn test_phrase_with_slop() {
        let pred = Predicate::phrase_with_options("title", vec!["hello", "world"], Some(2), None);
        assert_eq!(pred.to_query(), "@title:(hello world) => { $slop: 2; }");
    }

    #[test]
    fn test_phrase_with_inorder() {
        let pred =
            Predicate::phrase_with_options("title", vec!["hello", "world"], None, Some(true));
        assert_eq!(
            pred.to_query(),
            "@title:(hello world) => { $inorder: true; }"
        );
    }

    #[test]
    fn test_phrase_with_slop_and_inorder() {
        let pred =
            Predicate::phrase_with_options("title", vec!["hello", "world"], Some(2), Some(true));
        assert_eq!(
            pred.to_query(),
            "@title:(hello world) => { $slop: 2; $inorder: true; }"
        );
    }

    // Optional terms
    #[test]
    fn test_optional() {
        let pred = Predicate::text_search("title", "python").optional();
        assert_eq!(pred.to_query(), "~@title:python");
    }

    #[test]
    fn test_optional_combined() {
        let required = Predicate::text_search("title", "redis");
        let optional = Predicate::text_search("title", "tutorial").optional();
        let pred = required.and(optional);
        assert_eq!(pred.to_query(), "@title:redis ~@title:tutorial");
    }

    // Multi-field search
    #[test]
    fn test_multi_field_search() {
        let pred = Predicate::multi_field_search(vec!["title", "body"], "python");
        assert_eq!(pred.to_query(), "@title|body:python");
    }

    #[test]
    fn test_multi_field_search_three_fields() {
        let pred = Predicate::multi_field_search(vec!["title", "body", "summary"], "redis");
        assert_eq!(pred.to_query(), "@title|body|summary:redis");
    }

    // Geo polygon
    #[test]
    fn test_geo_polygon() {
        let points = vec![
            (0.0, 0.0),
            (0.0, 10.0),
            (10.0, 10.0),
            (10.0, 0.0),
            (0.0, 0.0),
        ];
        let pred = Predicate::geo_polygon("location", points);
        assert_eq!(pred.to_query(), "@location:[WITHIN $poly]");
    }

    #[test]
    fn test_geo_polygon_params() {
        let points = vec![
            (0.0, 0.0),
            (0.0, 10.0),
            (10.0, 10.0),
            (10.0, 0.0),
            (0.0, 0.0),
        ];
        let pred = Predicate::geo_polygon("location", points);
        let params = pred.get_params();
        assert_eq!(params.len(), 1);
        assert_eq!(params[0].0, "poly");
        assert!(params[0].1.starts_with("POLYGON(("));
    }

    // Vector KNN search
    #[test]
    fn test_vector_knn() {
        let pred = Predicate::vector_knn("embedding", 10, "query_vec");
        assert_eq!(pred.to_query(), "*=>[KNN 10 @embedding $query_vec]");
    }

    #[test]
    fn test_vector_knn_with_filter() {
        let filter = Predicate::eq("category", "science");
        let pred = Predicate::vector_knn_with_filter("embedding", 10, "query_vec", filter);
        assert_eq!(
            pred.to_query(),
            "@category:{science}=>[KNN 10 @embedding $query_vec]"
        );
    }

    // Vector range search
    #[test]
    fn test_vector_range() {
        let pred = Predicate::vector_range("embedding", 0.5, "query_vec");
        assert_eq!(pred.to_query(), "@embedding:[VECTOR_RANGE 0.5 $query_vec]");
    }

    // =========================================================================
    // Property-Based Tests
    // =========================================================================

    mod proptest_tests {
        use super::*;
        use proptest::prelude::*;

        // Valid field name strategy (alphanumeric, starting with letter)
        fn field_name_strategy() -> impl Strategy<Value = String> {
            "[a-zA-Z][a-zA-Z0-9_]{0,20}".prop_map(String::from)
        }

        // Valid tag value strategy (alphanumeric, may contain underscores)
        fn simple_tag_value_strategy() -> impl Strategy<Value = String> {
            "[a-zA-Z0-9_]{1,20}".prop_map(String::from)
        }

        proptest! {
            /// Numeric equality predicates should always produce valid query format.
            #[test]
            fn prop_eq_numeric_format(
                field in field_name_strategy(),
                value in any::<i64>(),
            ) {
                let pred = Predicate::eq(&field, value);
                let query = pred.to_query();

                // Should have format @field:[value value]
                let expected_prefix = format!("@{}:[", field);
                prop_assert!(query.starts_with(&expected_prefix));
                prop_assert!(query.ends_with("]"));
                let value_str = value.to_string();
                prop_assert!(query.contains(&value_str));
            }

            /// Greater-than predicates should produce valid range syntax.
            #[test]
            fn prop_gt_format(
                field in field_name_strategy(),
                value in any::<i64>(),
            ) {
                let pred = Predicate::gt(&field, value);
                let query = pred.to_query();

                // Should have format @field:[(value +inf]
                let expected_prefix = format!("@{}:[(", field);
                prop_assert!(query.starts_with(&expected_prefix));
                prop_assert!(query.ends_with("+inf]"));
                let value_str = value.to_string();
                prop_assert!(query.contains(&value_str));
            }

            /// Less-than predicates should produce valid range syntax.
            #[test]
            fn prop_lt_format(
                field in field_name_strategy(),
                value in any::<i64>(),
            ) {
                let pred = Predicate::lt(&field, value);
                let query = pred.to_query();

                // Should have format @field:[-inf (value]
                let expected_prefix = format!("@{}:[-inf (", field);
                prop_assert!(query.starts_with(&expected_prefix));
                prop_assert!(query.ends_with("]"));
                let value_str = value.to_string();
                prop_assert!(query.contains(&value_str));
            }

            /// Between predicates should contain both bounds.
            #[test]
            fn prop_between_contains_bounds(
                field in field_name_strategy(),
                min in any::<i32>(),
                max in any::<i32>(),
            ) {
                let pred = Predicate::between(&field, min as i64, max as i64);
                let query = pred.to_query();

                let min_str = min.to_string();
                let max_str = max.to_string();
                prop_assert!(query.contains(&min_str));
                prop_assert!(query.contains(&max_str));
            }

            /// Tag predicates should escape special characters.
            #[test]
            fn prop_tag_escapes_special_chars(
                field in field_name_strategy(),
            ) {
                // Test with a value containing special characters
                let special_value = "user@example.com";
                let pred = Predicate::tag(&field, special_value);
                let query = pred.to_query();

                // @ should be escaped as \@
                prop_assert!(query.contains(r"\@"));
                // . should be escaped as \.
                prop_assert!(query.contains(r"\."));
            }

            /// AND of two predicates should contain both query parts.
            #[test]
            fn prop_and_contains_both(
                field1 in field_name_strategy(),
                value1 in any::<i32>(),
                field2 in field_name_strategy(),
                value2 in any::<i32>(),
            ) {
                let pred1 = Predicate::gt(&field1, value1 as i64);
                let pred2 = Predicate::lt(&field2, value2 as i64);
                let combined = pred1.and(pred2);
                let query = combined.to_query();

                let field1_ref = format!("@{}", field1);
                let field2_ref = format!("@{}", field2);
                prop_assert!(query.contains(&field1_ref));
                prop_assert!(query.contains(&field2_ref));
            }

            /// OR of two predicates should contain pipe separator.
            #[test]
            fn prop_or_contains_separator(
                field in field_name_strategy(),
                value1 in simple_tag_value_strategy(),
                value2 in simple_tag_value_strategy(),
            ) {
                let pred1 = Predicate::tag(&field, &value1);
                let pred2 = Predicate::tag(&field, &value2);
                let combined = pred1.or(pred2);
                let query = combined.to_query();

                prop_assert!(query.contains(" | "));
            }

            /// NOT should wrap query in negation.
            #[test]
            fn prop_not_wraps_in_negation(
                field in field_name_strategy(),
                value in simple_tag_value_strategy(),
            ) {
                let pred = Predicate::tag(&field, &value).negate();
                let query = pred.to_query();

                prop_assert!(query.starts_with("-("));
                prop_assert!(query.ends_with(")"));
            }

            /// Float values should be preserved in queries.
            #[test]
            fn prop_float_values_preserved(
                field in field_name_strategy(),
                value in any::<f64>().prop_filter("Must be finite", |v| v.is_finite()),
            ) {
                let pred = Predicate::gt(&field, value);
                let query = pred.to_query();

                // The value should appear in the query (possibly formatted differently)
                let expected_prefix = format!("@{}:[(", field);
                prop_assert!(query.contains(&expected_prefix));
            }

            /// Fuzzy distance should be clamped to 1-3.
            #[test]
            fn prop_fuzzy_distance_clamped(
                field in field_name_strategy(),
                term in "[a-z]{3,10}",
                distance in 0u8..=10,
            ) {
                let pred = Predicate::fuzzy(&field, &term, distance);
                let query = pred.to_query();

                // Count the % characters (should be 2*clamped_distance)
                let pct_count = query.chars().filter(|&c| c == '%').count();
                let expected_distance = distance.clamp(1, 3) as usize;
                prop_assert_eq!(pct_count, expected_distance * 2);
            }

            /// PredicateBuilder with multiple predicates should join with spaces.
            #[test]
            fn prop_builder_joins_predicates(
                count in 2usize..=5,
            ) {
                let builder = (0..count).fold(PredicateBuilder::new(), |b, i| {
                    b.and(Predicate::gt(format!("field{}", i), i as i64))
                });
                let query = builder.build();

                // Should have count-1 spaces between predicates (approximately)
                // Each predicate has format @fieldN:[(N +inf]
                for i in 0..count {
                    let field_ref = format!("@field{}:", i);
                    prop_assert!(query.contains(&field_ref));
                }
            }

            /// Empty builder should produce wildcard query.
            #[test]
            fn prop_empty_builder_is_wildcard(_unused in 0..1i32) {
                let query = PredicateBuilder::new().build();
                prop_assert_eq!(query, "*");
            }

            /// Geo radius should include all parameters.
            #[test]
            fn prop_geo_radius_format(
                field in field_name_strategy(),
                lon in -180.0f64..=180.0,
                lat in -90.0f64..=90.0,
                radius in 0.1f64..=1000.0,
            ) {
                let pred = Predicate::geo_radius(&field, lon, lat, radius, "km");
                let query = pred.to_query();

                let expected_prefix = format!("@{}:[", field);
                prop_assert!(query.starts_with(&expected_prefix));
                prop_assert!(query.ends_with("km]"));
            }

            /// Vector KNN should have proper format.
            #[test]
            fn prop_vector_knn_format(
                field in field_name_strategy(),
                k in 1usize..=100,
                param in "[a-z_]{3,10}",
            ) {
                let pred = Predicate::vector_knn(&field, k, &param);
                let query = pred.to_query();

                let field_ref = format!("@{}", field);
                let param_ref = format!("${}", param);
                let k_str = k.to_string();
                prop_assert!(query.starts_with("*=>[KNN"));
                prop_assert!(query.contains(&field_ref));
                prop_assert!(query.contains(&param_ref));
                prop_assert!(query.contains(&k_str));
            }

            /// TagOr should produce pipe-separated tags.
            #[test]
            fn prop_tag_or_format(
                field in field_name_strategy(),
                tags in proptest::collection::vec(simple_tag_value_strategy(), 2..=5),
            ) {
                let pred = Predicate::tag_or(&field, tags.clone());
                let query = pred.to_query();

                // Should have n-1 pipes for n tags
                let pipe_count = query.matches('|').count();
                prop_assert_eq!(pipe_count, tags.len() - 1);

                // All tags should be present
                for tag in &tags {
                    prop_assert!(query.contains(tag));
                }
            }

            /// Boost should wrap predicate with weight.
            #[test]
            fn prop_boost_format(
                field in field_name_strategy(),
                value in simple_tag_value_strategy(),
                weight in 0.1f64..=10.0,
            ) {
                let pred = Predicate::tag(&field, &value).boost(weight);
                let query = pred.to_query();

                prop_assert!(query.starts_with("("));
                prop_assert!(query.contains("$weight:"));
            }

            /// Multi-field search should join fields with pipes.
            #[test]
            fn prop_multi_field_format(
                fields in proptest::collection::vec(field_name_strategy(), 2..=4),
                term in "[a-z]{3,10}",
            ) {
                let pred = Predicate::multi_field_search(fields.clone(), &term);
                let query = pred.to_query();

                // Check fields are joined with |
                let field_part: String = fields.join("|");
                let expected = format!("@{}:", field_part);
                prop_assert!(query.contains(&expected));
            }
        }
    }
}
