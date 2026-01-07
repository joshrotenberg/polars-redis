//! RediSearch Index management for polars-redis.
//!
//! This module provides typed helpers for creating and managing RediSearch indexes,
//! eliminating the need to write raw FT.CREATE commands.
//!
//! # Example
//!
//! ```no_run
//! use polars_redis::index::{Index, TextField, NumericField, TagField};
//!
//! // Define an index
//! let idx = Index::new("users_idx")
//!     .with_prefix("user:")
//!     .with_field(TextField::new("name").sortable())
//!     .with_field(NumericField::new("age").sortable())
//!     .with_field(TagField::new("status"));
//!
//! // Create the index
//! idx.create("redis://localhost:6379").unwrap();
//!
//! // Or ensure it exists (idempotent)
//! idx.ensure_exists("redis://localhost:6379").unwrap();
//! ```

use std::collections::HashMap;

use redis::Connection;

use crate::error::{Error, Result};
use crate::schema::RedisType;

// =============================================================================
// Field Types
// =============================================================================

/// Trait for RediSearch field types.
pub trait Field: Send + Sync {
    /// Get the field name.
    fn name(&self) -> &str;

    /// Get the RediSearch field type name.
    fn field_type(&self) -> &str;

    /// Convert field to FT.CREATE arguments.
    fn to_args(&self) -> Vec<String>;
}

/// A TEXT field for full-text search.
///
/// TEXT fields support full-text search with stemming, phonetic matching,
/// and relevance scoring.
#[derive(Debug, Clone)]
pub struct TextField {
    name: String,
    sortable: bool,
    nostem: bool,
    weight: f64,
    phonetic: Option<String>,
    noindex: bool,
    withsuffixtrie: bool,
}

impl TextField {
    /// Create a new TEXT field.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            sortable: false,
            nostem: false,
            weight: 1.0,
            phonetic: None,
            noindex: false,
            withsuffixtrie: false,
        }
    }

    /// Enable sorting on this field.
    pub fn sortable(mut self) -> Self {
        self.sortable = true;
        self
    }

    /// Disable stemming for this field.
    pub fn nostem(mut self) -> Self {
        self.nostem = true;
        self
    }

    /// Set relevance weight for scoring (default 1.0).
    pub fn weight(mut self, weight: f64) -> Self {
        self.weight = weight;
        self
    }

    /// Set phonetic algorithm for fuzzy matching.
    pub fn phonetic(mut self, algorithm: impl Into<String>) -> Self {
        self.phonetic = Some(algorithm.into());
        self
    }

    /// Store field but don't index it.
    pub fn noindex(mut self) -> Self {
        self.noindex = true;
        self
    }

    /// Enable suffix queries (*word).
    pub fn withsuffixtrie(mut self) -> Self {
        self.withsuffixtrie = true;
        self
    }
}

impl Field for TextField {
    fn name(&self) -> &str {
        &self.name
    }

    fn field_type(&self) -> &str {
        "TEXT"
    }

    fn to_args(&self) -> Vec<String> {
        let mut args = vec![self.name.clone(), "TEXT".to_string()];
        if self.nostem {
            args.push("NOSTEM".to_string());
        }
        if (self.weight - 1.0).abs() > f64::EPSILON {
            args.push("WEIGHT".to_string());
            args.push(self.weight.to_string());
        }
        if let Some(ref phonetic) = self.phonetic {
            args.push("PHONETIC".to_string());
            args.push(phonetic.clone());
        }
        if self.sortable {
            args.push("SORTABLE".to_string());
        }
        if self.noindex {
            args.push("NOINDEX".to_string());
        }
        if self.withsuffixtrie {
            args.push("WITHSUFFIXTRIE".to_string());
        }
        args
    }
}

/// A NUMERIC field for numeric range queries.
#[derive(Debug, Clone)]
pub struct NumericField {
    name: String,
    sortable: bool,
    noindex: bool,
}

impl NumericField {
    /// Create a new NUMERIC field.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            sortable: false,
            noindex: false,
        }
    }

    /// Enable sorting on this field.
    pub fn sortable(mut self) -> Self {
        self.sortable = true;
        self
    }

    /// Store field but don't index it.
    pub fn noindex(mut self) -> Self {
        self.noindex = true;
        self
    }
}

impl Field for NumericField {
    fn name(&self) -> &str {
        &self.name
    }

    fn field_type(&self) -> &str {
        "NUMERIC"
    }

    fn to_args(&self) -> Vec<String> {
        let mut args = vec![self.name.clone(), "NUMERIC".to_string()];
        if self.sortable {
            args.push("SORTABLE".to_string());
        }
        if self.noindex {
            args.push("NOINDEX".to_string());
        }
        args
    }
}

/// A TAG field for exact-match filtering.
///
/// TAG fields are optimized for exact matches and multi-value fields
/// (like categories, tags, statuses).
#[derive(Debug, Clone)]
pub struct TagField {
    name: String,
    separator: String,
    casesensitive: bool,
    sortable: bool,
    noindex: bool,
    withsuffixtrie: bool,
}

impl TagField {
    /// Create a new TAG field.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            separator: ",".to_string(),
            casesensitive: false,
            sortable: false,
            noindex: false,
            withsuffixtrie: false,
        }
    }

    /// Set separator character for multiple values.
    pub fn separator(mut self, sep: impl Into<String>) -> Self {
        self.separator = sep.into();
        self
    }

    /// Enable case-sensitive matching.
    pub fn casesensitive(mut self) -> Self {
        self.casesensitive = true;
        self
    }

    /// Enable sorting on this field.
    pub fn sortable(mut self) -> Self {
        self.sortable = true;
        self
    }

    /// Store field but don't index it.
    pub fn noindex(mut self) -> Self {
        self.noindex = true;
        self
    }

    /// Enable suffix queries.
    pub fn withsuffixtrie(mut self) -> Self {
        self.withsuffixtrie = true;
        self
    }
}

impl Field for TagField {
    fn name(&self) -> &str {
        &self.name
    }

    fn field_type(&self) -> &str {
        "TAG"
    }

    fn to_args(&self) -> Vec<String> {
        let mut args = vec![self.name.clone(), "TAG".to_string()];
        if self.separator != "," {
            args.push("SEPARATOR".to_string());
            args.push(self.separator.clone());
        }
        if self.casesensitive {
            args.push("CASESENSITIVE".to_string());
        }
        if self.sortable {
            args.push("SORTABLE".to_string());
        }
        if self.noindex {
            args.push("NOINDEX".to_string());
        }
        if self.withsuffixtrie {
            args.push("WITHSUFFIXTRIE".to_string());
        }
        args
    }
}

/// A GEO field for geographic queries.
#[derive(Debug, Clone)]
pub struct GeoField {
    name: String,
    noindex: bool,
}

impl GeoField {
    /// Create a new GEO field.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            noindex: false,
        }
    }

    /// Store field but don't index it.
    pub fn noindex(mut self) -> Self {
        self.noindex = true;
        self
    }
}

impl Field for GeoField {
    fn name(&self) -> &str {
        &self.name
    }

    fn field_type(&self) -> &str {
        "GEO"
    }

    fn to_args(&self) -> Vec<String> {
        let mut args = vec![self.name.clone(), "GEO".to_string()];
        if self.noindex {
            args.push("NOINDEX".to_string());
        }
        args
    }
}

/// Vector similarity algorithm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorAlgorithm {
    /// Brute-force flat index.
    Flat,
    /// Hierarchical Navigable Small World graph (approximate).
    Hnsw,
}

impl VectorAlgorithm {
    fn as_str(&self) -> &str {
        match self {
            VectorAlgorithm::Flat => "FLAT",
            VectorAlgorithm::Hnsw => "HNSW",
        }
    }
}

/// Vector distance metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistanceMetric {
    /// Cosine similarity.
    Cosine,
    /// Euclidean distance (L2).
    L2,
    /// Inner product.
    Ip,
}

impl DistanceMetric {
    fn as_str(&self) -> &str {
        match self {
            DistanceMetric::Cosine => "COSINE",
            DistanceMetric::L2 => "L2",
            DistanceMetric::Ip => "IP",
        }
    }
}

/// A VECTOR field for similarity search.
#[derive(Debug, Clone)]
pub struct VectorField {
    name: String,
    algorithm: VectorAlgorithm,
    dim: usize,
    distance_metric: DistanceMetric,
    initial_cap: Option<usize>,
    m: Option<usize>,
    ef_construction: Option<usize>,
    ef_runtime: Option<usize>,
    block_size: Option<usize>,
}

impl VectorField {
    /// Create a new VECTOR field.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            algorithm: VectorAlgorithm::Hnsw,
            dim: 384,
            distance_metric: DistanceMetric::Cosine,
            initial_cap: None,
            m: None,
            ef_construction: None,
            ef_runtime: None,
            block_size: None,
        }
    }

    /// Set the indexing algorithm.
    pub fn algorithm(mut self, algo: VectorAlgorithm) -> Self {
        self.algorithm = algo;
        self
    }

    /// Set the vector dimension.
    pub fn dim(mut self, dim: usize) -> Self {
        self.dim = dim;
        self
    }

    /// Set the distance metric.
    pub fn distance_metric(mut self, metric: DistanceMetric) -> Self {
        self.distance_metric = metric;
        self
    }

    /// Set initial index capacity (HNSW).
    pub fn initial_cap(mut self, cap: usize) -> Self {
        self.initial_cap = Some(cap);
        self
    }

    /// Set M parameter (HNSW).
    pub fn m(mut self, m: usize) -> Self {
        self.m = Some(m);
        self
    }

    /// Set construction-time search width (HNSW).
    pub fn ef_construction(mut self, ef: usize) -> Self {
        self.ef_construction = Some(ef);
        self
    }

    /// Set query-time search width (HNSW).
    pub fn ef_runtime(mut self, ef: usize) -> Self {
        self.ef_runtime = Some(ef);
        self
    }

    /// Set block size (FLAT).
    pub fn block_size(mut self, size: usize) -> Self {
        self.block_size = Some(size);
        self
    }
}

impl Field for VectorField {
    fn name(&self) -> &str {
        &self.name
    }

    fn field_type(&self) -> &str {
        "VECTOR"
    }

    fn to_args(&self) -> Vec<String> {
        let mut args = vec![
            self.name.clone(),
            "VECTOR".to_string(),
            self.algorithm.as_str().to_string(),
        ];

        // Build attributes
        let mut attrs: Vec<String> = vec![
            "TYPE".to_string(),
            "FLOAT32".to_string(),
            "DIM".to_string(),
            self.dim.to_string(),
            "DISTANCE_METRIC".to_string(),
            self.distance_metric.as_str().to_string(),
        ];

        if let Some(cap) = self.initial_cap {
            attrs.push("INITIAL_CAP".to_string());
            attrs.push(cap.to_string());
        }

        if self.algorithm == VectorAlgorithm::Hnsw {
            if let Some(m) = self.m {
                attrs.push("M".to_string());
                attrs.push(m.to_string());
            }
            if let Some(ef) = self.ef_construction {
                attrs.push("EF_CONSTRUCTION".to_string());
                attrs.push(ef.to_string());
            }
            if let Some(ef) = self.ef_runtime {
                attrs.push("EF_RUNTIME".to_string());
                attrs.push(ef.to_string());
            }
        } else if let Some(size) = self.block_size {
            attrs.push("BLOCK_SIZE".to_string());
            attrs.push(size.to_string());
        }

        args.push(attrs.len().to_string());
        args.extend(attrs);
        args
    }
}

/// A GEOSHAPE field for polygon and complex geometry queries.
#[derive(Debug, Clone)]
pub struct GeoShapeField {
    name: String,
    coord_system: String,
}

impl GeoShapeField {
    /// Create a new GEOSHAPE field.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            coord_system: "SPHERICAL".to_string(),
        }
    }

    /// Set coordinate system ("SPHERICAL" or "FLAT").
    pub fn coord_system(mut self, system: impl Into<String>) -> Self {
        self.coord_system = system.into();
        self
    }
}

impl Field for GeoShapeField {
    fn name(&self) -> &str {
        &self.name
    }

    fn field_type(&self) -> &str {
        "GEOSHAPE"
    }

    fn to_args(&self) -> Vec<String> {
        let mut args = vec![self.name.clone(), "GEOSHAPE".to_string()];
        if self.coord_system != "SPHERICAL" {
            args.push("COORD_SYSTEM".to_string());
            args.push(self.coord_system.clone());
        }
        args
    }
}

// =============================================================================
// Index Definition
// =============================================================================

/// Data type for the index (HASH or JSON).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IndexType {
    /// Index Redis hashes.
    #[default]
    Hash,
    /// Index RedisJSON documents.
    Json,
}

impl IndexType {
    fn as_str(&self) -> &str {
        match self {
            IndexType::Hash => "HASH",
            IndexType::Json => "JSON",
        }
    }
}

/// Differences between desired and existing index schemas.
#[derive(Debug, Default)]
pub struct IndexDiff {
    /// Fields that will be added.
    pub added: Vec<String>,
    /// Fields that will be removed.
    pub removed: Vec<String>,
    /// Fields that have changed (field_name -> (old_type, new_type)).
    pub changed: HashMap<String, (String, String)>,
    /// Fields that are unchanged.
    pub unchanged: Vec<String>,
}

impl IndexDiff {
    /// True if there are any differences.
    pub fn has_changes(&self) -> bool {
        !self.added.is_empty() || !self.removed.is_empty() || !self.changed.is_empty()
    }
}

/// Information about an existing RediSearch index.
#[derive(Debug)]
pub struct IndexInfo {
    /// Index name.
    pub name: String,
    /// Number of indexed documents.
    pub num_docs: u64,
    /// Field information.
    pub fields: Vec<(String, String)>,
    /// Key prefixes.
    pub prefixes: Vec<String>,
    /// Index type (HASH or JSON).
    pub index_type: IndexType,
}

/// A RediSearch index definition.
///
/// Provides a typed interface for creating and managing RediSearch indexes.
pub struct Index {
    name: String,
    prefixes: Vec<String>,
    fields: Vec<Box<dyn Field + Send + Sync>>,
    index_type: IndexType,
    stopwords: Option<Vec<String>>,
    language: Option<String>,
    language_field: Option<String>,
    score: Option<f64>,
    score_field: Option<String>,
    payload_field: Option<String>,
    maxtextfields: bool,
    nooffsets: bool,
    nohl: bool,
    nofields: bool,
    nofreqs: bool,
    skipinitialscan: bool,
}

impl Index {
    /// Create a new index definition.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            prefixes: Vec::new(),
            fields: Vec::new(),
            index_type: IndexType::Hash,
            stopwords: None,
            language: None,
            language_field: None,
            score: None,
            score_field: None,
            payload_field: None,
            maxtextfields: false,
            nooffsets: false,
            nohl: false,
            nofields: false,
            nofreqs: false,
            skipinitialscan: false,
        }
    }

    /// Get the index name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Set a single key prefix.
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefixes = vec![prefix.into()];
        self
    }

    /// Set multiple key prefixes.
    pub fn with_prefixes(mut self, prefixes: Vec<String>) -> Self {
        self.prefixes = prefixes;
        self
    }

    /// Add a field to the index.
    pub fn with_field<F: Field + Send + Sync + 'static>(mut self, field: F) -> Self {
        self.fields.push(Box::new(field));
        self
    }

    /// Set the index type (HASH or JSON).
    pub fn with_type(mut self, index_type: IndexType) -> Self {
        self.index_type = index_type;
        self
    }

    /// Set custom stopwords.
    pub fn with_stopwords(mut self, stopwords: Vec<String>) -> Self {
        self.stopwords = Some(stopwords);
        self
    }

    /// Disable stopwords.
    pub fn without_stopwords(mut self) -> Self {
        self.stopwords = Some(Vec::new());
        self
    }

    /// Set default language for stemming.
    pub fn with_language(mut self, language: impl Into<String>) -> Self {
        self.language = Some(language.into());
        self
    }

    /// Set field containing per-document language.
    pub fn with_language_field(mut self, field: impl Into<String>) -> Self {
        self.language_field = Some(field.into());
        self
    }

    /// Set default document score.
    pub fn with_score(mut self, score: f64) -> Self {
        self.score = Some(score);
        self
    }

    /// Set field containing per-document score.
    pub fn with_score_field(mut self, field: impl Into<String>) -> Self {
        self.score_field = Some(field.into());
        self
    }

    /// Set payload field.
    pub fn with_payload_field(mut self, field: impl Into<String>) -> Self {
        self.payload_field = Some(field.into());
        self
    }

    /// Optimize for many TEXT fields.
    pub fn maxtextfields(mut self) -> Self {
        self.maxtextfields = true;
        self
    }

    /// Don't store term offsets.
    pub fn nooffsets(mut self) -> Self {
        self.nooffsets = true;
        self
    }

    /// Don't store data for highlighting.
    pub fn nohl(mut self) -> Self {
        self.nohl = true;
        self
    }

    /// Don't store field names.
    pub fn nofields(mut self) -> Self {
        self.nofields = true;
        self
    }

    /// Don't store term frequencies.
    pub fn nofreqs(mut self) -> Self {
        self.nofreqs = true;
        self
    }

    /// Don't scan existing keys when creating.
    pub fn skipinitialscan(mut self) -> Self {
        self.skipinitialscan = true;
        self
    }

    /// Build the FT.CREATE command arguments.
    fn build_create_args(&self) -> Vec<String> {
        let mut args = vec![self.name.clone()];

        // ON HASH/JSON
        args.push("ON".to_string());
        args.push(self.index_type.as_str().to_string());

        // PREFIX
        if !self.prefixes.is_empty() {
            args.push("PREFIX".to_string());
            args.push(self.prefixes.len().to_string());
            args.extend(self.prefixes.clone());
        }

        // Index options
        if let Some(ref lang) = self.language {
            args.push("LANGUAGE".to_string());
            args.push(lang.clone());
        }
        if let Some(ref field) = self.language_field {
            args.push("LANGUAGE_FIELD".to_string());
            args.push(field.clone());
        }
        if let Some(score) = self.score {
            args.push("SCORE".to_string());
            args.push(score.to_string());
        }
        if let Some(ref field) = self.score_field {
            args.push("SCORE_FIELD".to_string());
            args.push(field.clone());
        }
        if let Some(ref field) = self.payload_field {
            args.push("PAYLOAD_FIELD".to_string());
            args.push(field.clone());
        }
        if self.maxtextfields {
            args.push("MAXTEXTFIELDS".to_string());
        }
        if self.nooffsets {
            args.push("NOOFFSETS".to_string());
        }
        if self.nohl {
            args.push("NOHL".to_string());
        }
        if self.nofields {
            args.push("NOFIELDS".to_string());
        }
        if self.nofreqs {
            args.push("NOFREQS".to_string());
        }
        if self.skipinitialscan {
            args.push("SKIPINITIALSCAN".to_string());
        }
        if let Some(ref stopwords) = self.stopwords {
            args.push("STOPWORDS".to_string());
            args.push(stopwords.len().to_string());
            args.extend(stopwords.clone());
        }

        // SCHEMA
        args.push("SCHEMA".to_string());
        for field in &self.fields {
            args.extend(field.to_args());
        }

        args
    }

    /// Create the index in Redis.
    pub fn create(&self, url: &str) -> Result<()> {
        let client = redis::Client::open(url)?;
        let mut conn = client.get_connection()?;
        self.create_with_conn(&mut conn)
    }

    /// Create the index using an existing connection.
    pub fn create_with_conn(&self, conn: &mut Connection) -> Result<()> {
        let args = self.build_create_args();
        redis::cmd("FT.CREATE")
            .arg(&args)
            .query::<()>(conn)
            .map_err(Error::Connection)
    }

    /// Create the index if it doesn't exist.
    pub fn create_if_not_exists(&self, url: &str) -> Result<()> {
        let client = redis::Client::open(url)?;
        let mut conn = client.get_connection()?;
        self.create_if_not_exists_with_conn(&mut conn)
    }

    /// Create the index if it doesn't exist, using an existing connection.
    pub fn create_if_not_exists_with_conn(&self, conn: &mut Connection) -> Result<()> {
        match self.create_with_conn(conn) {
            Ok(()) => Ok(()),
            Err(Error::Connection(e)) if e.to_string().contains("Index already exists") => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Drop the index.
    pub fn drop(&self, url: &str) -> Result<()> {
        let client = redis::Client::open(url)?;
        let mut conn = client.get_connection()?;
        self.drop_with_conn(&mut conn, false)
    }

    /// Drop the index, optionally deleting documents.
    pub fn drop_with_docs(&self, url: &str) -> Result<()> {
        let client = redis::Client::open(url)?;
        let mut conn = client.get_connection()?;
        self.drop_with_conn(&mut conn, true)
    }

    /// Drop the index using an existing connection.
    pub fn drop_with_conn(&self, conn: &mut Connection, delete_docs: bool) -> Result<()> {
        let mut cmd = redis::cmd("FT.DROPINDEX");
        cmd.arg(&self.name);
        if delete_docs {
            cmd.arg("DD");
        }
        match cmd.query::<()>(conn) {
            Ok(()) => Ok(()),
            Err(e) if e.to_string().contains("Unknown index name") => Ok(()),
            Err(e) => Err(Error::Connection(e)),
        }
    }

    /// Check if the index exists.
    pub fn exists(&self, url: &str) -> Result<bool> {
        let client = redis::Client::open(url)?;
        let mut conn = client.get_connection()?;
        self.exists_with_conn(&mut conn)
    }

    /// Check if the index exists using an existing connection.
    pub fn exists_with_conn(&self, conn: &mut Connection) -> Result<bool> {
        match redis::cmd("FT.INFO")
            .arg(&self.name)
            .query::<Vec<redis::Value>>(conn)
        {
            Ok(_) => Ok(true),
            Err(e) if e.to_string().contains("Unknown index name") => Ok(false),
            Err(e) => Err(Error::Connection(e)),
        }
    }

    /// Ensure the index exists, creating it if necessary.
    ///
    /// This is idempotent and safe for concurrent access.
    pub fn ensure_exists(&self, url: &str) -> Result<()> {
        self.create_if_not_exists(url)
    }

    /// Ensure the index exists using an existing connection.
    pub fn ensure_exists_with_conn(&self, conn: &mut Connection) -> Result<()> {
        self.create_if_not_exists_with_conn(conn)
    }

    /// Recreate the index (drop and create).
    pub fn recreate(&self, url: &str) -> Result<()> {
        let client = redis::Client::open(url)?;
        let mut conn = client.get_connection()?;
        self.drop_with_conn(&mut conn, false)?;
        self.create_with_conn(&mut conn)
    }

    /// Get the FT.CREATE command as a string (for debugging).
    pub fn to_command_string(&self) -> String {
        let args = self.build_create_args();
        format!("FT.CREATE {}", args.join(" "))
    }

    /// Create an index from a schema (field name to RedisType mapping).
    ///
    /// String types become TAG fields by default. Use `text_fields` to specify
    /// which should be TEXT fields for full-text search.
    pub fn from_schema(
        name: impl Into<String>,
        prefix: impl Into<String>,
        schema: &[(String, RedisType)],
        text_fields: &[&str],
        sortable_fields: &[&str],
    ) -> Self {
        let text_set: std::collections::HashSet<&str> = text_fields.iter().copied().collect();
        let sortable_set: std::collections::HashSet<&str> =
            sortable_fields.iter().copied().collect();

        let mut index = Index::new(name).with_prefix(prefix);

        for (field_name, redis_type) in schema {
            let is_sortable = sortable_set.contains(field_name.as_str());

            match redis_type {
                RedisType::Int64 | RedisType::Float64 => {
                    let mut field = NumericField::new(field_name);
                    if is_sortable {
                        field = field.sortable();
                    }
                    index = index.with_field(field);
                }
                RedisType::Utf8 => {
                    if text_set.contains(field_name.as_str()) {
                        let mut field = TextField::new(field_name);
                        if is_sortable {
                            field = field.sortable();
                        }
                        index = index.with_field(field);
                    } else {
                        let mut field = TagField::new(field_name);
                        if is_sortable {
                            field = field.sortable();
                        }
                        index = index.with_field(field);
                    }
                }
                RedisType::Boolean => {
                    let mut field = TagField::new(field_name);
                    if is_sortable {
                        field = field.sortable();
                    }
                    index = index.with_field(field);
                }
                RedisType::Date | RedisType::Datetime => {
                    // Date/Datetime are stored as strings, treat as TAG for exact match
                    // or use NUMERIC if stored as timestamps
                    let mut field = TagField::new(field_name);
                    if is_sortable {
                        field = field.sortable();
                    }
                    index = index.with_field(field);
                }
            }
        }

        index
    }
}

impl std::fmt::Display for Index {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_command_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_text_field_args() {
        let field = TextField::new("name").sortable().weight(2.0);
        let args = field.to_args();
        assert!(args.contains(&"TEXT".to_string()));
        assert!(args.contains(&"SORTABLE".to_string()));
        assert!(args.contains(&"WEIGHT".to_string()));
        assert!(args.contains(&"2".to_string()));
    }

    #[test]
    fn test_numeric_field_args() {
        let field = NumericField::new("age").sortable();
        let args = field.to_args();
        assert_eq!(args, vec!["age", "NUMERIC", "SORTABLE"]);
    }

    #[test]
    fn test_tag_field_args() {
        let field = TagField::new("status").separator("|").casesensitive();
        let args = field.to_args();
        assert!(args.contains(&"TAG".to_string()));
        assert!(args.contains(&"SEPARATOR".to_string()));
        assert!(args.contains(&"|".to_string()));
        assert!(args.contains(&"CASESENSITIVE".to_string()));
    }

    #[test]
    fn test_vector_field_args() {
        let field = VectorField::new("embedding")
            .algorithm(VectorAlgorithm::Hnsw)
            .dim(384)
            .distance_metric(DistanceMetric::Cosine);
        let args = field.to_args();
        assert!(args.contains(&"VECTOR".to_string()));
        assert!(args.contains(&"HNSW".to_string()));
        assert!(args.contains(&"DIM".to_string()));
        assert!(args.contains(&"384".to_string()));
        assert!(args.contains(&"COSINE".to_string()));
    }

    #[test]
    fn test_index_build_args() {
        let index = Index::new("users_idx")
            .with_prefix("user:")
            .with_field(TextField::new("name").sortable())
            .with_field(NumericField::new("age").sortable())
            .with_field(TagField::new("status"));

        let cmd = index.to_command_string();
        assert!(cmd.contains("FT.CREATE users_idx"));
        assert!(cmd.contains("ON HASH"));
        assert!(cmd.contains("PREFIX 1 user:"));
        assert!(cmd.contains("SCHEMA"));
        assert!(cmd.contains("name TEXT SORTABLE"));
        assert!(cmd.contains("age NUMERIC SORTABLE"));
        assert!(cmd.contains("status TAG"));
    }

    #[test]
    fn test_index_from_schema() {
        let schema = vec![
            ("name".to_string(), RedisType::Utf8),
            ("age".to_string(), RedisType::Int64),
            ("active".to_string(), RedisType::Boolean),
        ];

        let index = Index::from_schema("users_idx", "user:", &schema, &["name"], &["age"]);

        let cmd = index.to_command_string();
        assert!(cmd.contains("name TEXT"));
        assert!(cmd.contains("age NUMERIC SORTABLE"));
        assert!(cmd.contains("active TAG"));
    }
}
