//! Business rule. Evolved from Java `rules/Rule.java`: the expression is now CEL
//! (not SpEL) and the rule carries its routing target + version per the locked
//! plan (S1/S5). Serializes with camelCase keys.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A persistent business rule. The compiled form lives in the `eval` crate (S2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Rule {
    pub id: String,
    pub description: String,
    /// CEL expression evaluated against the event payload.
    pub expression: String,
    /// Topic matched events are routed to.
    pub target_topic: String,
    pub enabled: bool,
    pub version: i64,
    pub updated_at: DateTime<Utc>,
}
