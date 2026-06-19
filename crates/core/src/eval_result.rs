//! Outcome of evaluating rules against one event. Mirrors Java
//! `eval/EvaluationResult.java` and `eval/RuleResult.java`, including the
//! verdict precedence (MATCHED > ERRORED > UNMATCHED).

use crate::audit::AuditType;
use serde::{Deserialize, Serialize};

/// Result of evaluating a single rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuleResult {
    pub rule_id: String,
    pub audit_type: AuditType,
    #[serde(default)]
    pub reason: Option<String>,
}

/// The outcome of evaluating a set of rules against a single event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EvaluationResult {
    pub rule_results: Vec<RuleResult>,
}

impl EvaluationResult {
    pub fn new(rule_results: Vec<RuleResult>) -> Self {
        Self { rule_results }
    }

    /// True if at least one rule matched.
    pub fn matched(&self) -> bool {
        self.rule_results
            .iter()
            .any(|r| r.audit_type == AuditType::Matched)
    }

    /// Overall verdict: MATCHED if any matched, else ERRORED if any errored,
    /// else UNMATCHED.
    pub fn verdict(&self) -> AuditType {
        if self.matched() {
            AuditType::Matched
        } else if self
            .rule_results
            .iter()
            .any(|r| r.audit_type == AuditType::Errored)
        {
            AuditType::Errored
        } else {
            AuditType::Unmatched
        }
    }
}
