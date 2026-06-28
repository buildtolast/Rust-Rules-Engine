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
    #[must_use]
    pub fn matched(&self) -> bool {
        self.rule_results
            .iter()
            .any(|r| r.audit_type == AuditType::Matched)
    }

    /// Overall verdict: MATCHED if any matched, else ERRORED if any errored,
    /// else UNMATCHED.
    #[must_use]
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

#[cfg(test)]
mod tests {
    use super::*;

    fn result(audit_type: AuditType) -> RuleResult {
        RuleResult { rule_id: "r1".into(), audit_type, reason: None }
    }

    #[test]
    fn new_stores_results() {
        let r = EvaluationResult::new(vec![result(AuditType::Matched)]);
        assert_eq!(r.rule_results.len(), 1);
    }

    #[test]
    fn matched_true_when_any_matched() {
        let r = EvaluationResult::new(vec![result(AuditType::Unmatched), result(AuditType::Matched)]);
        assert!(r.matched());
    }

    #[test]
    fn matched_false_when_none_matched() {
        let r = EvaluationResult::new(vec![result(AuditType::Unmatched), result(AuditType::Errored)]);
        assert!(!r.matched());
    }

    #[test]
    fn matched_false_when_empty() {
        assert!(!EvaluationResult::new(vec![]).matched());
    }

    #[test]
    fn verdict_matched_takes_precedence_over_errored() {
        let r = EvaluationResult::new(vec![result(AuditType::Errored), result(AuditType::Matched)]);
        assert_eq!(r.verdict(), AuditType::Matched);
    }

    #[test]
    fn verdict_errored_when_no_match_but_some_errored() {
        let r = EvaluationResult::new(vec![result(AuditType::Unmatched), result(AuditType::Errored)]);
        assert_eq!(r.verdict(), AuditType::Errored);
    }

    #[test]
    fn verdict_unmatched_when_all_unmatched() {
        let r = EvaluationResult::new(vec![result(AuditType::Unmatched), result(AuditType::Unmatched)]);
        assert_eq!(r.verdict(), AuditType::Unmatched);
    }

    #[test]
    fn verdict_unmatched_when_empty() {
        assert_eq!(EvaluationResult::new(vec![]).verdict(), AuditType::Unmatched);
    }

    #[test]
    fn rule_result_reason_defaults_none_in_json() {
        let json = r#"{"ruleId":"x","auditType":"MATCHED"}"#;
        let rr: RuleResult = serde_json::from_str(json).unwrap();
        assert!(rr.reason.is_none());
    }

    #[test]
    fn evaluation_result_serializes_camel_case() {
        let r = EvaluationResult::new(vec![result(AuditType::Matched)]);
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("ruleResults"));
        assert!(json.contains("auditType"));
    }
}

