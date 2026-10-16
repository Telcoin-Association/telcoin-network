//! Static checks over the operations artifacts that ship with the repository: the Grafana
//! dashboard, the Prometheus rule file, its promtool unit tests and the compose profile.
//!
//! The artifacts are compile-time inputs, so a missing file fails the build of the test target
//! and a malformed one fails the matching test. The checks enforce the label and scope policy of
//! issue #1688: every node or host query is scoped by job and instance, no query matches on an
//! address-like label, every alert names its severity, action and runbook section, and every
//! alert has a promtool unit test.

use std::collections::BTreeSet;

use eyre::{eyre, Result};
use serde_json::Value as Json;
use serde_yaml::Value as Yaml;

/// The Grafana dashboard provisioned for a node.
const DASHBOARD: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../etc/grafana/telcoin-node-metrics.json"
));
/// The alerting and recording rules for host network pressure and committee connectivity.
const RULES: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../etc/prometheus/rules/telcoin-host-network.rules.yaml"
));
/// The promtool unit tests for [`RULES`].
const RULE_TESTS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../etc/prometheus/rules/telcoin-host-network.test.yaml"
));
/// The compose file that carries the `observability` profile.
const COMPOSE: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../etc/compose.yaml"));

/// Template variables every scoped query relies on.
const TEMPLATE_VARIABLES: [&str; 6] =
    ["job", "instance", "network", "node_job", "node_instance", "device"];
/// Alert severities the runbook documents.
const SEVERITIES: [&str; 3] = ["info", "warning", "critical"];
/// Annotation keys every alert must carry.
const ANNOTATIONS: [&str; 4] = ["summary", "description", "action", "runbook_url"];
/// Prefix of every runbook link: a section of the host network observability page.
const RUNBOOK_PREFIX: &str = "https://github.com/Telcoin-Association/telcoin-network/blob/main/docs/src/getting-started/host-network-observability.md#";
/// Matcher keys that would carry an address, a peer identity or a URL into a label.
/// `source` is not listed: the batch-origin label is bounded (`local` or `remote`).
const FORBIDDEN_MATCHER_KEYS: [&str; 6] = ["addr", "address", "ip", "src", "peer_id", "url"];
/// The compose profile that runs the observability stack.
const OBSERVABILITY_PROFILE: &str = "observability";
/// Metric name prefixes that must be scoped, and whether the prefix is the whole name.
const SCOPED_PREFIXES: [(&str, bool); 3] = [("tn_", false), ("node_", false), ("up", true)];

/// Every panel under `node`, including the panels nested in a row.
fn panels(node: &Json) -> Vec<&Json> {
    node.get("panels")
        .and_then(Json::as_array)
        .into_iter()
        .flatten()
        .flat_map(|panel| std::iter::once(panel).chain(panels(panel)))
        .collect()
}

/// A panel's id and title for failure messages.
fn panel_name(panel: &Json) -> String {
    let id =
        panel.get("id").and_then(Json::as_u64).map_or_else(|| "?".to_string(), |id| id.to_string());
    let title = panel.get("title").and_then(Json::as_str).unwrap_or("");
    format!("panel {id} ({title})")
}

/// True when the character can be part of a metric or label name.
fn is_name_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == ':'
}

/// The label matcher block that follows the metric reference ending at byte `end`, if any.
fn matcher_block(expr: &str, end: usize) -> Option<&str> {
    let inner = expr.get(end..)?.trim_start().strip_prefix('{')?;
    inner.find('}').and_then(|close| inner.get(..close))
}

/// Metric references in `expr` that start with `prefix` and lack a `job=~` or `instance=~`
/// matcher. A `prefix` occurrence inside a label value (after `"`), inside a template variable
/// (after `$`) or inside a longer name is not a reference. With `exact`, only a name equal to
/// `prefix` counts.
fn unscoped_references(expr: &str, prefix: &str, exact: bool) -> Vec<String> {
    expr.match_indices(prefix)
        .filter_map(|(start, _)| {
            let preceded_by = expr.get(..start).and_then(|head| head.chars().next_back());
            let starts_name = preceded_by.is_none_or(|c| !is_name_char(c) && c != '"' && c != '$');
            let end = expr.get(start..).map_or(start, |tail| {
                start + tail.find(|c: char| !is_name_char(c)).unwrap_or(tail.len())
            });
            let name = expr.get(start..end).unwrap_or(prefix);
            let is_reference = starts_name && (!exact || name == prefix);
            let scoped = matcher_block(expr, end)
                .is_some_and(|block| block.contains("job=~") && block.contains("instance=~"));
            (is_reference && !scoped).then(|| name.to_string())
        })
        .collect()
}

/// True when `expr` has a label matcher on `key` (`{key=`, `,key=` or ` key=`, also `!=`).
fn has_matcher_key(expr: &str, key: &str) -> bool {
    expr.match_indices(key).any(|(start, _)| {
        let before = expr.get(..start).and_then(|head| head.chars().next_back());
        let after = expr.get(start + key.len()..).and_then(|tail| tail.chars().next());
        before.is_some_and(|c| c == '{' || c == ',' || c == ' ')
            && after.is_some_and(|c| c == '=' || c == '!')
    })
}

/// The alerting rules of the rule file.
fn alerts(rules: &Yaml) -> Vec<&Yaml> {
    rules
        .get("groups")
        .and_then(Yaml::as_sequence)
        .into_iter()
        .flatten()
        .flat_map(|group| group.get("rules").and_then(Yaml::as_sequence).into_iter().flatten())
        .filter(|rule| rule.get("alert").is_some())
        .collect()
}

/// The string under `key` of a YAML mapping, or empty when absent or not a string.
fn text<'a>(node: &'a Yaml, key: &str) -> &'a str {
    node.get(key).and_then(Yaml::as_str).unwrap_or("")
}

/// The dashboard parses, every panel has a unique id and a description, and the template
/// variables are exactly the scoped set.
#[test]
fn dashboard_panels_are_identified_and_described() -> Result<()> {
    let dashboard: Json = serde_json::from_str(DASHBOARD)?;
    let all = panels(&dashboard);
    let ids: Vec<u64> =
        all.iter().filter_map(|panel| panel.get("id").and_then(Json::as_u64)).collect();
    let unique: BTreeSet<u64> = ids.iter().copied().collect();
    assert_eq!(ids.len(), all.len(), "every panel must carry an id");
    assert_eq!(unique.len(), ids.len(), "panel ids must be unique");
    let undescribed: Vec<String> = all
        .iter()
        .copied()
        .filter(|panel| panel.get("type").and_then(Json::as_str) != Some("row"))
        .filter(|panel| {
            panel.get("description").and_then(Json::as_str).is_none_or(|d| d.trim().is_empty())
        })
        .map(panel_name)
        .collect();
    assert!(undescribed.is_empty(), "panels without a description: {undescribed:?}");
    let variables: BTreeSet<&str> = dashboard
        .get("templating")
        .and_then(|templating| templating.get("list"))
        .and_then(Json::as_array)
        .ok_or_else(|| eyre!("dashboard has no templating list"))?
        .iter()
        .filter_map(|variable| variable.get("name").and_then(Json::as_str))
        .collect();
    let expected: BTreeSet<&str> = TEMPLATE_VARIABLES.into_iter().collect();
    assert_eq!(variables, expected, "template variables must match the scoped set");
    Ok(())
}

/// Every node, host and `up` reference in a query carries job and instance matchers, and no
/// query matches on an address-like label.
#[test]
fn dashboard_queries_are_scoped_and_address_free() -> Result<()> {
    let dashboard: Json = serde_json::from_str(DASHBOARD)?;
    let exprs: Vec<(String, &str)> = panels(&dashboard)
        .into_iter()
        .flat_map(|panel| {
            panel
                .get("targets")
                .and_then(Json::as_array)
                .into_iter()
                .flatten()
                .filter_map(|target| target.get("expr").and_then(Json::as_str))
                .map(move |expr| (panel_name(panel), expr))
        })
        .collect();
    assert!(!exprs.is_empty(), "the dashboard must contain queries");
    let unscoped: Vec<String> = exprs
        .iter()
        .flat_map(|(panel, expr)| {
            SCOPED_PREFIXES
                .into_iter()
                .flat_map(|(prefix, exact)| unscoped_references(expr, prefix, exact))
                .map(move |name| format!("{panel}: {name}"))
        })
        .collect();
    assert!(unscoped.is_empty(), "metric references without job and instance scope: {unscoped:?}");
    let addressed: Vec<String> = exprs
        .iter()
        .filter(|(_, expr)| FORBIDDEN_MATCHER_KEYS.iter().any(|key| has_matcher_key(expr, key)))
        .map(|(panel, expr)| format!("{panel}: {expr}"))
        .collect();
    assert!(addressed.is_empty(), "queries that match on an address-like label: {addressed:?}");
    Ok(())
}

/// Every alert has a documented severity, the four annotations, a runbook link into the host
/// network observability page and a job matcher in its expression.
#[test]
fn alerts_carry_severity_annotations_and_scope() -> Result<()> {
    let rules: Yaml = serde_yaml::from_str(RULES)?;
    let all = alerts(&rules);
    assert!(!all.is_empty(), "the rule file must define alerts");
    let faults: Vec<String> = all
        .iter()
        .flat_map(|alert| {
            let name = text(alert, "alert").to_string();
            let severity = alert.get("labels").map_or("", |labels| text(labels, "severity"));
            let annotations = alert.get("annotations");
            let missing = ANNOTATIONS
                .into_iter()
                .filter(|key| annotations.is_none_or(|a| text(a, key).trim().is_empty()))
                .map(|key| format!("{name}: missing annotation {key}"));
            let bad_severity =
                (!SEVERITIES.contains(&severity)).then(|| format!("{name}: severity {severity:?}"));
            let bad_runbook = (!annotations
                .is_some_and(|a| text(a, "runbook_url").starts_with(RUNBOOK_PREFIX)))
            .then(|| format!("{name}: runbook_url outside the runbook page"));
            let unscoped = (!text(alert, "expr").contains("job="))
                .then(|| format!("{name}: expr has no job matcher"));
            missing.chain(bad_severity).chain(bad_runbook).chain(unscoped).collect::<Vec<_>>()
        })
        .collect();
    assert!(faults.is_empty(), "alert rule faults: {faults:?}");
    Ok(())
}

/// Every alert in the rule file appears in at least one promtool unit test.
#[test]
fn every_alert_has_a_unit_test() -> Result<()> {
    let rules: Yaml = serde_yaml::from_str(RULES)?;
    let tests: Yaml = serde_yaml::from_str(RULE_TESTS)?;
    let tested: BTreeSet<&str> = tests
        .get("tests")
        .and_then(Yaml::as_sequence)
        .into_iter()
        .flatten()
        .flat_map(|test| {
            test.get("alert_rule_test").and_then(Yaml::as_sequence).into_iter().flatten()
        })
        .filter_map(|case| case.get("alertname").and_then(Yaml::as_str))
        .collect();
    let untested: Vec<&str> = alerts(&rules)
        .into_iter()
        .map(|alert| text(alert, "alert"))
        .filter(|name| !tested.contains(name))
        .collect();
    assert!(untested.is_empty(), "alerts without a promtool unit test: {untested:?}");
    Ok(())
}

/// Every service of the observability profile pins its image to a version tag.
#[test]
fn observability_profile_pins_image_tags() -> Result<()> {
    let compose: Yaml = serde_yaml::from_str(COMPOSE)?;
    let services = compose
        .get("services")
        .and_then(Yaml::as_mapping)
        .ok_or_else(|| eyre!("compose file has no services"))?;
    let observability: Vec<(&str, &str)> = services
        .iter()
        .filter(|(_, service)| {
            service.get("profiles").and_then(Yaml::as_sequence).is_some_and(|profiles| {
                profiles.iter().any(|profile| profile.as_str() == Some(OBSERVABILITY_PROFILE))
            })
        })
        .map(|(name, service)| (name.as_str().unwrap_or(""), text(service, "image")))
        .collect();
    assert!(!observability.is_empty(), "the compose file must define the observability profile");
    let unpinned: Vec<String> = observability
        .iter()
        .filter(|(_, image)| {
            !image.rsplit_once(':').is_some_and(|(_, tag)| {
                tag != "latest"
                    && tag.chars().next().is_some_and(|c| c == 'v' || c.is_ascii_digit())
            })
        })
        .map(|(name, image)| format!("{name}: {image}"))
        .collect();
    assert!(unpinned.is_empty(), "observability services without a pinned image tag: {unpinned:?}");
    Ok(())
}
