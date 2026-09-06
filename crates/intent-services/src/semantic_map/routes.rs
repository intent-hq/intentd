use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use super::activity::{MapActivity, MapActivityKind};
use super::manifest::Manifest;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RouteFilter {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub kinds: Vec<MapActivityKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Route {
    pub visits: Vec<String>,
    pub transitions: Vec<RouteTransition>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RouteTransition {
    pub from: String,
    pub to: String,
    pub count: u32,
    pub evidence: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

#[must_use]
pub fn derive_route(
    manifest: &Manifest,
    activities: &[MapActivity],
    filter: &RouteFilter,
) -> Route {
    let mut ordered: Vec<(usize, &MapActivity)> = activities
        .iter()
        .enumerate()
        .filter(|(_, activity)| matches_filter(activity, filter))
        .collect();
    ordered.sort_by(|(left_index, left), (right_index, right)| {
        left.ts
            .cmp(&right.ts)
            .then_with(|| left_index.cmp(right_index))
    });

    let mut route = Route::default();
    let mut visited = HashSet::new();
    let mut last_by_agent: HashMap<Option<&str>, &str> = HashMap::new();
    let mut transition_index: HashMap<(String, String), usize> = HashMap::new();

    for (_, activity) in ordered {
        let Some(region) = activity.region_id.as_deref() else {
            continue;
        };
        if visited.insert(region) {
            route.visits.push(region.to_string());
        }

        let agent = activity.agent_id.as_deref();
        let previous = last_by_agent.insert(agent, region);
        let Some(previous) = previous.filter(|previous| *previous != region) else {
            continue;
        };
        let key = (previous.to_string(), region.to_string());
        if let Some(index) = transition_index.get(&key).copied() {
            let transition = &mut route.transitions[index];
            transition.count = transition.count.saturating_add(1);
            add_evidence(transition, activity.path.as_deref());
            continue;
        }
        let label = manifest
            .crossings
            .iter()
            .find(|crossing| crossing.from == previous && crossing.to == region)
            .map(|crossing| crossing.label.clone());
        let mut transition = RouteTransition {
            from: previous.to_string(),
            to: region.to_string(),
            count: 1,
            evidence: Vec::new(),
            label,
        };
        add_evidence(&mut transition, activity.path.as_deref());
        transition_index.insert(key, route.transitions.len());
        route.transitions.push(transition);
    }
    route
}

fn matches_filter(activity: &MapActivity, filter: &RouteFilter) -> bool {
    if filter
        .agent_id
        .as_deref()
        .is_some_and(|agent| activity.agent_id.as_deref() != Some(agent))
    {
        return false;
    }
    if !filter.kinds.is_empty() && !filter.kinds.contains(&activity.kind) {
        return false;
    }
    if filter
        .since
        .as_deref()
        .is_some_and(|since| activity.ts.as_str() < since)
    {
        return false;
    }
    filter
        .until
        .as_deref()
        .is_none_or(|until| activity.ts.as_str() <= until)
}

fn add_evidence(transition: &mut RouteTransition, path: Option<&str>) {
    if let Some(path) = path {
        if !transition.evidence.iter().any(|existing| existing == path) {
            transition.evidence.push(path.to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::semantic_map::{Crossing, ManifestSource};

    fn activity(agent: &str, region: &str, path: &str, ts: &str) -> MapActivity {
        MapActivity {
            region_id: Some(region.into()),
            agent_id: Some(agent.into()),
            agent_name: None,
            path: Some(path.into()),
            kind: MapActivityKind::Edit,
            ts: ts.into(),
        }
    }

    #[test]
    fn interleaved_agents_do_not_create_cross_agent_transitions() {
        let manifest = Manifest {
            version: 1,
            regions: vec![],
            crossings: vec![Crossing {
                from: "a".into(),
                to: "b".into(),
                label: "A becomes B".into(),
            }],
            source: ManifestSource::Curated,
        };
        let route = derive_route(
            &manifest,
            &[
                activity("one", "a", "a/1", "1"),
                activity("two", "c", "c/1", "2"),
                activity("one", "b", "b/1", "3"),
                activity("two", "d", "d/1", "4"),
                activity("one", "a", "a/2", "5"),
                activity("one", "b", "b/2", "6"),
            ],
            &RouteFilter::default(),
        );
        assert_eq!(route.visits, ["a", "c", "b", "d"]);
        assert_eq!(route.transitions.len(), 3);
        assert_eq!(route.transitions[0].count, 2);
        assert_eq!(route.transitions[0].label.as_deref(), Some("A becomes B"));
        assert_eq!(route.transitions[0].evidence, ["b/1", "b/2"]);
        assert!(route
            .transitions
            .iter()
            .all(|transition| !(transition.from == "c" && transition.to == "b")));
    }
}
