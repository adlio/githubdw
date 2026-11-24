//! Light queries: find what changed for 1 point per 100 pull requests.
//!
//! Every query here selects only identifiers and timestamps, never a nested
//! connection with bodies, so GitHub charges 1 point per page however many
//! `author:` terms the search names (measured from 1 to 1,320 terms). The
//! full node selection is spent only on pull requests these queries show
//! changed (see [`super::queue`]).

use std::collections::BTreeMap;

use serde_json::Value;

use crate::error::{Error, Result};
use crate::fetch::{GhClient, is_inaccessible, node_index};
use crate::sync::search_timestamp;

/// Results per light page, and the most a window may match before it is
/// bisected: a leaf is only accepted when one page serves all of it, so no
/// result depends on paging a search deep.
pub(crate) const LIGHT_PAGE: i64 = 100;

/// GitHub's hard ceiling on results one search query serves.
const RESULT_CAP: i64 = crate::fetch::user_search::SEARCH_RESULT_CAP;

const LIGHT_SEARCH_QUERY: &str = r#"
query($q: String!, $cursor: String) {
  rateLimit { limit cost remaining resetAt }
  search(query: $q, type: ISSUE, first: 100, after: $cursor) {
    issueCount
    pageInfo { hasNextPage endCursor }
    nodes {
      ... on PullRequest { id number updatedAt repository { nameWithOwner } }
    }
  }
}
"#;

/// One pull request a light query returned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Found {
    /// `owner/name#number`, lowercase.
    pub pr_key: String,
    pub node_id: String,
    pub updated_at: String,
}

/// Everything one enumeration served.
#[derive(Debug, Default)]
pub(crate) struct Enumeration {
    pub items: Vec<Found>,
    /// Matches GitHub counted but would not show (SAML, forbidden).
    pub inaccessible: i64,
    /// Windows GitHub did not serve in full, and why. Empty means complete.
    pub gaps: Vec<String>,
    pub queries: u64,
}

impl Enumeration {
    pub fn complete(&self) -> bool {
        self.gaps.is_empty()
    }
}

/// True for errors that end the whole tick rather than one window.
fn stops_tick(error: &Error) -> bool {
    matches!(
        error,
        Error::Cancelled
            | Error::RateLimited { .. }
            | Error::QuotaReserve(_)
            | Error::GhUnavailable(_)
            | Error::Database(_)
    )
}

struct Page {
    total: i64,
    items: Vec<Found>,
    inaccessible: i64,
    next: Option<String>,
}

fn parse_page(data: &Value, errors: &[Value]) -> std::result::Result<Page, String> {
    let mut skipped = std::collections::BTreeSet::new();
    for error in errors {
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("unknown GraphQL error");
        match node_index(error) {
            Some(index) if is_inaccessible(error) => {
                skipped.insert(index);
            }
            _ => return Err(message.to_string()),
        }
    }
    let search = data
        .get("search")
        .filter(|v| !v.is_null())
        .ok_or("search block missing from response")?;
    let total = search
        .get("issueCount")
        .and_then(Value::as_i64)
        .ok_or("search.issueCount missing")?;
    let info = search.get("pageInfo").ok_or("search has no pageInfo")?;
    let has_next = info
        .get("hasNextPage")
        .and_then(Value::as_bool)
        .ok_or("search has no hasNextPage")?;
    let next = info
        .get("endCursor")
        .and_then(Value::as_str)
        .map(str::to_string);
    if has_next && next.is_none() {
        return Err("search reports another page but no endCursor".into());
    }
    let nodes = search
        .get("nodes")
        .and_then(Value::as_array)
        .ok_or("search has no nodes array")?;
    let mut items = Vec::new();
    for (index, node) in nodes.iter().enumerate() {
        if skipped.contains(&index) {
            continue;
        }
        if node.is_null() {
            return Err("search contains a null node with no error".into());
        }
        // An issue that slips past `is:pr` answers the fragment with `{}`.
        if node.as_object().is_some_and(serde_json::Map::is_empty) {
            continue;
        }
        let text = |field: &str| node.get(field).and_then(Value::as_str);
        let repository = node
            .get("repository")
            .and_then(|r| r.get("nameWithOwner"))
            .and_then(Value::as_str)
            .ok_or("search node has no repository")?;
        let number = node
            .get("number")
            .and_then(Value::as_i64)
            .ok_or("search node has no number")?;
        items.push(Found {
            pr_key: format!("{}#{number}", repository.to_lowercase()),
            node_id: text("id").ok_or("search node has no id")?.to_string(),
            updated_at: text("updatedAt")
                .ok_or("search node has no updatedAt")?
                .to_string(),
        });
    }
    Ok(Page {
        total,
        items,
        inaccessible: skipped.len() as i64,
        next: has_next.then_some(next).flatten(),
    })
}

/// Enumerate every pull request matching `base` whose `field` (`updated` or
/// `created`) lies in the second range `[start, end]`.
///
/// Windows are walked oldest first with `sort:updated-asc`, and any window
/// matching more than [`LIGHT_PAGE`] is bisected by seconds, so each accepted
/// leaf is served whole by one page. A single second holding more than a
/// page is paged (up to GitHub's 1,000-result cap). A leaf whose served
/// count disagrees with GitHub's own count is recorded as a gap; the caller
/// must not claim it. `after_query` runs after every request (lease renewal).
pub(crate) fn enumerate(
    client: &mut GhClient,
    base: &str,
    field: &str,
    start: i64,
    end: i64,
    after_query: &mut dyn FnMut() -> Result<()>,
) -> Result<Enumeration> {
    let mut out = Enumeration::default();
    if start > end {
        return Ok(out);
    }
    // Stack of windows; the oldest is on top.
    let mut pending = vec![(start, end)];
    while let Some((low, high)) = pending.pop() {
        let expression = format!(
            "{base} {field}:{}..{} sort:updated-asc",
            search_timestamp(low)?,
            search_timestamp(high)?
        );
        let mut cursor: Option<String> = None;
        let mut served = 0_i64;
        let mut total = None;
        loop {
            let mut variables: Vec<(&str, &str)> = vec![("q", expression.as_str())];
            if let Some(after) = cursor.as_deref() {
                variables.push(("cursor", after));
            }
            let answer = client.graphql_search_with_errors(LIGHT_SEARCH_QUERY, &variables);
            out.queries += 1;
            after_query()?;
            let (data, errors) = match answer {
                Ok(answer) => answer,
                Err(error) if stops_tick(&error) => return Err(error),
                Err(error) => {
                    out.gaps.push(format!("'{expression}': {error}"));
                    break;
                }
            };
            let page = match parse_page(&data, &errors) {
                Ok(page) => page,
                Err(reason) if reason.to_lowercase().contains("rate limit") => {
                    return Err(Error::RateLimited {
                        resource: "graphql".into(),
                        wake_at: chrono::Utc::now() + chrono::Duration::minutes(1),
                        reason,
                    });
                }
                Err(reason) => {
                    out.gaps.push(format!("'{expression}': {reason}"));
                    break;
                }
            };
            if total.is_none() {
                total = Some(page.total);
                if page.total > LIGHT_PAGE && low < high {
                    let middle = low + (high - low) / 2;
                    // Push the newer half first so the older half is walked first.
                    pending.push((middle + 1, high));
                    pending.push((low, middle));
                    break;
                }
                if page.total > RESULT_CAP {
                    out.gaps.push(format!(
                        "'{expression}': {} matches share one second, above GitHub's \
                         {RESULT_CAP}-result cap",
                        page.total
                    ));
                }
            }
            served += page.items.len() as i64 + page.inaccessible;
            out.inaccessible += page.inaccessible;
            out.items.extend(page.items);
            match page.next {
                Some(next) if Some(&next) != cursor.as_ref() => cursor = Some(next),
                _ => {
                    let total = total.unwrap_or(0);
                    if served != total && total <= RESULT_CAP {
                        out.gaps.push(format!(
                            "'{expression}': GitHub counted {total} matches but served {served}"
                        ));
                    }
                    break;
                }
            }
        }
    }
    // A pull request updated during the walk can be served by two windows.
    let mut seen = std::collections::HashSet::new();
    out.items.retain(|item| seen.insert(item.pr_key.clone()));
    Ok(out)
}

/// A GitHub login is letters, digits and single hyphens, at most 39 long.
pub(crate) fn valid_login(login: &str) -> bool {
    !login.is_empty()
        && login.len() <= 39
        && login
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-')
}

/// `owner/name` with only the characters GitHub allows, safe to inline into
/// query text.
pub(crate) fn valid_repository(repository: &str) -> bool {
    let segment = |s: &str| {
        !s.is_empty()
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
    };
    repository
        .split_once('/')
        .is_some_and(|(owner, name)| segment(owner) && segment(name) && !name.contains('/'))
}

/// A GraphQL node id (base64-like), safe to inline into query text.
pub(crate) fn valid_node_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 200
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'=' | b'+' | b'/'))
}

/// Run one query of aliased selections (`a0: ...`, `a1: ...`) and return the
/// value of each alias, `None` for an alias GitHub could not resolve, with
/// the error that came with it. Errors are matched to aliases by their path.
pub(crate) fn aliased(
    client: &mut GhClient,
    selections: &[String],
) -> Result<Vec<(Option<Value>, Option<Value>)>> {
    let mut query = String::from("query {\n  rateLimit { limit cost remaining resetAt }\n");
    for (index, selection) in selections.iter().enumerate() {
        query.push_str(&format!("  a{index}: {selection}\n"));
    }
    query.push('}');
    let (data, errors) = client.graphql_with_errors(&query, &[])?;
    let mut by_alias: BTreeMap<usize, Value> = BTreeMap::new();
    for error in errors {
        let alias = error
            .get("path")
            .and_then(Value::as_array)
            .and_then(|path| path.first())
            .and_then(Value::as_str)
            .and_then(|name| name.strip_prefix('a'))
            .and_then(|n| n.parse::<usize>().ok());
        match alias {
            Some(index) => {
                by_alias.entry(index).or_insert(error);
            }
            None => {
                let message = error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown GraphQL error");
                if message.to_lowercase().contains("rate limit") {
                    return Err(Error::RateLimited {
                        resource: "graphql".into(),
                        wake_at: chrono::Utc::now() + chrono::Duration::minutes(1),
                        reason: message.to_string(),
                    });
                }
                return Err(Error::GitHubApi(message.to_string()));
            }
        }
    }
    Ok((0..selections.len())
        .map(|index| {
            let value = data
                .get(format!("a{index}"))
                .filter(|v| !v.is_null())
                .cloned();
            (value, by_alias.remove(&index))
        })
        .collect())
}

/// The newest `updatedAt` a repository connection reported, if any.
pub(crate) fn newest_updated(connection: Option<&Value>) -> Option<String> {
    connection?
        .get("nodes")?
        .as_array()?
        .first()?
        .get("updatedAt")?
        .as_str()
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_inlined_into_queries_are_checked() {
        assert!(valid_login("octo-cat9"));
        assert!(!valid_login("a\"b"));
        assert!(!valid_login(""));
        assert!(valid_repository("acme/widgets.rs"));
        assert!(!valid_repository("acme/wid gets"));
        assert!(!valid_repository("acme"));
        assert!(!valid_repository("a/b/c"));
        assert!(valid_node_id("PR_kwDOABC123=="));
        assert!(!valid_node_id("PR\"){x}"));
    }
}
