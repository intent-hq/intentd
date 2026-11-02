// Issue half of the nightly payload-sweep reporter. Ported from cloudlands-fe
// scripts/nightly-test-issues.mjs: one serial writer re-reads the complete
// intent-hq/intent issue and comment inventory before every write, matches a
// failure by its stable marker (in an issue body or in a comment that adopted
// it), follows "marked as duplicate" to the canonical issue, comments on an
// existing issue instead of filing a duplicate (reopening one closed as
// completed), and records a per-run-attempt occurrence marker so a retried
// reporter never writes twice. A pass never closes an issue.
import {
  ARTIFACT,
  ISSUE_REPO,
  OWNER,
  SOURCE_REPO,
  failureKey,
  itemTitle,
  marker,
  occurrenceMarker,
  safeText,
} from './nightly-payload-report.mjs';

const CREATED = '<!-- nightly-payload-created:v1 -->';
const fold = (s) =>
  String(s ?? '')
    .toLowerCase()
    .replaceAll('`', '')
    .replace(/\s+/g, ' ');
const issueUrl = (n) => `https://github.com/${ISSUE_REPO}/issues/${n}`;

function legacyDuplicates(bodies) {
  const references = new Set();
  for (const body of bodies) {
    // Require the entire human text to be affirmative declarations. A matching
    // line surrounded by historical discussion, Markdown or HTML is ambiguous.
    const lines = (body ?? '')
      .split(/\r?\n/)
      .filter(
        (line) => line.trim() && !/^<!-- nightly-payload-failure:v1:[a-f0-9]{64} -->$/.test(line),
      );
    const declarations = lines.map((line) =>
      /^ {0,3}duplicate(?: of|:)\s+(?:https:\/\/github\.com\/intent-hq\/intent\/issues\/|intent-hq\/intent#|#)([1-9]\d*)[.!]?[ \t]*$/i.exec(
        line,
      ),
    );
    if (declarations.every(Boolean))
      for (const match of declarations) references.add(Number(match[1]));
  }
  return [...references];
}

function matchingIssues(item, inventory) {
  const marked = new Set(
    inventory.comments
      .filter((c) => c.body.includes(marker(item.key)))
      .map((c) => Number(c.issue_url.split('/').at(-1))),
  );
  const exact = inventory.issues.filter(
    (i) => (i.body ?? '').includes(marker(item.key)) || marked.has(i.number),
  );
  if (exact.length) return { candidates: exact, exact: true };
  // A title fragment is not sufficient to adopt a human issue. Require the
  // complete reporter title and, for a budget failure, the method and variant.
  const strong = inventory.issues.filter((i) => {
    const text = fold(`${i.title}\n${i.body ?? ''}`);
    const named = item.kind === 'budget' ? [item.method, item.variant] : [item.test];
    return text.includes(fold(itemTitle(item))) && named.every((n) => text.includes(fold(n)));
  });
  return { candidates: strong, exact: false };
}

function evidenceBody(item, plan, occurrences, firstSeen, latestSeen, candidates = [], note = '') {
  const { run, release, artifact } = plan;
  const attempts = [...new Set(occurrences.map((o) => o.attempt))].sort((a, b) => a - b);
  const runUrl = (attempt) =>
    `https://github.com/${SOURCE_REPO}/actions/runs/${run.id}/attempts/${attempt}`;
  const reportUrl = artifact
    ? `https://github.com/${SOURCE_REPO}/actions/runs/${run.id}/artifacts/${artifact.id}`
    : `${runUrl(attempts.at(-1))}#artifacts`;
  const evidence = occurrences
    .map((o) => `${o.artifacts.join(', ')} (attempt ${o.attempt}, observed ${o.observedAt}):\n${o.evidence}`)
    .join('\n\n');
  const kib = (bytes) => `${bytes} bytes (${(bytes / 1024).toFixed(1)} KiB)`;
  const measured =
    item.kind !== 'budget'
      ? ''
      : `Method: ${safeText(item.method)}\n\nVariant: ${safeText(item.variant)}\n\n` +
        `Problem: ${safeText(item.category)}\n\n` +
        (item.bytes !== null
          ? `Measured: ${kib(item.bytes)} against the ${kib(item.budget)} read payload budget` +
            (item.recorded !== null ? `; baseline-full.txt records ${item.recorded} bytes` : '') +
            '.\n\n'
          : `Recorded in baseline-full.txt: ${item.recorded ?? 'unknown'} bytes; the reply now fits the ${kib(item.budget)} budget or is no longer swept, so delete its line.\n\n`) +
        (item.sizeLine ? `Size report line: <code>${safeText(item.sizeLine)}</code>\n\n` : '');
  return (
    `${marker(item.key)}\n${attempts.map((attempt) => occurrenceMarker(run, item.key, attempt)).join('\n')}\n` +
    `Nightly full-scale payload sweep failure (INTENTD_LARGE_STATE_SCALE=full).\n\n` +
    `First seen: ${firstSeen}. Latest seen: ${latestSeen}.\n` +
    `Failing intentd main commit: [${run.head_sha}](https://github.com/${SOURCE_REPO}/commit/${run.head_sha}).\n` +
    `Latest released intentd version: ${release ? `[${safeText(release.tag)}](${release.url})${release.prerelease ? ' (alpha)' : ''}, published ${release.publishedAt}` : 'unknown'}.\n` +
    `${attempts.map((attempt) => `[Run ${run.id}, attempt ${attempt}](${runUrl(attempt)})`).join(', ')}.\n\n` +
    `Test: ${safeText(item.test)}\n\n` +
    measured +
    `Sorted size report and test log: [${ARTIFACT} artifact](${reportUrl}) (size-report.txt, test.log).\n\n` +
    `Evidence:\n<pre>${safeText(evidence).slice(0, 8000)}</pre>\n\n` +
    (candidates.length
      ? `Possible existing issues need human triage: ${candidates.map((i) => `[${i.number}](${issueUrl(i.number)})`).join(', ')}. No automatic adoption because the match is ambiguous.\n\n`
      : '') +
    note
  );
}

function verifyCreated(issue) {
  if (!(issue.body ?? '').includes(CREATED)) return;
  const labels = (issue.labels ?? []).map((l) => (typeof l === 'string' ? l : l.name));
  if (
    issue.type?.name !== 'Bug' ||
    !['component:intentd', 'agent-filed'].every((l) => labels.includes(l)) ||
    !issue.assignees?.length
  ) {
    throw new Error(
      `Issue ${issue.number} exists but required Bug type, labels or owner are missing; check token permissions and retry`,
    );
  }
}

export function synchronizeIssues(
  plan,
  client,
  { write = false, checkpoint = () => {}, now = () => new Date().toISOString() } = {},
) {
  const results = [];
  let inventory = { issues: [], comments: [] };
  let since;
  const refresh = () => {
    const started = new Date(new Date(now()).getTime() - 60_000).toISOString();
    const next = client.inventory(since);
    const issues = new Map(inventory.issues.map((i) => [i.number, i]));
    const comments = new Map(inventory.comments.map((c) => [c.id, c]));
    for (const issue of next.issues) issues.set(issue.number, issue);
    for (const comment of next.comments) comments.set(comment.id, comment);
    inventory = { issues: [...issues.values()], comments: [...comments.values()] };
    since = started;
  };
  refresh(); // Includes all states and all pages, including adopted markers in comments.
  const resolveCanonical = (issue) => {
    const seen = new Set();
    while (issue.state === 'closed') {
      if (seen.has(issue.number)) throw new Error('Duplicate issue cycle');
      seen.add(issue.number);
      const history = client.canonical(issue.number);
      if (!['marked', 'unmarked', 'none'].includes(history?.state))
        throw new Error('Unreadable duplicate decision');
      let canonical = history.number;
      if (history.state === 'none') {
        const refs = legacyDuplicates([
          issue.body,
          ...client.comments(issue.number).map((c) => c.body),
        ]);
        if (refs.length > 1)
          throw new Error(`Ambiguous duplicate reference on issue ${issue.number}`);
        canonical = refs[0];
      }
      if (!canonical) break;
      issue = client.issue(canonical);
    }
    return issue;
  };
  for (const item of plan.items) {
    if (item.key !== failureKey(item)) throw new Error('Invalid failure identity');
    if (
      !Array.isArray(item.occurrences) ||
      item.occurrences.length === 0 ||
      !item.occurrences.every(
        (o) =>
          Number.isSafeInteger(o.attempt) &&
          o.attempt >= 1 &&
          o.attempt <= plan.run.run_attempt &&
          typeof o.observedAt === 'string' &&
          Number.isFinite(Date.parse(o.observedAt)) &&
          Array.isArray(o.artifacts) &&
          typeof o.evidence === 'string',
      )
    )
      throw new Error('Missing or invalid occurrence provenance; collect the report again');
    refresh(); // Serial writer + authoritative recheck immediately before every create.
    const match = matchingIssues(item, inventory);
    let candidates = match.candidates.map((i) => resolveCanonical(client.issue(i.number)));
    candidates = [...new Map(candidates.map((i) => [i.number, i])).values()];
    if (match.exact && candidates.length > 1)
      throw new Error(
        `Conflicting exact failure markers: ${candidates.map((i) => issueUrl(i.number)).join(', ')}`,
      );
    let issue = candidates.length === 1 ? candidates[0] : null;
    const result = {
      key: item.key,
      action: issue ? 'update' : 'create',
      issue: issue?.number ?? null,
      candidates: candidates.map((i) => i.number),
      url: issue ? issueUrl(issue.number) : null,
      occurrences: item.occurrences.map(({ attempt, observedAt, job, artifacts }) => ({
        attempt,
        observedAt,
        job,
        artifacts,
      })),
    };
    results.push(result);
    checkpoint(results);
    const observed = item.occurrences
      .map((o) => o.observedAt)
      .sort((a, b) => Date.parse(a) - Date.parse(b));
    if (!issue) {
      const body = `${CREATED}\n${evidenceBody(item, plan, item.occurrences, observed[0], observed.at(-1), candidates)}\nNew-failure triage owner: @${OWNER}.`;
      if (!write) continue;
      issue = client.create(itemTitle(item).replace(/\s+/g, ' ').slice(0, 240), body);
      result.issue = issue.number;
      result.url = issueUrl(issue.number);
      checkpoint(results); // Preserve the receipt even if metadata validation fails next.
      verifyCreated(issue);
      inventory.issues.push(issue);
      continue;
    }
    verifyCreated(issue);
    const comments = client.comments(issue.number);
    const bodies = [issue.body, ...comments.map((c) => c.body)];
    const pending = item.occurrences.filter(
      (o) =>
        !bodies.some((body) => body?.includes(occurrenceMarker(plan.run, item.key, o.attempt))),
    );
    if (pending.length === 0) {
      result.action = 'already-recorded';
      checkpoint(results);
      continue;
    }
    const previous = bodies.filter((s) => s?.includes(marker(item.key)));
    const sightings = [
      ...observed,
      ...previous.flatMap((body) =>
        [...body.matchAll(/(?:First|Latest) seen: ([0-9T:.+-]+Z)\./g)].map((match) => match[1]),
      ),
    ]
      .filter((time) => Number.isFinite(Date.parse(time)))
      .sort((a, b) => Date.parse(a) - Date.parse(b));
    const firstSeen = sightings[0];
    const latestSeen = sightings.at(-1);
    const predatesClosure =
      issue.state === 'closed' &&
      pending.every((o) => Date.parse(o.observedAt) <= Date.parse(issue.closed_at));
    const notPlanned = issue.state === 'closed' && issue.state_reason !== 'completed';
    const owners = (issue.assignees ?? []).map((a) => a.login).filter((s) => /^[\w-]+$/.test(s));
    const note = predatesClosure
      ? 'This delayed completion predates the issue closure; the issue stays closed.\n'
      : notPlanned
        ? `Deliberate closure preserved; recurrence needs owner review: ${(owners.length ? owners : [OWNER]).map((s) => `@${s}`).join(', ')}.\n`
        : '';
    if (issue.state === 'closed' && !notPlanned && !predatesClosure) {
      result.action = 'reopen';
      if (write) client.reopen(issue.number);
    }
    if (notPlanned) result.action = 'closed-recurrence';
    if (predatesClosure) result.action = 'historical-evidence';
    if (write)
      client.comment(
        issue.number,
        evidenceBody(item, plan, pending, firstSeen, latestSeen, [], note),
      );
    checkpoint(results);
  }
  return results;
}
