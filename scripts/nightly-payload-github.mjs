import { execFileSync } from 'node:child_process';
import { openSync, closeSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { ISSUE_REPO, OWNER, SOURCE_REPO } from './nightly-payload-report.mjs';

// GitHub access for the nightly payload-sweep reporter. Ported from cloudlands-fe
// scripts/nightly-test-github.mjs: REST/GraphQL through `gh`, bounded in-memory
// reads of fixed artifact members, and validated issue/comment responses.

// Python's standard-library ZIP reader is present on the hosted runner and our
// dev hosts. Read bounded named members in memory; do not extract any paths.
const READ_MEMBERS = `import json,sys,zipfile
result={}
try:
 with zipfile.ZipFile(sys.argv[1]) as archive:
  for key,name in json.loads(sys.argv[2]).items():
   members=[i for i in archive.infolist() if i.filename==name]
   if len(members)>1: raise ValueError('Duplicate ZIP member: '+name)
   if not members: continue
   if members[0].file_size>32*1024*1024: raise ValueError('Member exceeds 32 MiB: '+name)
   result[key]=archive.read(members[0]).decode('utf-8')
except (ValueError,zipfile.BadZipFile,UnicodeError,RuntimeError) as error:
 result={'archiveError':str(error)}
print(json.dumps(result))
`;

function validIssue(issue) {
  if (
    !Number.isSafeInteger(issue?.number) ||
    issue.number < 1 ||
    typeof issue.title !== 'string' ||
    !(issue.body === null || typeof issue.body === 'string') ||
    !['open', 'closed'].includes(issue.state) ||
    !Array.isArray(issue.assignees) ||
    issue.pull_request
  )
    throw new Error('Malformed issue response');
  return issue;
}
function validComment(comment) {
  if (
    !Number.isSafeInteger(comment?.id) ||
    typeof comment.body !== 'string' ||
    !/^https:\/\/api\.github\.com\/repos\/intent-hq\/intent\/issues\/\d+$/.test(
      comment.issue_url ?? '',
    )
  )
    throw new Error('Malformed comment response');
  return comment;
}

export function gh(args, options = {}) {
  try {
    return execFileSync('gh', args, {
      encoding: 'utf8',
      maxBuffer: 64 * 1024 * 1024,
      timeout: 120_000,
      stdio: ['pipe', 'pipe', 'pipe'],
      ...options,
    });
  } catch (error) {
    throw new Error(
      `gh ${args[0]} failed: ${String(error.stderr ?? error.message).slice(0, 3000)}`,
      { cause: error },
    );
  }
}

export function githubClient({ run = gh, directory }) {
  const api = (path, data, method = 'POST') => {
    const args = ['api', path];
    if (data !== undefined) args.push('--method', method, '--input', '-');
    const result = JSON.parse(run(args, data === undefined ? {} : { input: JSON.stringify(data) }));
    if (result?.errors) throw new Error(`GitHub API errors: ${JSON.stringify(result.errors)}`);
    return result;
  };
  const pages = (path, key) => {
    const result = [];
    for (let page = 1; ; page += 1) {
      const data = api(`${path}${path.includes('?') ? '&' : '?'}per_page=100&page=${page}`);
      const chunk = key ? data?.[key] : data;
      if (
        !Array.isArray(chunk) ||
        (key && (!Number.isSafeInteger(data.total_count) || data.total_count < 0))
      )
        throw new Error(`Unreadable paginated API response: ${path}`);
      result.push(...chunk);
      if (chunk.length < 100) {
        if (key && result.length < data.total_count)
          throw new Error(`Truncated paginated API response: ${path}`);
        return result;
      }
    }
  };
  return {
    api,
    pages,
    run: (id) => api(`repos/${SOURCE_REPO}/actions/runs/${id}`),
    jobs: (id) => pages(`repos/${SOURCE_REPO}/actions/runs/${id}/jobs?filter=all`, 'jobs'),
    artifacts: (id) => pages(`repos/${SOURCE_REPO}/actions/runs/${id}/artifacts`, 'artifacts'),
    // Newest published (non-draft) intentd release, alpha included: the
    // version a failing main commit is compared against in the issue body.
    latestRelease: () => {
      const releases = api(`repos/${SOURCE_REPO}/releases?per_page=30`);
      if (!Array.isArray(releases)) throw new Error('Unreadable release list');
      const published = releases
        .filter((r) => !r.draft && /^v\d+\.\d+\.\d+/.test(r?.tag_name ?? '') && Number.isFinite(Date.parse(r.published_at)))
        .sort((a, b) => Date.parse(b.published_at) - Date.parse(a.published_at));
      if (!published.length) throw new Error('No published intentd release found');
      const [latest] = published;
      return { tag: latest.tag_name, publishedAt: latest.published_at, prerelease: latest.prerelease === true, url: latest.html_url };
    },
    archive: (artifact, paths, json = []) => {
      if (!Number.isSafeInteger(artifact.id) || artifact.id < 1)
        throw new Error('Invalid artifact ID');
      const path = join(directory, `${artifact.id}.zip`);
      const fd = openSync(path, 'w');
      try {
        run(['api', `repos/${SOURCE_REPO}/actions/artifacts/${artifact.id}/zip`], {
          stdio: ['ignore', fd, 'pipe'],
          encoding: undefined,
        });
      } finally {
        closeSync(fd);
      }
      // Read only fixed members. Never extract paths, evaluate code, source env files,
      // use artifact-provided filenames, or load dependencies from an archive.
      const members = JSON.parse(
        execFileSync('python3', ['-c', READ_MEMBERS, path, JSON.stringify(paths)], {
          encoding: 'utf8',
          maxBuffer: 128 * 1024 * 1024,
          timeout: 30_000,
        }),
      );
      if (members.archiveError) return members;
      const data = {};
      for (const [key, raw] of Object.entries(members)) {
        writeFileSync(join(directory, `${artifact.id}-${key}.${json.includes(key) ? 'json' : 'txt'}`), raw);
        if (!json.includes(key)) {
          data[key] = raw;
          continue;
        }
        try {
          data[key] = JSON.parse(raw);
        } catch {
          data[key] = null;
        }
      }
      return data;
    },
    inventory: (since) => {
      const query = since ? `&since=${encodeURIComponent(since)}` : '';
      const issues = pages(
        `repos/${ISSUE_REPO}/issues?state=all&sort=created&direction=asc${query}`,
      ).filter((i) => !i.pull_request);
      // Repository comments include newly adopted markers absent from human bodies.
      // These REST listings are authoritative; no search-index dependency or cap.
      const comments = pages(
        `repos/${ISSUE_REPO}/issues/comments?sort=created&direction=asc${query}`,
      );
      issues.forEach(validIssue);
      comments.forEach(validComment);
      return { issues, comments };
    },
    issue: (number) => validIssue(api(`repos/${ISSUE_REPO}/issues/${number}`)),
    comments: (number) => pages(`repos/${ISSUE_REPO}/issues/${number}/comments`).map(validComment),
    canonical: (number) => {
      let cursor = null;
      let history = { state: 'none', number: null };
      do {
        const response = api('graphql', {
          query: `query($number:Int!,$cursor:String) {
          repository(owner:"intent-hq",name:"intent") { issue(number:$number) {
            timelineItems(first:100,after:$cursor,itemTypes:[MARKED_AS_DUPLICATE_EVENT,UNMARKED_AS_DUPLICATE_EVENT]) {
              nodes { __typename ... on MarkedAsDuplicateEvent {
                canonical { ... on Issue { number repository { nameWithOwner } } }
                duplicate { ... on Issue { number repository { nameWithOwner } } }
              } ... on UnmarkedAsDuplicateEvent {
                canonical { ... on Issue { number repository { nameWithOwner } } }
                duplicate { ... on Issue { number repository { nameWithOwner } } }
              } }
              pageInfo { hasNextPage endCursor }
            }
          } }
        }`,
          variables: { number, cursor },
        });
        const timeline = response?.data?.repository?.issue?.timelineItems;
        if (!Array.isArray(timeline?.nodes) || typeof timeline?.pageInfo?.hasNextPage !== 'boolean')
          throw new Error('Unreadable duplicate history');
        for (const event of timeline.nodes) {
          if (
            event.duplicate?.number === number &&
            event.duplicate?.repository?.nameWithOwner === ISSUE_REPO
          ) {
            if (event.__typename === 'UnmarkedAsDuplicateEvent')
              history = { state: 'unmarked', number: null };
            else if (event.__typename === 'MarkedAsDuplicateEvent')
              history = {
                state: 'marked',
                number: event.canonical?.number,
                repository: event.canonical?.repository?.nameWithOwner,
              };
          }
        }
        cursor = timeline.pageInfo.hasNextPage ? timeline.pageInfo.endCursor : null;
        if (timeline.pageInfo.hasNextPage && !cursor)
          throw new Error('Truncated duplicate history');
      } while (cursor);
      if (history.state === 'marked') {
        if (history.repository !== ISSUE_REPO)
          throw new Error('Duplicate canonical issue is outside the central tracker');
        if (!Number.isSafeInteger(history.number) || history.number < 1)
          throw new Error('Unreadable duplicate canonical issue');
      }
      return { state: history.state, number: history.number };
    },
    create: (title, body) =>
      validIssue(
        api(`repos/${ISSUE_REPO}/issues`, {
          title,
          body,
          type: 'Bug',
          labels: ['component:intentd', 'agent-filed'],
          assignees: [OWNER],
        }),
      ),
    comment: (number, body) =>
      validComment(api(`repos/${ISSUE_REPO}/issues/${number}/comments`, { body })),
    reopen: (number) => {
      const issue = validIssue(
        api(`repos/${ISSUE_REPO}/issues/${number}`, { state: 'open' }, 'PATCH'),
      );
      if (issue.state !== 'open') throw new Error(`Issue ${number} did not reopen`);
      return issue;
    },
  };
}

export function readJson(path) {
  return JSON.parse(readFileSync(path, 'utf8'));
}
