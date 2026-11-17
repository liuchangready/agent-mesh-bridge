const { readdirSync } = require('node:fs');
const { resolve } = require('node:path');
const { run } = require('node:test');
const { spec } = require('node:test/reporters');

// Start the longest integration files first so they do not become a late tail.
const priority = new Map([['git-worktree-stash.test.js', 0], ['git-worktree-merge.test.js', 1]]);
const files = readdirSync('out/test', { recursive: true })
  .filter(file => file.endsWith('.test.js'))
  .sort((a, b) => (priority.get(a) ?? 2) - (priority.get(b) ?? 2) || a.localeCompare(b))
  .map(file => resolve('out/test', file));
if (!files.length) throw new Error('No compiled tests found; run npm run compile first.');

run({ files, concurrency: 4 })
  .on('test:fail', () => { process.exitCode = 1; })
  .compose(spec)
  .pipe(process.stdout);
