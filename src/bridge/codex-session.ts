import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

/**
 * Codex session ids come in two spellings.
 *
 * `codex exec --json` (and the bridge) sometimes captures the bare thread UUID
 * (`01a17d29-d40d-74c3-831d-878dd4edc0e5`); a session index or a rollout file
 * name yields `rollout-2026-10-27T19-40-56-01a17d29-…`. `codex exec resume`
 * resolves the thread through its rollout record, so the bare UUID fails with
 *
 *   thread/resume: thread/resume failed: no rollout found for thread id <uuid>
 *
 * even though the session exists on disk. Normalise a bare UUID to the rollout
 * file name before resuming — the bridge must not require the caller to know
 * which spelling its own CLI happened to print.
 */

const UUID = '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}';
const ROLLOUT_NAME = new RegExp(`^rollout-\\d{4}-\\d{2}-\\d{2}T\\d{2}-\\d{2}-\\d{2}-(${UUID})$`, 'i');
const BARE_UUID = new RegExp(`^${UUID}$`, 'i');

export function isCodexRolloutName(sessionId: string): boolean {
  return ROLLOUT_NAME.test(String(sessionId || '').trim());
}

export function isBareCodexThreadId(sessionId: string): boolean {
  return BARE_UUID.test(String(sessionId || '').trim());
}

/**
 * The id to pass to `codex exec resume`.
 *
 * Returns the input untouched for any other agent/spelling, or when the
 * rollout cannot be found (the caller then sees codex's own error instead of a
 * silently rewritten id).
 */
export function resolveCodexResumeId(sessionId: string, codexHome?: string): string {
  const raw = String(sessionId || '').trim();
  if (!raw || !isBareCodexThreadId(raw)) return raw;

  const home = codexHome || path.join(os.homedir(), '.codex');
  const found = findRolloutFile(path.join(home, 'sessions'), raw.toLowerCase());
  if (!found) return raw;
  return path.basename(found, '.jsonl');
}

/** Newest-first scan of `sessions/<yyyy>/<mm>/<dd>/rollout-*.jsonl`. */
function findRolloutFile(sessionsDir: string, uuid: string): string | undefined {
  let years: string[];
  try {
    years = fs.readdirSync(sessionsDir).filter((name) => /^\d{4}$/.test(name)).sort().reverse();
  } catch {
    return undefined;
  }
  const suffix = `-${uuid}.jsonl`;
  for (const year of years) {
    for (const month of readDirs(path.join(sessionsDir, year))) {
      for (const day of readDirs(path.join(sessionsDir, year, month))) {
        const dir = path.join(sessionsDir, year, month, day);
        let entries: string[];
        try {
          entries = fs.readdirSync(dir);
        } catch {
          continue;
        }
        const hit = entries.find(
          (name) => name.toLowerCase().endsWith(suffix) && name.startsWith('rollout-'),
        );
        if (hit) return path.join(dir, hit);
      }
    }
  }
  return undefined;
}

function readDirs(dir: string): string[] {
  try {
    return fs
      .readdirSync(dir, { withFileTypes: true })
      .filter((entry) => entry.isDirectory())
      .map((entry) => entry.name)
      .sort()
      .reverse();
  } catch {
    return [];
  }
}
