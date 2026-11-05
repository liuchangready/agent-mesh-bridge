import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execFile } from 'child_process';
import { needsShellWrapper } from './platform';

/**
 * Codex session ids come in two spellings.
 *
 * `codex exec --json` (and the bridge) captures the bare thread UUID
 * (`01a17d29-d40d-74c3-831d-878dd4edc0e5`); a rollout file name looks like
 * `rollout-2026-10-27T19-40-56-01a17d29-…`.
 *
 * Which spelling `resume` accepts depends on the CLI generation:
 *  - 0.16x resolves by thread UUID (or a thread name) and rejects a rollout
 *    file name with `No saved session found with ID rollout-…`;
 *  - older builds resolved through the rollout record and rejected a bare UUID
 *    with `no rollout found for thread id <uuid>`.
 *
 * The bridge therefore stores the canonical UUID and resolves the argument at
 * call time from the launcher's own `resume --help` (probed once, cached). The
 * persisted id never depends on which build happens to be installed.
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

/** The canonical thread UUID, whatever spelling the ledger/store used. */
export function extractCodexThreadId(sessionId: string): string {
  const raw = String(sessionId || '').trim();
  const match = ROLLOUT_NAME.exec(raw);
  return match ? match[1] : raw;
}

/** What one Codex launcher supports, decided from its own `resume --help`. */
export interface CodexResumeSupport {
  /** Advertises `--no-daemon` (the extension-bundled thin client needs it). */
  noDaemon: boolean;
  /** Resolves `resume` by thread UUID (0.16x). False means legacy rollout names. */
  uuidIds: boolean;
}

const supportCache = new Map<string, CodexResumeSupport>();

/** Seed the cache (activation probe, tests). */
export function rememberCodexResumeSupport(
  executable: string,
  support: CodexResumeSupport,
): void {
  if (executable) supportCache.set(executable, support);
}

export function cachedCodexResumeSupport(
  executable?: string | null,
): CodexResumeSupport | undefined {
  if (!executable) return undefined;
  return supportCache.get(executable);
}

/**
 * Parse `codex resume --help`.
 *
 * 0.16x says "Session id (UUID) or session name. UUIDs take precedence if it
 * parses"; legacy builds talk about the rollout record. An unreadable help is
 * treated as modern (UUID), which is the form current CLIs resolve.
 */
export function parseCodexResumeHelp(text: string): CodexResumeSupport {
  const value = String(text || '');
  const mentionsUuid = /\(UUID\)|UUIDs? take precedence/i.test(value);
  const mentionsRollout = /rollout record|rollout file/i.test(value);
  return {
    noDaemon: /--no-daemon/.test(value),
    uuidIds: mentionsUuid || !mentionsRollout,
  };
}

/** Probe one launcher once; concurrent callers share the cached result. */
export function probeCodexResumeSupport(
  executable: string,
  timeoutMs: number = 4000,
): Promise<CodexResumeSupport> {
  const cached = supportCache.get(executable);
  if (cached) return Promise.resolve(cached);

  return new Promise<CodexResumeSupport>((resolve) => {
    let settled = false;
    const finish = (support: CodexResumeSupport) => {
      if (settled) return;
      settled = true;
      supportCache.set(executable, support);
      resolve(support);
    };

    try {
      const wrap = needsShellWrapper(executable);
      const command = wrap ? process.env.ComSpec || 'cmd.exe' : executable;
      const args = wrap ? ['/c', executable, 'resume', '--help'] : ['resume', '--help'];
      const child = execFile(
        command,
        args,
        { timeout: timeoutMs, windowsHide: true, encoding: 'utf8' },
        (error, stdout, stderr) => {
          const text = `${stdout || ''}\n${stderr || ''}`;
          if (error && !text.trim()) {
            finish({ noDaemon: false, uuidIds: true });
            return;
          }
          finish(parseCodexResumeHelp(text));
        },
      );
      child.stdin?.end();
    } catch {
      finish({ noDaemon: false, uuidIds: true });
    }
  });
}

/**
 * The id to hand to `codex resume` / `codex exec resume`.
 *
 * Modern CLI: the bare thread UUID (a rollout file name is not an id).
 * Legacy CLI: the rollout file name, when it can be found.
 */
export function codexResumeArgument(
  sessionId: string,
  support?: CodexResumeSupport,
  codexHome?: string,
): string {
  const raw = String(sessionId || '').trim();
  if (!raw) return raw;
  if (support && support.uuidIds === false) return resolveCodexResumeId(raw, codexHome);
  return extractCodexThreadId(raw);
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
