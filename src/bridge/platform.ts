import { requestWindowsJobStop, windowsJobOf } from './windows-job';
import * as child_process from 'child_process';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import type { ProcessIdentity } from './types';
import { requestWindowsProcessHelper, windowsPowerShell, windowsProcessTable } from './windows-process-helper';

/**
 * Platform facts the bridge needs at runtime.
 *
 * Every OS-specific decision lives here so the rest of the bridge can stay
 * platform-neutral. Nothing in this module may assume Windows: each branch has
 * a POSIX counterpart, and the POSIX path is the one this bridge is developed
 * against locally.
 */

/** True when the bridge runs on Windows. */
export const IS_WINDOWS = process.platform === 'win32';

/** List separator used by `PATH` on this platform. */
export const PATH_DELIMITER = IS_WINDOWS ? ';' : ':';

// Spawn adapters bind the identity they captured for an exact ChildProcess
// handle. Registration can then distinguish a trusted spawn-time identity from
// an arbitrary/stale birthId passed with a numerically matching PID.
const spawnedChildBirthIds = new WeakMap<object, string>();

export function bindSpawnedChildIdentity(child: object, birthId: string | undefined): void {
  if (birthId?.trim()) spawnedChildBirthIds.set(child, birthId.trim());
}

export function getSpawnedChildIdentity(child: object): string | undefined {
  return spawnedChildBirthIds.get(child);
}

/**
 * Extensions the Windows loader appends when resolving a bare command.
 * `PATHEXT` is authoritative when the host defines it.
 */
function windowsExecExtensions(): string[] {
  const raw = process.env.PATHEXT;
  const list = (raw && raw.trim() ? raw : '.COM;.EXE;.BAT;.CMD')
    .split(';')
    .map((entry) => entry.trim().toLowerCase())
    .filter((entry) => entry.startsWith('.'));
  return Array.from(new Set(list));
}

/** The user's home directory. */
export function homeDir(): string {
  return os.homedir();
}

/**
 * Names a bare command may be stored under here.
 *
 * POSIX systems run the literal name; Windows appends a loader extension.
 */
export function binaryCandidates(base: string): string[] {
  if (!IS_WINDOWS) return [base];
  if (windowsExecExtensions().some((ext) => base.toLowerCase().endsWith(ext))) return [base];
  return [base, ...windowsExecExtensions().map((ext) => `${base}${ext}`)];
}

/**
 * The name a bare command is invoked as when no installation could be located.
 *
 * This is only a fallback for error reporting: a missing executable is caught
 * by the caller's guard before it is ever spawned.
 */
export function defaultBinary(base: string): string {
  return IS_WINDOWS ? `${base}.exe` : base;
}

/**
 * Whether this path is a runnable file on the current platform.
 *
 * Windows has no execute bit, so existence is the whole test there. POSIX
 * launchers must also carry the execute bit — an npm shim that lost its mode
 * (`chmod -x`) otherwise looks installed and then fails to spawn.
 */
export function isExecutableFile(candidate: string): boolean {
  try {
    const stat = fs.statSync(candidate);
    if (!stat.isFile()) return false;
    if (IS_WINDOWS) return true;
    fs.accessSync(candidate, fs.constants.X_OK);
    return true;
  } catch {
    return false;
  }
}

/**
 * Resolve a command against `PATH` the way the host shell would.
 *
 * `command` may be an absolute/relative path, which bypasses the scan. The
 * returned path is only ever a file that can actually be launched here.
 */
export function lookupInPath(
  command: string,
  pathValue: string = process.env.PATH || '',
): string | null {
  if (!command) return null;

  // An explicit path is not subject to PATH scanning.
  if (command.includes('/') || (IS_WINDOWS && command.includes('\\'))) {
    return isExecutableFile(command) ? command : null;
  }

  for (const dir of pathValue.split(PATH_DELIMITER)) {
    if (!dir) continue;
    for (const name of binaryCandidates(command)) {
      const full = path.join(dir, name);
      if (isExecutableFile(full)) return full;
    }
  }
  return null;
}

/**
 * Whether launching this executable requires a shell wrapper.
 *
 * Windows npm shims are `.cmd` batch files, which `spawn` cannot execute
 * directly. POSIX launchers are executed as-is, shebang or not.
 */
export function needsShellWrapper(execPath: string): boolean {
  if (!IS_WINDOWS) return false;
  const lower = execPath.toLowerCase();
  return lower.endsWith('.cmd') || lower.endsWith('.bat');
}

/**
 * Every directory that may hold VS Code-family extensions.
 *
 * A remote/SSH workspace installs extensions under `~/.vscode-server` or
 * `~/.vscode-server-insiders`, so scanning only `~/.vscode` misses the binaries
 * that are actually present on a headless host.
 */
export function vscodeExtensionDirs(home: string = homeDir()): string[] {
  return [
    path.join(home, '.vscode', 'extensions'),
    path.join(home, '.vscode-server', 'extensions'),
    path.join(home, '.vscode-insiders', 'extensions'),
    path.join(home, '.vscode-server-insiders', 'extensions'),
  ];
}

/**
 * Relative locations of an extension-bundled CLI binary inside an extension
 * directory, most specific first.
 */
export function bundledCliRelativePaths(base: string): string[] {
  const names = binaryCandidates(base);
  const relative: string[] = [];
  for (const dir of bundledArchDirs()) {
    for (const name of names) relative.push(path.join('bin', dir, name));
  }
  for (const name of names) relative.push(path.join('bin', name));
  return relative;
}

/**
 * Architecture directory names used by extension-bundled CLIs, best match first.
 *
 * The same architecture is spelled differently per vendor: Linux arm64 builds
 * ship as both `linux-arm64` and `linux-aarch64`, so both are probed instead of
 * trusting one convention.
 */
function bundledArchDirs(): string[] {
  const armLike = process.arch === 'arm64' || process.arch === 'arm';
  if (process.platform === 'win32') {
    return armLike ? ['windows-arm64', 'windows-x86_64'] : ['windows-x86_64', 'windows-arm64'];
  }
  if (process.platform === 'darwin') {
    return armLike ? ['darwin-arm64', 'darwin-x86_64'] : ['darwin-x86_64', 'darwin-arm64'];
  }
  return armLike
    ? ['linux-arm64', 'linux-aarch64', 'linux-x86_64']
    : ['linux-x86_64', 'linux-arm64', 'linux-aarch64'];
}

/**
 * Terminate a process and everything it spawned.
 *
 * Windows has no process groups, so `taskkill /T` walks the tree. The bridge
 * spawns agents without `detached`, so POSIX must not signal a process group —
 * that would reach the extension host. Instead the tree is collected from
 * `/proc` and killed bottom-up.
 */
export function killProcessTree(
  child: child_process.ChildProcess,
  expectedBirthId?: string,
  identityWasCaptured: boolean = false,
): void {
  const job = windowsJobOf(child);
  if (job) {
    requestWindowsJobStop(job);
    return;
  }

  const rootPid = child.pid;
  if (!rootPid || child.exitCode !== null || child.signalCode !== null) return;

  // Never call taskkill /T with a numeric PID: if the original root exited and
  // Windows reused the PID, taskkill would walk the replacement process tree.
  const rootBirthId = identityWasCaptured ? expectedBirthId : (expectedBirthId || getProcessBirthId(rootPid));
  if (!rootBirthId) {
    // A launch-time identity that could not be captured is not permission to
    // rediscover a process by PID. Untracked legacy callers may use their live
    // ChildProcess handle for root-only cleanup, but never expand the subtree.
    if (!identityWasCaptured && child.exitCode === null && child.signalCode === null) {
      try { child.kill('SIGKILL'); } catch {}
    }
    return;
  }
  if (probeProcessIdentity(rootPid, rootBirthId) !== 'alive') return;

  let descendants: ProcessIdentity[] = [];
  try {
    descendants = collectDescendants(rootPid).map((pid) => ({ pid, birthId: getProcessBirthId(pid) }));
  } catch {
    return;
  }
  for (const identity of descendants) {
    if (!identity.birthId || probeProcessIdentity(identity.pid, identity.birthId) !== 'alive') continue;
    if (probeProcessIdentity(identity.pid, identity.birthId) !== 'alive') continue;
    try { process.kill(identity.pid, 'SIGKILL'); } catch {}
  }

  // Re-verify the original root immediately before using its child handle. If
  // it exited or its PID was reused, do not signal the replacement process.
  if (probeProcessIdentity(rootPid, rootBirthId) === 'alive') {
    try { child.kill('SIGKILL'); } catch {}
  }
}

/**
 * Terminate a process id and everything it spawned.
 *
 * Used for orphan cleanup when only the recorded pid survives, not a handle.
 */
export type KillPidTreeProvider = (pid: number) => void;
let customKillPidTreeProvider: KillPidTreeProvider | undefined;

export function setKillPidTreeProvider(provider: KillPidTreeProvider | undefined): void {
  customKillPidTreeProvider = provider;
}

export function killPidTree(pid: number): void {
  if (!pid) return;
  if (customKillPidTreeProvider) {
    customKillPidTreeProvider(pid);
    return;
  }

  if (IS_WINDOWS) {
    try {
      child_process.execFileSync('taskkill', ['/pid', String(pid), '/T', '/F'], {
        stdio: 'ignore',
        windowsHide: true,
      });
      return;
    } catch {
      // Fall through to the direct kill below.
    }
  } else {
    for (const descendant of collectDescendants(pid)) {
      try {
        process.kill(descendant, 'SIGKILL');
      } catch {}
    }
  }

  try {
    process.kill(pid, 'SIGKILL');
  } catch {}
}

/**
 * Every descendant PID of `rootPid`, parents before children.
 *
 * Windows uses a CIM process snapshot; Linux reads `/proc/<pid>/stat`.
 */
export function collectDescendants(rootPid: number): number[] {
  if (!rootPid || rootPid <= 0) return [];
  const childrenByParent = new Map<number, number[]>();

  if (IS_WINDOWS) {
    const output = child_process.execFileSync(
      windowsPowerShell,
      ['-NoProfile', '-NonInteractive', '-Command',
        `$ErrorActionPreference = 'Stop'; ConvertTo-Json -InputObject @(${windowsProcessTable}) -Compress`],
      { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'], timeout: 8000, windowsHide: true },
    );
    return windowsDescendants(rootPid, JSON.parse(output));
  } else {
    try {
      const entries = fs.readdirSync('/proc');
      for (const entry of entries) {
        if (!/^\d+$/.test(entry)) continue;
        const pid = Number(entry);
        try {
          const stat = fs.readFileSync(`/proc/${pid}/stat`, 'utf8');
          const closeParen = stat.lastIndexOf(')');
          if (closeParen === -1) continue;
          const ppid = Number(stat.slice(closeParen + 2).split(' ')[1]);
          if (!Number.isFinite(ppid)) continue;
          const siblings = childrenByParent.get(ppid);
          if (siblings) siblings.push(pid);
          else childrenByParent.set(ppid, [pid]);
        } catch {}
      }
    } catch {}
  }

  return descendantsFromTable(rootPid, childrenByParent);
}

function windowsDescendants(rootPid: number, rows: unknown): number[] {
  if (!Array.isArray(rows) || !rows.length) throw new Error('Windows process table is unavailable.');
  const childrenByParent = new Map<number, number[]>();
  const createdAt = new Map<number, string>();
  for (const row of rows) {
    const pid = row?.ProcessId;
    const parent = row?.ParentProcessId;
    if (!Number.isSafeInteger(pid) || !Number.isSafeInteger(parent)) throw new Error('Invalid Windows process table row.');
    if (typeof row.Created === 'string' && /^\d{19}$/.test(row.Created)) createdAt.set(pid, row.Created);
    if (pid <= 0 || parent <= 0) continue;
    const children = childrenByParent.get(parent);
    if (children) children.push(pid);
    else childrenByParent.set(parent, [pid]);
  }
  return descendantsFromTable(rootPid, childrenByParent, (pid, parent) => {
    const childStart = createdAt.get(pid);
    const parentStart = createdAt.get(parent);
    if (!childStart || !parentStart) throw new Error('Windows process tree creation times are unavailable.');
    // ParentProcessId can outlive its original owner and refer to a recycled PID.
    return childStart >= parentStart;
  });
}

export async function collectDescendantsAsync(rootPid: number): Promise<number[]> {
  if (!IS_WINDOWS) return collectDescendants(rootPid);
  return windowsDescendants(rootPid, await requestWindowsProcessHelper({ operation: 'processTable' }));
}

function descendantsFromTable(
  rootPid: number,
  childrenByParent: Map<number, number[]>,
  belongsToParent: (pid: number, parent: number) => boolean = () => true,
): number[] {
  const descendants: number[] = [];
  const queue: number[] = [rootPid];
  const seen = new Set<number>([rootPid]);
  while (queue.length > 0) {
    const current = queue.shift()!;
    for (const pid of childrenByParent.get(current) ?? []) {
      if (!seen.has(pid) && belongsToParent(pid, current)) {
        seen.add(pid);
        descendants.push(pid);
        queue.push(pid);
      }
    }
  }
  return descendants;
}

/**
 * Returns true if candidatePid is rootPid or a descendant of rootPid.
 */
export function isProcessDescendant(candidatePid: number, rootPid: number): boolean {
  if (!candidatePid || !rootPid || candidatePid <= 0 || rootPid <= 0) return false;
  if (candidatePid === rootPid) return true;
  return collectDescendants(rootPid).includes(candidatePid);
}

/**
 * Returns true only if candidatePid is a strict descendant of rootPid (not rootPid itself).
 */
export function isProcessStrictDescendant(candidatePid: number, rootPid: number): boolean {
  if (!candidatePid || !rootPid || candidatePid <= 0 || rootPid <= 0) return false;
  if (candidatePid === rootPid) return false;
  try {
    return collectDescendants(rootPid).includes(candidatePid);
  } catch {
    return false;
  }
}

/**
 * Process birth identity resolver.
 *
 * Windows: process creation time (UTC file time).
 * Linux: starttime tick from /proc/<pid>/stat.
 * macOS: process start time via ps.
 */
let cachedCurrentProcessBirthId: string | undefined;

export type ProcessBirthIdProvider = (pid: number) => string | undefined;
let customBirthIdProvider: ProcessBirthIdProvider | undefined;

export function setProcessBirthIdProvider(provider: ProcessBirthIdProvider | undefined): void {
  customBirthIdProvider = provider;
}

export function getProcessBirthId(pid: number): string | undefined {
  if (!pid || pid <= 0) return undefined;
  if (customBirthIdProvider) {
    return customBirthIdProvider(pid);
  }
  if (pid === process.pid && cachedCurrentProcessBirthId) {
    return cachedCurrentProcessBirthId;
  }
  try {
    if (IS_WINDOWS) {
      const output = child_process.execFileSync(
        windowsPowerShell,
        [
          '-NoProfile',
          '-NonInteractive',
          '-Command',
          `$p = Get-Process -Id ${pid} -ErrorAction SilentlyContinue; if ($p) { $p.StartTime.ToFileTimeUtc() }`,
        ],
        { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'], windowsHide: true, timeout: 8000 },
      );
      const trimmed = output.trim();
      const birthId = /^\d+$/.test(trimmed) ? trimmed : undefined;
      if (pid === process.pid && birthId) cachedCurrentProcessBirthId = birthId;
      return birthId;
    } else if (process.platform === 'linux') {
      const stat = fs.readFileSync(`/proc/${pid}/stat`, 'utf8');
      const closeParen = stat.lastIndexOf(')');
      if (closeParen !== -1) {
        const fields = stat.slice(closeParen + 2).split(' ');
        const starttime = fields[19];
        if (starttime && /^\d+$/.test(starttime)) {
          if (pid === process.pid) cachedCurrentProcessBirthId = starttime;
          return starttime;
        }
      }
    } else {
      const output = child_process.execFileSync(
        'ps',
        ['-o', 'lstart=', '-p', String(pid)],
        { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'], timeout: 3000 },
      );
      const birthId = output.trim();
      if (birthId) {
        if (pid === process.pid) cachedCurrentProcessBirthId = birthId;
        return birthId;
      }
    }
  } catch {
    return undefined;
  }
  return undefined;
}

/** Reads fresh OS identities and preserves the FILETIME strings used by older records. */
export async function getProcessBirthIds(pids: number[]): Promise<Map<number, string>> {
  const unique = [...new Set(pids.filter(pid => Number.isSafeInteger(pid) && pid > 0))];
  const identities = new Map<number, string>();
  if (!unique.length) return identities;
  if (IS_WINDOWS && !customBirthIdProvider) {
    try {
      const reply = await requestWindowsProcessHelper({ operation: 'birthIds', pids: unique }) as Record<string, string>;
      for (const pid of unique) {
        const birthId = reply[pid];
        if (typeof birthId === 'string' && /^\d+$/.test(birthId)) identities.set(pid, birthId);
      }
      return identities;
    } catch {}
  }
  for (const pid of unique) {
    const birthId = getProcessBirthId(pid);
    if (birthId) identities.set(pid, birthId);
  }
  return identities;
}

/** Capture a launched child's OS identity without blocking close delivery on Windows. */
export async function getProcessBirthIdAsync(pid: number): Promise<string | undefined> {
  if (!Number.isSafeInteger(pid) || pid <= 0) return undefined;
  return (await getProcessBirthIds([pid])).get(pid);
}

export type ProcessProbeProvider = (pid: number, expectedBirthId?: string) => 'alive' | 'exited' | 'unknown';
let customProbeProvider: ProcessProbeProvider | undefined;

export function setProcessProbeProvider(provider: ProcessProbeProvider | undefined): void {
  customProbeProvider = provider;
}

/**
 * Probes process identity against expected birth ID to prevent PID reuse.
 * Returns 'alive' if alive and matching, 'exited' if not running or birthId changed,
 * or 'unknown' if permissions prevent checking or birthId is indeterminate.
 */
export function probeProcessIdentity(
  pid: number,
  expectedBirthId?: string,
): 'alive' | 'exited' | 'unknown' {
  if (!pid || pid <= 0) return 'exited';
  if (customProbeProvider) {
    return customProbeProvider(pid, expectedBirthId);
  }
  try {
    process.kill(pid, 0);
  } catch (err: any) {
    if (err?.code === 'EPERM') return 'unknown';
    return 'exited';
  }

  if (!expectedBirthId) {
    return 'alive';
  }

  const actualBirthId = getProcessBirthId(pid);
  if (!actualBirthId) {
    return 'unknown';
  }
  return actualBirthId === expectedBirthId ? 'alive' : 'exited';
}

export async function probeProcessIdentityAsync(
  pid: number,
  expectedBirthId: string,
): Promise<'alive' | 'exited' | 'unknown'> {
  if (!IS_WINDOWS || customProbeProvider || customBirthIdProvider) return probeProcessIdentity(pid, expectedBirthId);
  const observed = probeProcessIdentity(pid);
  if (observed !== 'alive') return observed;
  const actual = (await getProcessBirthIds([pid])).get(pid);
  if (!actual) return probeProcessIdentity(pid) === 'exited' ? 'exited' : 'unknown';
  return actual === expectedBirthId ? 'alive' : 'exited';
}

export type ConfirmProcessTreeExitProvider = (
  pid: number,
  timeoutMs?: number,
  expectedBirthId?: string,
  knownDescendants?: Array<{ pid: number; birthId?: string }>,
  allowFreshRootScan?: boolean,
) => Promise<boolean>;
let customConfirmExitProvider: ConfirmProcessTreeExitProvider | undefined;

export function setConfirmProcessTreeExitProvider(
  provider: ConfirmProcessTreeExitProvider | undefined,
): void {
  customConfirmExitProvider = provider;
}

/**
 * Waits for a process and all its descendants to exit within timeoutMs (default 10s).
 * Tracks descendant PIDs and their birthIds, verifying exit with probeProcessIdentity.
 * Only returns true when root and all descendants are confirmed dead.
 */
export async function confirmProcessTreeExit(
  pid: number,
  timeoutMs: number = 10_000,
  expectedBirthId?: string,
  knownDescendants: Array<{ pid: number; birthId?: string }> = [],
  allowFreshRootScan: boolean = true,
): Promise<boolean> {
  if (!pid || pid <= 0) return true;
  if (customConfirmExitProvider) {
    return customConfirmExitProvider(pid, timeoutMs, expectedBirthId, knownDescendants, allowFreshRootScan);
  }
  const deadline = Date.now() + timeoutMs;
  const knownProcessBirthIds = new Map<string, { pid: number; birthId?: string }>();
  knownProcessBirthIds.set(`${pid}:${expectedBirthId ?? 'unknown'}`, { pid, birthId: expectedBirthId });
  for (const item of knownDescendants) {
    knownProcessBirthIds.set(`${item.pid}:${item.birthId ?? 'unknown'}`, item);
  }
  let enumerationUnconfirmed = false;

  const scanDescendants = async () => {
    // A numeric PID can be reused as soon as the original root exits. Only
    // enumerate a fresh tree while the exact root birth ID is still alive.
    if (!allowFreshRootScan || !expectedBirthId) return;
    const rootStatus = await probeProcessIdentityAsync(pid, expectedBirthId);
    if (rootStatus === 'unknown') enumerationUnconfirmed = true;
    if (rootStatus !== 'alive') return;
    try {
      const descendantPids = await collectDescendantsAsync(pid);
      const birthIds = await getProcessBirthIds([pid, ...descendantPids]);
      if (birthIds.get(pid) !== expectedBirthId) {
        enumerationUnconfirmed = true;
        return;
      }
      enumerationUnconfirmed = false;
      for (const descendantPid of descendantPids) {
        const birthId = birthIds.get(descendantPid);
        const key = `${descendantPid}:${birthId ?? 'unknown'}`;
        if (!knownProcessBirthIds.has(key)) {
          knownProcessBirthIds.set(key, { pid: descendantPid, birthId });
        }
      }
    } catch {
      enumerationUnconfirmed = true;
    }
  };

  // Windows process start-time lookup crosses into PowerShell/WMI. Query the
  // complete retained tree in one bounded batch per poll instead of launching
  // one shell for every PID; missing/ambiguous birth IDs remain unknown.
  const allConfirmedExited = async (): Promise<boolean> => {
    if (enumerationUnconfirmed) return false;
    const identities = [...knownProcessBirthIds.values()];
    if (customProbeProvider || identities.some((identity) => !identity.birthId)) {
      return identities.every((identity) =>
        Boolean(identity.birthId) && probeProcessIdentity(identity.pid, identity.birthId) === 'exited',
      );
    }
    const present: Array<{ pid: number; birthId: string }> = [];
    for (const identity of identities) {
      try {
        process.kill(identity.pid, 0);
        present.push({ pid: identity.pid, birthId: identity.birthId! });
      } catch (error: any) {
        if (error?.code === 'EPERM') return false;
        if (error?.code !== 'ESRCH') return false;
        // The exact PID is absent, so the recorded process identity exited.
      }
    }
    if (present.length === 0) return true;
    const currentBirthIds = await getProcessBirthIds(present.map((identity) => identity.pid));
    return present.every((identity) => {
      const actual = currentBirthIds.get(identity.pid);
      return actual !== undefined && actual !== identity.birthId;
    });
  };

  while (Date.now() < deadline) {
    await scanDescendants();
    if (await allConfirmedExited()) return true;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  return allConfirmedExited();
}
