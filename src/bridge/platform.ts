import * as child_process from 'child_process';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

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
export function killProcessTree(child: child_process.ChildProcess): void {
  if (!child.pid) return;
  killPidTree(child.pid);
  try {
    child.kill('SIGKILL');
  } catch {}
}

/**
 * Terminate a process id and everything it spawned.
 *
 * Used for orphan cleanup when only the recorded pid survives, not a handle.
 */
export function killPidTree(pid: number): void {
  if (!pid) return;

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
 * Linux exposes the parent link in `/proc/<pid>/stat`; elsewhere (macOS) there
 * is no dependency-free source, so callers degrade to killing the direct child.
 */
function collectDescendants(rootPid: number): number[] {
  let entries: string[];
  try {
    entries = fs.readdirSync('/proc');
  } catch {
    return [];
  }

  const childrenByParent = new Map<number, number[]>();
  for (const entry of entries) {
    if (!/^\d+$/.test(entry)) continue;
    const pid = Number(entry);
    let stat: string;
    try {
      stat = fs.readFileSync(`/proc/${pid}/stat`, 'utf8');
    } catch {
      continue; // The process exited while we were scanning.
    }
    // `comm` (field 2) may contain spaces and parentheses, so the parent id is
    // parsed after the final ')': state ppid pgrp ...
    const closeParen = stat.lastIndexOf(')');
    if (closeParen === -1) continue;
    const ppid = Number(stat.slice(closeParen + 2).split(' ')[1]);
    if (!Number.isFinite(ppid)) continue;
    const siblings = childrenByParent.get(ppid);
    if (siblings) siblings.push(pid);
    else childrenByParent.set(ppid, [pid]);
  }

  const descendants: number[] = [];
  const queue: number[] = [rootPid];
  while (queue.length > 0) {
    const current = queue.shift()!;
    for (const pid of childrenByParent.get(current) ?? []) {
      descendants.push(pid);
      queue.push(pid);
    }
  }
  return descendants;
}
