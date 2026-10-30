/**
 * Detect a stale extension host.
 *
 * VS Code keeps the extension host (and the JS it loaded) alive across a
 * reinstall: installing a `.vsix` replaces files on disk, it does not restart
 * the process that already imported them. A same-version force-reinstall is
 * especially easy to miss, because VS Code sees no version change and often
 * shows no "Reload Required" prompt at all. The result is the worst debugging
 * state there is: the files on disk are new, the code in memory is old, and a
 * brand-new feature looks like a plain bug.
 *
 * This module answers "is the code I am executing older than the code on disk?"
 * without needing a build id, using two independent on-disk signals:
 *
 *  1. `package.json` -> `__metadata.installedTimestamp`: written by VS Code when
 *     it installs this extension folder, so it catches "reinstalled while the
 *     window was open" even when the archive preserved the build's mtimes.
 *  2. `out/extension.js` mtime: catches `tsc -w` recompiles and plain file
 *     copies, which never touch the manifest.
 *
 * Both are compared with the moment this module was imported (not the process
 * start: the extension can be activated long after startup), so a normal reload,
 * an install before this window opened, or a build that finished before the
 * module loaded cannot produce a false alarm.
 *
 * A second, independent case is "a NEWER version is installed in a sibling
 * folder": that old folder keeps running until the window reloads, and the user
 * may have dismissed the prompt. `findNewerInstalledVersion()` covers it.
 *
 * Everything here is pure/best-effort: an unreadable file yields "unknown", and
 * an unknown build is never reported as stale.
 */

import * as fs from 'fs';
import * as path from 'path';

/** Slack for an install/compile that lands while activation is still running. */
export const DEFAULT_STALE_TOLERANCE_MS = 3_000;

/** What the on-disk extension folder says about itself. */
export interface BuildStamp {
  /** Version declared by the on-disk manifest. */
  version?: string;
  /** When VS Code last (re)installed this folder, if it recorded one. */
  installedAtMs?: number;
  /** mtime of the compiled entry script (dev / copy / watch fallback). */
  scriptMtimeMs?: number;
}

export type StaleReason = 'files-replaced-after-load' | 'newer-version-installed';

export interface FreshnessInput {
  /** When this extension module was read into memory. */
  moduleLoadedAtMs: number;
  stamp: BuildStamp;
  /** A newer version available in a sibling folder, when the caller found one. */
  newerInstalledVersion?: string;
  toleranceMs?: number;
}

export interface FreshnessVerdict {
  stale: boolean;
  reasons: StaleReason[];
  /** Newest on-disk timestamp minus module load time (negative = older). */
  driftMs: number;
  runningVersion?: string;
  newerInstalledVersion?: string;
}

/**
 * Compare the code in memory with the files on disk.
 *
 * Unknown timestamps (missing manifest metadata, unreadable script) contribute
 * nothing: a build we cannot date is never reported as stale.
 */
export function detectStaleBuild(input: FreshnessInput): FreshnessVerdict {
  const tolerance = Number.isFinite(input.toleranceMs)
    ? Math.max(0, Number(input.toleranceMs))
    : DEFAULT_STALE_TOLERANCE_MS;

  const candidates = [input.stamp.installedAtMs, input.stamp.scriptMtimeMs].filter(
    (value): value is number => typeof value === 'number' && Number.isFinite(value),
  );
  const newest = candidates.length > 0 ? Math.max(...candidates) : undefined;
  const driftMs = newest === undefined ? 0 : newest - input.moduleLoadedAtMs;

  const reasons: StaleReason[] = [];
  if (newest !== undefined && driftMs > tolerance) {
    reasons.push('files-replaced-after-load');
  }

  const newer = input.newerInstalledVersion;
  const running = input.stamp.version;
  if (newer && (!running || compareVersions(newer, running) > 0)) {
    reasons.push('newer-version-installed');
  }

  return {
    stale: reasons.length > 0,
    reasons,
    driftMs,
    runningVersion: running,
    newerInstalledVersion: reasons.includes('newer-version-installed') ? newer : undefined,
  };
}

/** Read the manifest metadata and entry-script mtime of an extension folder. */
export function readBuildStamp(extensionPath: string): BuildStamp {
  const stamp: BuildStamp = {};

  try {
    const manifest = JSON.parse(
      fs.readFileSync(path.join(extensionPath, 'package.json'), 'utf8'),
    ) as { version?: unknown; __metadata?: { installedTimestamp?: unknown } };
    if (typeof manifest.version === 'string') stamp.version = manifest.version;
    const installed = manifest.__metadata?.installedTimestamp;
    if (typeof installed === 'number' && Number.isFinite(installed)) {
      stamp.installedAtMs = installed;
    }
  } catch {
    // Not an installed extension (F5 dev host, or an unreadable folder).
  }

  try {
    stamp.scriptMtimeMs = fs.statSync(path.join(extensionPath, 'out', 'extension.js')).mtimeMs;
  } catch {
    // Uncompiled / moved entry script: the manifest signal still applies.
  }

  return stamp;
}

/**
 * A newer version of the SAME extension sitting next to this folder.
 *
 * VS Code installs `<publisher>.<name>-<version>` folders in ~/.vscode/extensions
 * and keeps the old one until the window reloads. Scanning siblings is the only
 * way to notice "the user already installed 1.1.1, this window still runs 1.1.0"
 * from inside the running (old) copy.
 */
export function findNewerInstalledVersion(options: {
  extensionPath: string;
  extensionId?: string;
  runningVersion?: string;
}): string | undefined {
  const { extensionPath, extensionId, runningVersion } = options;
  if (!extensionId || !runningVersion) return undefined;

  const root = path.dirname(extensionPath);
  let newest: string | undefined;

  try {
    for (const entry of fs.readdirSync(root)) {
      if (!entry.startsWith(`${extensionId}-`)) continue;
      const version = parseFolderVersion(entry.slice(extensionId.length + 1));
      if (!version) continue;
      if (!newest || compareVersions(version, newest) > 0) newest = version;
    }
  } catch {
    return undefined;
  }

  return newest && compareVersions(newest, runningVersion) > 0 ? newest : undefined;
}

/**
 * `1.2.3`, `1.2.3-win32-x64`, `1.2.3-rc.1` -> the release version (`1.2.3`).
 *
 * Everything after the numeric triple is dropped on purpose. VS Code appends a
 * platform/arch suffix to installed folders (`-win32-x64`, `-linux-arm64`), and
 * treating that as a semver prerelease would sort the SAME version below itself
 * — a same-version platform build would then look older and never be reported.
 * Prerelease suffixes are dropped too; for a "you should reload" nudge the
 * numeric triple is precise enough.
 */
export function parseFolderVersion(suffix: string): string | undefined {
  const match = /^(\d+\.\d+\.\d+)/.exec((suffix || '').trim());
  return match ? match[1] : undefined;
}

/**
 * Numeric version comparison; a prerelease sorts below its release.
 *
 * Deliberately not full semver: extension folders only ever carry
 * `major.minor.patch[-suffix]`, and a wrong-but-close ordering is harmless here
 * (the user only gets an extra nudge to reload).
 */
export function compareVersions(a: string, b: string): number {
  const left = splitVersion(a);
  const right = splitVersion(b);

  for (let i = 0; i < 3; i++) {
    const diff = (left.numbers[i] || 0) - (right.numbers[i] || 0);
    if (diff !== 0) return diff > 0 ? 1 : -1;
  }

  if (left.prerelease === right.prerelease) return 0;
  if (!left.prerelease) return 1;
  if (!right.prerelease) return -1;
  return left.prerelease > right.prerelease ? 1 : -1;
}

function splitVersion(version: string): { numbers: number[]; prerelease: string } {
  const trimmed = String(version || '')
    .trim()
    .replace(/^v/i, '')
    // VS Code's target-platform suffix is not a prerelease: `1.1.0-win32-x64`
    // and `1.1.0` must compare equal, or the same build looks older than itself.
    .replace(/-(?:win32|linux|alpine|darwin|web)(?:-(?:x64|ia32|arm64|armhf))?$/i, '');
  const [core, prerelease = ''] = trimmed.split('-', 2);
  const numbers = core.split('.').map((part) => {
    const parsed = Number.parseInt(part, 10);
    return Number.isFinite(parsed) ? parsed : 0;
  });
  while (numbers.length < 3) numbers.push(0);
  return { numbers, prerelease };
}

/** Human-readable one-liner for the warning notification and the log. */
export function describeStaleBuild(verdict: FreshnessVerdict): string {
  if (!verdict.stale) return '';

  const parts: string[] = [];
  if (verdict.reasons.includes('files-replaced-after-load')) {
    const seconds = Math.max(0, Math.round(verdict.driftMs / 1000));
    parts.push(`磁盘上的扩展文件比当前 extension host 加载的代码新约 ${seconds} 秒`);
  }
  if (verdict.newerInstalledVersion) {
    parts.push(
      `已安装新版本 ${verdict.newerInstalledVersion}` +
        (verdict.runningVersion ? `（当前窗口运行 ${verdict.runningVersion}）` : ''),
    );
  }

  return (
    `检测到扩展构建落后：${parts.join('；')}。` +
    '当前窗口仍在运行旧代码，新功能不会出现；重载窗口即可切换到新构建。'
  );
}
