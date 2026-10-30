import { test, describe } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  DEFAULT_STALE_TOLERANCE_MS,
  compareVersions,
  describeStaleBuild,
  detectStaleBuild,
  findNewerInstalledVersion,
  parseFolderVersion,
  readBuildStamp,
} from '../bridge/build-staleness';

const LOADED_AT = 1_000_000_000_000; // fixed reference for every case

describe('detectStaleBuild', () => {
  test('a build installed and compiled before this module loaded is fresh', () => {
    const verdict = detectStaleBuild({
      moduleLoadedAtMs: LOADED_AT,
      stamp: { version: '1.1.0', installedAtMs: LOADED_AT - 60_000, scriptMtimeMs: LOADED_AT - 90_000 },
    });

    assert.strictEqual(verdict.stale, false);
    assert.deepStrictEqual(verdict.reasons, []);
    assert.ok(verdict.driftMs < 0);
  });

  test('a reinstall after this module loaded is stale (same-version force reinstall)', () => {
    const verdict = detectStaleBuild({
      moduleLoadedAtMs: LOADED_AT,
      stamp: { version: '1.1.0', installedAtMs: LOADED_AT + 30_000 },
    });

    assert.strictEqual(verdict.stale, true);
    assert.deepStrictEqual(verdict.reasons, ['files-replaced-after-load']);
    assert.strictEqual(verdict.driftMs, 30_000);
  });

  test('a `tsc -w` recompile after load is stale even without manifest metadata', () => {
    const verdict = detectStaleBuild({
      moduleLoadedAtMs: LOADED_AT,
      stamp: { version: '1.1.0', scriptMtimeMs: LOADED_AT + 5_000 },
    });

    assert.strictEqual(verdict.stale, true);
    assert.deepStrictEqual(verdict.reasons, ['files-replaced-after-load']);
  });

  test('the newest signal wins when the manifest is older than the script', () => {
    const verdict = detectStaleBuild({
      moduleLoadedAtMs: LOADED_AT,
      stamp: { installedAtMs: LOADED_AT - 10_000, scriptMtimeMs: LOADED_AT + 20_000 },
    });

    assert.strictEqual(verdict.stale, true);
    assert.strictEqual(verdict.driftMs, 20_000);
  });

  test('a write that lands inside the tolerance window is not reported', () => {
    const verdict = detectStaleBuild({
      moduleLoadedAtMs: LOADED_AT,
      stamp: { installedAtMs: LOADED_AT + DEFAULT_STALE_TOLERANCE_MS },
    });

    assert.strictEqual(verdict.stale, false);
  });

  test('the tolerance is configurable', () => {
    const input = {
      moduleLoadedAtMs: LOADED_AT,
      stamp: { installedAtMs: LOADED_AT + 10_000 },
    };
    assert.strictEqual(detectStaleBuild(input).stale, true);
    assert.strictEqual(detectStaleBuild({ ...input, toleranceMs: 10_000 }).stale, false);
  });

  test('an undatable build is never called stale', () => {
    const verdict = detectStaleBuild({ moduleLoadedAtMs: LOADED_AT, stamp: {} });

    assert.strictEqual(verdict.stale, false);
    assert.strictEqual(verdict.driftMs, 0);
  });

  test('a newer version in a sibling folder is stale even when this folder is untouched', () => {
    const verdict = detectStaleBuild({
      moduleLoadedAtMs: LOADED_AT,
      stamp: { version: '1.1.0' },
      newerInstalledVersion: '1.1.1',
    });

    assert.strictEqual(verdict.stale, true);
    assert.deepStrictEqual(verdict.reasons, ['newer-version-installed']);
    assert.strictEqual(verdict.newerInstalledVersion, '1.1.1');
    assert.strictEqual(verdict.runningVersion, '1.1.0');
  });

  test('an equal or older sibling version is ignored', () => {
    const verdict = detectStaleBuild({
      moduleLoadedAtMs: LOADED_AT,
      stamp: { version: '1.1.0' },
      newerInstalledVersion: '1.1.0',
    });

    assert.strictEqual(verdict.stale, false);
  });
});

describe('compareVersions', () => {
  test('compares numerically, not lexically', () => {
    assert.ok(compareVersions('1.10.0', '1.9.9') > 0);
    assert.ok(compareVersions('0.9.0', '1.0.0') < 0);
    assert.strictEqual(compareVersions('1.1.0', '1.1.0'), 0);
  });

  test('a prerelease sorts below its release', () => {
    assert.ok(compareVersions('1.1.0-rc.1', '1.1.0') < 0);
    assert.ok(compareVersions('1.1.0', '1.1.0-rc.1') > 0);
    assert.ok(compareVersions('1.1.0-rc.2', '1.1.0-rc.1') > 0);
  });

  test('tolerates a leading v and short forms', () => {
    assert.strictEqual(compareVersions('v1.2.3', '1.2.3'), 0);
    assert.strictEqual(compareVersions('1.2', '1.2.0'), 0);
  });
});

describe('parseFolderVersion', () => {
  test('extracts the version from an installed folder suffix', () => {
    assert.strictEqual(parseFolderVersion('1.1.0'), '1.1.0');
    assert.strictEqual(parseFolderVersion('1.1.0-rc.1'), '1.1.0');
  });

  test('drops the platform/arch suffix instead of treating it as a prerelease', () => {
    // `1.1.0-win32-x64` must compare EQUAL to `1.1.0`, otherwise a same-version
    // platform build sorts below itself and the newer-version check misfires.
    assert.strictEqual(parseFolderVersion('1.1.0-win32-x64'), '1.1.0');
    assert.strictEqual(parseFolderVersion('1.1.0-linux-arm64'), '1.1.0');
    assert.strictEqual(compareVersions('1.1.0-win32-x64', '1.1.0'), 0);
  });

  test('ignores a folder that carries no version', () => {
    assert.strictEqual(parseFolderVersion('unpacked'), undefined);
  });
});

describe('readBuildStamp', () => {
  test('reads version, install timestamp and the entry script mtime', () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'bridge-build-stamp-'));
    try {
      fs.writeFileSync(
        path.join(root, 'package.json'),
        JSON.stringify({ name: 'agent-mesh-bridge', version: '1.1.0', __metadata: { installedTimestamp: 123456789 } }),
      );
      fs.mkdirSync(path.join(root, 'out'));
      fs.writeFileSync(path.join(root, 'out', 'extension.js'), '// compiled');

      const stamp = readBuildStamp(root);

      assert.strictEqual(stamp.version, '1.1.0');
      assert.strictEqual(stamp.installedAtMs, 123456789);
      const scriptMtime = fs.statSync(path.join(root, 'out', 'extension.js')).mtimeMs;
      assert.strictEqual(stamp.scriptMtimeMs, scriptMtime);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  test('an unreadable folder yields an empty stamp instead of throwing', () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'bridge-build-empty-'));
    try {
      assert.deepStrictEqual(readBuildStamp(root), {});
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});

describe('findNewerInstalledVersion', () => {
  test('finds a newer sibling folder for the same extension', () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'bridge-extensions-'));
    try {
      const extensionId = 'agent-mesh.agent-mesh-bridge';
      const current = path.join(root, `${extensionId}-1.1.0`);
      fs.mkdirSync(current);
      fs.mkdirSync(path.join(root, `${extensionId}-1.1.1`));
      fs.mkdirSync(path.join(root, 'someone-else.other-9.9.9'));

      assert.strictEqual(
        findNewerInstalledVersion({
          extensionPath: current,
          extensionId,
          runningVersion: '1.1.0',
        }),
        '1.1.1',
      );
      assert.strictEqual(
        findNewerInstalledVersion({
          extensionPath: current,
          extensionId,
          runningVersion: '1.1.1',
        }),
        undefined,
      );
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  test('an unknown extension id or running version disables the check', () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'bridge-extensions-none-'));
    try {
      const current = path.join(root, 'agent-mesh.agent-mesh-bridge-1.1.0');
      fs.mkdirSync(current);

      assert.strictEqual(
        findNewerInstalledVersion({ extensionPath: current, runningVersion: '1.1.0' }),
        undefined,
      );
      assert.strictEqual(
        findNewerInstalledVersion({
          extensionPath: current,
          extensionId: 'agent-mesh.agent-mesh-bridge',
        }),
        undefined,
      );
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});

describe('describeStaleBuild', () => {
  test('is empty for a fresh host', () => {
    assert.strictEqual(
      describeStaleBuild({ stale: false, reasons: [], driftMs: 0 }),
      '',
    );
  });

  test('names the drift in seconds', () => {
    const text = describeStaleBuild({
      stale: true,
      reasons: ['files-replaced-after-load'],
      driftMs: 42_000,
    });

    assert.ok(text.includes('42 秒'), text);
    assert.ok(text.includes('重载'), text);
  });

  test('names the newer installed version', () => {
    const text = describeStaleBuild({
      stale: true,
      reasons: ['newer-version-installed'],
      driftMs: 0,
      runningVersion: '1.1.0',
      newerInstalledVersion: '1.1.1',
    });

    assert.ok(text.includes('1.1.1'), text);
    assert.ok(text.includes('1.1.0'), text);
  });
});
