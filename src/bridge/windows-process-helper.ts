import { spawn } from 'child_process';
import { createHash, randomUUID } from 'crypto';
import { Socket } from 'net';
import { resolve } from 'path';
import { createInterface } from 'readline';
import { setTimeout as delay } from 'timers/promises';

const script = `
$ErrorActionPreference = 'Stop'
$mutexes = @{}
try {
  while ($null -ne ($line = [Console]::ReadLine())) {
    $request = ConvertFrom-Json -InputObject $line
    $reply = @{ id = $request.id }
    try {
      switch ($request.operation) {
        'birthIds' {
          $identities = @{}
          foreach ($processId in $request.pids) {
            try {
              $started = (Get-Process -Id $processId -ErrorAction Stop).StartTime
              $identities[[string]$processId] = $started.ToFileTimeUtc().ToString([Globalization.CultureInfo]::InvariantCulture)
            } catch {}
          }
          $reply.value = $identities
        }
        'acquire' {
          $reply.value = $false
          if ($mutexes.ContainsKey($request.name) -or [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() -gt $request.deadline) { break }
          $mutex = [System.Threading.Mutex]::new($false, $request.name)
          $held = $false
          try {
            try { $held = $mutex.WaitOne(0) } catch [System.Threading.AbandonedMutexException] { $held = $true }
            if ($held) {
              $mutexes[$request.name] = @{ mutex = $mutex; token = $request.token }
              $reply.value = $true
            }
          } finally {
            if (-not $held) { $mutex.Dispose() }
          }
        }
        'release' {
          $lease = $mutexes[$request.name]
          if ($lease -and $lease.token -eq $request.token) {
            $lease.mutex.ReleaseMutex()
            $lease.mutex.Dispose()
            $mutexes.Remove($request.name)
          }
          $reply.value = $true
        }
      }
    } catch {
      $reply.error = $_.Exception.Message
    }
    [Console]::WriteLine((ConvertTo-Json -InputObject $reply -Compress -Depth 4))
  }
} finally {
  foreach ($lease in $mutexes.Values) {
    $lease.mutex.ReleaseMutex()
    $lease.mutex.Dispose()
  }
}
`;

type Request =
  | { operation: 'birthIds'; pids: number[] }
  | { operation: 'acquire'; name: string; token: string; deadline: number }
  | { operation: 'release'; name: string; token: string };

interface Pending {
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
}

function startHelper() {
  // stdin closes with the owner, so the PowerShell thread releases all its mutexes on owner exit.
  const child = spawn('powershell.exe', [
    '-NoLogo', '-NoProfile', '-NonInteractive', '-EncodedCommand', Buffer.from(script, 'utf16le').toString('base64'),
  ], { windowsHide: true, stdio: ['pipe', 'pipe', 'ignore'] });
  const pending = new Map<number, Pending>();
  let sequence = 0;
  const failAll = (error: Error) => {
    for (const request of pending.values()) request.reject(error);
    pending.clear();
  };
  child.unref();
  (child.stdin as Socket).unref?.();
  (child.stdout as Socket).unref?.();
  child.stdin.on('error', failAll);
  child.on('error', failAll);
  child.once('close', () => {
    helper = undefined;
    failAll(new Error('Windows process helper exited'));
  });
  createInterface({ input: child.stdout }).on('line', line => {
    try {
      const reply = JSON.parse(line);
      const request = pending.get(reply.id);
      if (reply.error) request?.reject(new Error(reply.error));
      else request?.resolve(reply.value);
    } catch (error) {
      failAll(error as Error);
    }
  });
  return (request: Request, timeoutMs: number): Promise<unknown> => new Promise((resolve, reject) => {
    const id = ++sequence;
    const finish = () => {
      pending.delete(id);
      clearTimeout(timer);
    };
    const timer = setTimeout(() => {
      finish();
      reject(new Error(`Windows process helper timed out during ${request.operation}`));
    }, timeoutMs);
    pending.set(id, {
      resolve: value => { finish(); resolve(value); },
      reject: error => { finish(); reject(error); },
    });
    child.stdin.write(JSON.stringify({ ...request, id }) + '\n', error => {
      if (error) pending.get(id)?.reject(error);
    });
  });
}

let helper: ReturnType<typeof startHelper> | undefined;

export function requestWindowsProcessHelper(request: Request, timeoutMs = 8000): Promise<unknown> {
  helper ??= startHelper();
  return helper(request, timeoutMs);
}

export function windowsMeshMutexName(lockPath: string): string {
  return 'AgentMeshMerge_' + createHash('sha256').update(resolve(lockPath).toLowerCase()).digest('hex').slice(0, 32);
}

/** Uses the same named mutex as an older MeshFileLock task writer. */
export class WindowsTaskLock {
  private disposed = false;
  private release?: () => Promise<void>;

  constructor(private readonly lockPath: string) {}

  async acquire(onWait?: () => void, cancelled?: () => boolean, timeoutMs = 15_000): Promise<(() => Promise<void>) | undefined> {
    const deadline = Date.now() + timeoutMs;
    const name = windowsMeshMutexName(this.lockPath);
    const token = randomUUID();
    let releasing: Promise<void> | undefined;
    const release = () => releasing ??= requestWindowsProcessHelper({ operation: 'release', name, token }).then(() => undefined);
    while (!this.disposed && !cancelled?.() && Date.now() < deadline) {
      let acquired;
      try {
        acquired = await requestWindowsProcessHelper({ operation: 'acquire', name, token, deadline }, Math.max(1, deadline - Date.now()));
      } catch (error) {
        // A late acquire reply may have been lost. Cancel only this token, in request order.
        await release();
        if (Date.now() >= deadline) return undefined;
        throw error;
      }
      if (acquired === true) {
        if (this.disposed || cancelled?.() || Date.now() >= deadline) {
          await release();
          return undefined;
        }
        this.release = release;
        return release;
      }
      onWait?.();
      await delay(Math.min(25, Math.max(0, deadline - Date.now())));
    }
    return undefined;
  }

  dispose(): void {
    this.disposed = true;
    void this.release?.().catch(() => undefined);
  }
}
