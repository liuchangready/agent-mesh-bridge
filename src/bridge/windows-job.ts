import { ChildProcess, spawn, SpawnOptions } from 'child_process';
import { randomUUID } from 'crypto';
import { mkdirSync, readFileSync, writeFileSync } from 'fs';
import { basename, isAbsolute, join, resolve } from 'path';

export interface WindowsJobBinding {
  id: string;
  prefix: string;
}

const jobs = new WeakMap<ChildProcess, WindowsJobBinding>();
const executable = resolve(__dirname, '../../bin/agent-bridge-windows-job.exe');

export function spawnWindowsJob(
  command: string,
  args: readonly string[],
  options: SpawnOptions,
  receiptDirectory: string,
  ownerBirthId: string,
): ChildProcess {
  if (!ownerBirthId) throw new Error('Cannot launch a Windows Job without the controller birthId.');
  const id = randomUUID();
  mkdirSync(receiptDirectory, { recursive: true });
  const prefix = resolve(join(receiptDirectory, 'bridge-job-' + id));
  const child = spawn(executable, [prefix, String(process.pid), ownerBirthId,
    options.shell ? 'cmd' : 'direct', command, ...args], {
    ...options, detached: true, shell: false, windowsVerbatimArguments: false,
  });
  jobs.set(child, { id, prefix });
  return child;
}

export function windowsJobOf(child?: ChildProcess): WindowsJobBinding | undefined {
  return child && jobs.get(child);
}

export function startWindowsJob(child?: ChildProcess): void {
  const job = windowsJobOf(child);
  if (job) writeFileSync(job.prefix + '.start', '');
}

function validJob(job: WindowsJobBinding): boolean {
  return /^[0-9a-f-]{36}$/.test(job.id) && typeof job.prefix === 'string' &&
    isAbsolute(job.prefix) && basename(job.prefix) === 'bridge-job-' + job.id;
}

export function requestWindowsJobStop(job: WindowsJobBinding): boolean {
  if (!validJob(job)) return false;
  try {
    writeFileSync(job.prefix + '.stop', '');
    return true;
  } catch {
    return false;
  }
}

export function windowsJobFinished(job: WindowsJobBinding): boolean {
  if (!validJob(job)) return false;
  try {
    return readFileSync(job.prefix + '.done', 'utf8') === '1';
  } catch {
    return false;
  }
}

export async function stopWindowsJob(job: WindowsJobBinding, timeoutMs: number): Promise<boolean> {
  if (windowsJobFinished(job)) return true;
  if (!requestWindowsJobStop(job)) return false;
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (windowsJobFinished(job)) return true;
    await new Promise(resolve => setTimeout(resolve, 25));
  }
  return windowsJobFinished(job);
}
