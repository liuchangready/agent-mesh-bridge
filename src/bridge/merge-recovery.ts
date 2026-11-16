import * as crypto from 'crypto';
import * as fs from 'fs';
import * as path from 'path';
import { DelegationThread, enterTaskFailure } from './types';

export function createMergeLog(dir: string, safeId: string, number: number, content: string): { logPath: string; logFileNumber: number } {
  fs.mkdirSync(dir, { recursive: true });
  for (let logFileNumber = number; ; logFileNumber++) {
    const logPath = path.join(dir, `${safeId}_merge_${logFileNumber}.log`);
    try { fs.writeFileSync(logPath, content, { encoding: 'utf8', flag: 'wx' }); return { logPath, logFileNumber }; }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error; }
  }
}

export function failureFingerprint(phase: string, code: string, summary: string, items: string[] = []): string {
  const normalized = summary.toLowerCase().replace(/\u001b\[[0-9;]*m/g, '')
    .replace(/\b[0-9a-f]{40,64}\b/g, '<sha>')
    .replace(/\b[0-9a-f]{8}-[0-9a-f-]{27,}\b/g, '<id>')
    .replace(/\b\d{4}-\d\d-\d\dt\S+/g, '<time>')
    .replace(/\b[a-z]:[\\/][^\s:'"]+/g, '<path>')
    .replace(/\/(?:tmp|var\/tmp|private\/tmp)\/[^\s:'"]+/g, '<path>')
    .replace(/\b\d+(?:\.\d+)?\s*(?:ms|seconds?|minutes?)\b/g, '<duration>')
    .replace(/\s+/g, ' ').trim().slice(0, 2000);
  return crypto.createHash('sha256').update(JSON.stringify([phase.toLowerCase(), code.toUpperCase(), normalized, [...items].sort()])).digest('hex');
}

/** Count only completed failures; waiting for an admission or a quota reset is not a retry. */
export function noteMergeFailure(thread: DelegationThread, phase: string, code: string, summary: string, items: string[] = []): {
  fingerprint: string; retryGeneration: number; consecutiveRetries: number;
} {
  const previous = thread.mergeControl;
  const fingerprint = failureFingerprint(phase, code, summary, items);
  const retryGeneration = previous?.retryGeneration || 0;
  const consecutiveRetries = previous?.failureFingerprint === fingerprint ? previous.consecutiveRetries + 1 : 0;
  thread.mergeControl = { ...previous, revision: (previous?.revision || 0) + 1, retryGeneration, failureFingerprint: fingerprint, consecutiveRetries };
  if (consecutiveRetries >= 6 && !(thread.status === 'failed' && thread.failure?.stage === 'merge')) {
    enterTaskFailure(thread, { stage: 'merge', reason: summary.trim() || code, code, at: new Date().toISOString(), round: thread.currentRound });
  }
  return { fingerprint, retryGeneration, consecutiveRetries };
}

export function resetMergeBudget(thread: DelegationThread): void {
  const previous = thread.mergeControl;
  thread.mergeControl = { revision: (previous?.revision || 0) + 1, retryGeneration: (previous?.retryGeneration || 0) + 1, consecutiveRetries: 0 };
}
