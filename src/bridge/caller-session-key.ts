/** Normalize Codex rollout names and bare UUIDs to the same native session key. */
export function callerSessionKey(callerSessionId: string): string {
  const raw = String(callerSessionId || '');
  const match = /^rollout-\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-([0-9a-f-]{36})$/i.exec(raw);
  return match ? match[1].toLowerCase() : raw;
}
