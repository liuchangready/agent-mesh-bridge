import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

export type CancellationAction = 'pause' | 'stop' | 'delete';

export interface CancellationRequest {
  action: CancellationAction;
  reason: string;
  at: string;
  by?: string;
}

const CANCELLATION_DIR = 'cancellations';

/**
 * Cross-process cancellation channel.
 *
 * Pause/delete may be requested from a different process than the one actually
 * running the agent (the VSCode extension host vs. an agent's own MCP server).
 * A small per-thread file bridges that gap: the runner polls it while the child
 * is alive and kills the process tree when it appears.
 */
export class CancellationRegistry {
  private readonly dir: string;

  constructor(storageDir?: string) {
    const base = storageDir || path.join(os.homedir(), '.agent-mesh-bridge');
    this.dir = path.join(base, CANCELLATION_DIR);
  }

  public request(
    threadId: string,
    action: CancellationAction,
    reason: string,
    by?: string,
  ): CancellationRequest {
    const request: CancellationRequest = {
      action,
      reason: reason || 'No reason provided',
      at: new Date().toISOString(),
      by,
    };
    try {
      fs.mkdirSync(this.dir, { recursive: true });
      fs.writeFileSync(this.filePath(threadId), JSON.stringify(request), 'utf8');
    } catch (err) {
      console.warn(`[CancellationRegistry] Failed to request ${action} for ${threadId}:`, err);
    }
    return request;
  }

  /** Read and remove the request (at-most-once consumption by the runner). */
  public consume(threadId: string): CancellationRequest | undefined {
    const file = this.filePath(threadId);
    try {
      const raw = fs.readFileSync(file, 'utf8');
      fs.unlinkSync(file);
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed.action === 'string' && typeof parsed.reason === 'string') {
        return parsed as CancellationRequest;
      }
    } catch {}
    return undefined;
  }

  /** Read without deleting. */
  public peek(threadId: string): CancellationRequest | undefined {
    try {
      const parsed = JSON.parse(fs.readFileSync(this.filePath(threadId), 'utf8'));
      if (parsed && typeof parsed.action === 'string' && typeof parsed.reason === 'string') {
        return parsed as CancellationRequest;
      }
    } catch {}
    return undefined;
  }

  public clear(threadId: string): void {
    try {
      fs.unlinkSync(this.filePath(threadId));
    } catch {}
  }

  private filePath(threadId: string): string {
    const safe = threadId.replace(/[^A-Za-z0-9_.-]/g, '_');
    return path.join(this.dir, `${safe}.json`);
  }
}
