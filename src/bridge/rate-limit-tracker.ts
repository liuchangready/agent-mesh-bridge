import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { AgentId, RateLimitInfo } from './types';

export class RateLimitTracker {
  private static instance: RateLimitTracker | null = null;
  private filePath: string;
  private limits: Partial<Record<AgentId, RateLimitInfo>> = {};

  constructor(customStorageDir?: string) {
    const dir = customStorageDir || path.join(os.homedir(), '.agent-mesh-bridge');
    if (!fs.existsSync(dir)) {
      try {
        fs.mkdirSync(dir, { recursive: true });
      } catch {}
    }
    this.filePath = path.join(dir, 'rate_limits.json');
    this.load();
  }

  public static getInstance(customStorageDir?: string): RateLimitTracker {
    if (!RateLimitTracker.instance) {
      RateLimitTracker.instance = new RateLimitTracker(customStorageDir);
    }
    return RateLimitTracker.instance;
  }

  private load(): void {
    if (fs.existsSync(this.filePath)) {
      try {
        const raw = fs.readFileSync(this.filePath, 'utf8');
        this.limits = JSON.parse(raw) || {};
      } catch {
        this.limits = {};
      }
    }
  }

  private save(): void {
    try {
      fs.writeFileSync(this.filePath, JSON.stringify(this.limits, null, 2), 'utf8');
    } catch {}
  }

  /**
   * Retrieves active rate limit info for an agent, clearing it if recovery time has elapsed.
   */
  public getRateLimitInfo(agent: AgentId): RateLimitInfo | undefined {
    this.load();
    const info = this.limits[agent];
    if (!info) return undefined;

    if (info.resetsAt) {
      const resetTime = new Date(info.resetsAt).getTime();
      const now = Date.now();
      if (now >= resetTime) {
        // Limit has expired / agent recovered
        this.clearExhaustion(agent);
        return undefined;
      }

      const remainingSec = Math.max(0, Math.round((resetTime - now) / 1000));
      return {
        ...info,
        remainingSeconds: remainingSec,
        resetsAtFormatted: this.formatResetTime(resetTime, remainingSec)
      };
    }

    return info;
  }

  public isExhausted(agent: AgentId): boolean {
    const info = this.getRateLimitInfo(agent);
    return !!info && info.isExhausted;
  }

  /**
   * Clear exhaustion status manually.
   */
  public clearExhaustion(agent: AgentId): void {
    if (this.limits[agent]) {
      delete this.limits[agent];
      this.save();
    }
  }

  /**
   * Parse error text or explicitly record exhaustion with recovery timestamp.
   */
  public recordExhaustion(
    agent: AgentId,
    errorText: string,
    fallbackDurationMs: number = 60 * 60 * 1000 // 1 hour default
  ): RateLimitInfo {
    const now = Date.now();
    let durationMs = fallbackDurationMs;

    // Pattern 1: "retry after X seconds/minutes/hours"
    const retryAfterMatch = errorText.match(/retry\s+after\s+(\d+)\s*(s|sec|seconds?|m|min|minutes?|h|hours?)?/i);
    if (retryAfterMatch) {
      const val = parseInt(retryAfterMatch[1], 10);
      const unit = (retryAfterMatch[2] || 's').toLowerCase();
      if (unit.startsWith('h')) durationMs = val * 3600 * 1000;
      else if (unit.startsWith('m')) durationMs = val * 60 * 1000;
      else durationMs = val * 1000;
    }

    // Pattern 2: "resets in X seconds/minutes/hours" or "try again in X"
    const inMatch = errorText.match(/(?:resets\s+in|try\s+again\s+in)\s+(\d+)\s*(s|sec|seconds?|m|min|minutes?|h|hours?)?/i);
    if (inMatch) {
      const val = parseInt(inMatch[1], 10);
      const unit = (inMatch[2] || 'm').toLowerCase();
      if (unit.startsWith('h')) durationMs = val * 3600 * 1000;
      else if (unit.startsWith('m')) durationMs = val * 60 * 1000;
      else durationMs = val * 1000;
    }

    // Pattern 3: "resets at HH:MM (AM/PM)?"
    const atMatch = errorText.match(/resets\s+at\s+(\d{1,2}):(\d{2})(?::(\d{2}))?\s*([ap]m)?/i);
    if (atMatch) {
      let hours = parseInt(atMatch[1], 10);
      const minutes = parseInt(atMatch[2], 10);
      const ampm = atMatch[4]?.toLowerCase();
      if (ampm === 'pm' && hours < 12) hours += 12;
      if (ampm === 'am' && hours === 12) hours = 0;

      const targetDate = new Date();
      targetDate.setHours(hours, minutes, 0, 0);
      if (targetDate.getTime() <= now) {
        // Roll over to tomorrow
        targetDate.setDate(targetDate.getDate() + 1);
      }
      durationMs = targetDate.getTime() - now;
    }

    const resetTimestamp = now + durationMs;
    const remainingSec = Math.round(durationMs / 1000);
    const resetsAt = new Date(resetTimestamp).toISOString();

    const info: RateLimitInfo = {
      isExhausted: true,
      resetsAt,
      resetsAtFormatted: this.formatResetTime(resetTimestamp, remainingSec),
      remainingSeconds: remainingSec,
      reason: this.extractReason(errorText),
      lastUpdated: new Date().toISOString()
    };

    this.limits[agent] = info;
    this.save();
    return info;
  }

  private extractReason(text: string): string {
    const trimmed = text.replace(/[\r\n]+/g, ' ').trim();
    if (trimmed.includes('429')) return '429 Rate Limit Exceeded';
    if (/insufficient_quota|quota/i.test(trimmed)) return 'Quota Exhausted';
    if (/resource_exhausted/i.test(trimmed)) return 'Resource Exhausted';
    if (/overloaded/i.test(trimmed)) return 'Model Overloaded';
    return trimmed.slice(0, 100);
  }

  private formatResetTime(resetTimestamp: number, remainingSec: number): string {
    const date = new Date(resetTimestamp);
    const pad = (n: number) => String(n).padStart(2, '0');
    const timeStr = `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;

    let remainingStr = '';
    if (remainingSec >= 3600) {
      const hrs = Math.floor(remainingSec / 3600);
      const mins = Math.floor((remainingSec % 3600) / 60);
      remainingStr = `in ${hrs}h ${mins}m`;
    } else if (remainingSec >= 60) {
      const mins = Math.floor(remainingSec / 60);
      remainingStr = `in ${mins}m`;
    } else {
      remainingStr = `in ${remainingSec}s`;
    }

    return `${timeStr} (${remainingStr})`;
  }
}
