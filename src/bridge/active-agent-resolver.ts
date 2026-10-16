import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { AgentId } from './types';
import { AgentDetector } from './agent-detector';

export class ActiveAgentResolver {
  private manualOverride: AgentId | null = null;
  private detector: AgentDetector;

  constructor(detector: AgentDetector) {
    this.detector = detector;
  }

  public setManualOverride(agent: AgentId | null): void {
    this.manualOverride = agent;
  }

  public getManualOverride(): AgentId | null {
    return this.manualOverride;
  }

  /**
   * Determine the active default agent based on priority heuristics:
   * 1. Manual user override (via status bar or config)
   * 2. Host environment detection (Antigravity IDE vs standard VSCode)
   * 3. Installed / Active extension markers
   * 4. LRU recent session activity (within last 5-10 minutes)
   * 5. First available agent fallback
   */
  public async resolveActiveAgent(): Promise<{
    agent: AgentId;
    mode: 'manual' | 'auto';
    reason: string;
  }> {
    if (this.manualOverride) {
      return { agent: this.manualOverride, mode: 'manual', reason: 'User manual override' };
    }

    // 1. Antigravity IDE detection
    if (
      process.env.ANTIGRAVITY_APP ||
      process.env.GEMINI_CLI ||
      (process.env.VSCODE_PORTABLE &&
        process.env.VSCODE_PORTABLE.toLowerCase().includes('antigravity'))
    ) {
      return {
        agent: 'antigravity',
        mode: 'auto',
        reason: 'Detected Antigravity IDE host environment',
      };
    }

    // 2. Recent session file mtime heuristic (5 minutes = 300,000 ms)
    const lruResult = this.detectMostRecentAgentSession();
    if (lruResult) {
      return {
        agent: lruResult.agent,
        mode: 'auto',
        reason: `Recent session activity (${lruResult.mtimeAgoSec}s ago)`,
      };
    }

    // 3. Fallback based on executable availability
    const agyAvail = !!this.detector.resolveExecutable('antigravity');
    const codexAvail = !!this.detector.resolveExecutable('codex');
    const claudeAvail = !!this.detector.resolveExecutable('claude_code');

    if (codexAvail) {
      return { agent: 'codex', mode: 'auto', reason: 'Detected OpenAI Codex executable' };
    }
    if (agyAvail) {
      return {
        agent: 'antigravity',
        mode: 'auto',
        reason: 'Detected Google Antigravity executable',
      };
    }
    if (claudeAvail) {
      return { agent: 'claude_code', mode: 'auto', reason: 'Detected Claude Code CLI executable' };
    }

    return { agent: 'codex', mode: 'auto', reason: 'Default fallback agent' };
  }

  /**
   * Scan native session directories to check which agent was used most recently.
   */
  private detectMostRecentAgentSession(): { agent: AgentId; mtimeAgoSec: number } | null {
    const home = os.homedir();
    const now = Date.now();
    const candidates: Array<{ agent: AgentId; latestTime: number }> = [];

    // Antigravity sessions/brain
    const geminiBrain = path.join(home, '.gemini', 'antigravity', 'brain');
    const geminiTime = this.getLatestMtimeInDir(geminiBrain);
    if (geminiTime > 0) {
      candidates.push({ agent: 'antigravity', latestTime: geminiTime });
    }

    // Claude sessions
    const claudeSessions = path.join(home, '.claude', 'sessions');
    const claudeTime = this.getLatestMtimeInDir(claudeSessions);
    if (claudeTime > 0) {
      candidates.push({ agent: 'claude_code', latestTime: claudeTime });
    }

    // Codex sessions
    const codexSessions = path.join(home, '.codex', 'sessions');
    const codexTime = this.getLatestMtimeInDir(codexSessions);
    if (codexTime > 0) {
      candidates.push({ agent: 'codex', latestTime: codexTime });
    }

    if (candidates.length === 0) return null;

    candidates.sort((a, b) => b.latestTime - a.latestTime);
    const top = candidates[0];
    const diffSec = Math.round((now - top.latestTime) / 1000);

    // Only count if within last 10 minutes (600s)
    if (diffSec < 600) {
      return { agent: top.agent, mtimeAgoSec: diffSec };
    }

    return null;
  }

  private getLatestMtimeInDir(dirPath: string): number {
    if (!fs.existsSync(dirPath)) return 0;
    try {
      const stat = fs.statSync(dirPath);
      let maxTime = stat.mtimeMs;
      const files = fs.readdirSync(dirPath);
      // Sample recent entries up to 20
      for (const file of files.slice(0, 20)) {
        try {
          const s = fs.statSync(path.join(dirPath, file));
          if (s.mtimeMs > maxTime) maxTime = s.mtimeMs;
        } catch {}
      }
      return maxTime;
    } catch {
      return 0;
    }
  }
}
