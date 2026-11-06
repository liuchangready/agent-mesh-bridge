/**
 * User-consent gate for pinning a receiver model.
 *
 * Two rules the bridge enforces here, in this order:
 *
 *  1. **Default first.** A caller may not invent a receiver model on its own.
 *     Unless the caller declares the request came from the user
 *     (`modelRequestedByUser: true`), a `model` that differs from the agent's
 *     configured default is ignored and the configured default is used.
 *  2. **Switching needs a click.** When the caller does declare a user request,
 *     the user still confirms the switch in VS Code. Only an explicit approval
 *     lets the non-default model run; "use default" falls back, and a dismissal
 *     or a timeout fails the dispatch closed.
 *
 * The confirmation is a cross-process handshake, because the caller reaches the
 * bridge through its own MCP server process, which has no VS Code APIs:
 *
 *   MCP process                                VS Code extension host
 *   ───────────                                ──────────────────────
 *   writes <bridge-home>/model-approvals/<id>.request.json
 *   polls for <id>.decision.json          ⟵    polls the directory, shows a modal
 *                                              dialog, writes <id>.decision.json
 *
 * Both files are removed after the decision is logged to
 * `<bridge-home>/model-approvals.log`, so the channel never accumulates state.
 * If the request cannot even be written (no VS Code, read-only home), the model
 * falls back to the configured default instead of blocking the task.
 */

import * as crypto from 'crypto';
import * as fs from 'fs';
import * as path from 'path';
import { AgentId, ModelApprovalRecord, ModelApprovalStatus } from './types';
import {
  getBridgeHome,
  loadModelApprovalRequired,
  normalizeModelId,
  resolveConfiguredModel,
} from './model-catalog';

export type { ModelApprovalRecord, ModelApprovalStatus } from './types';

export interface ModelApprovalRequest {
  id: string;
  targetAgent: AgentId;
  /** Model the caller wants the receiver to run with. */
  requestedModel: string;
  /** The agent's configured default, i.e. what runs unless this is approved. */
  defaultModel?: string;
  initiatorAgent?: AgentId;
  /** Task title / prompt snippet, so the dialog says what this switch is for. */
  title?: string;
  at: string;
  /** After this instant the request is stale and must not be shown anymore. */
  expiresAt: string;
}

export interface ModelApprovalDecision {
  id: string;
  status: ModelApprovalStatus;
  at: string;
  by?: string;
  note?: string;
  /**
   * The request never reached a user (no writable bridge home, VS Code closed
   * with no extension host). Treated as "keep the default" rather than as a
   * refusal, because nobody was actually asked.
   */
  channelError?: boolean;
}

/** How a caller-supplied request is answered; injectable for tests. */
export type ModelApprovalHandler = (request: ModelApprovalRequest) => Promise<ModelApprovalDecision>;

export const MODEL_APPROVAL_DIR = 'model-approvals';
export const MODEL_APPROVAL_LOG = 'model-approvals.log';
export const DEFAULT_MODEL_APPROVAL_TIMEOUT_MS = 180_000;
export const DEFAULT_MODEL_APPROVAL_POLL_MS = 250;

/** Error codes surfaced to the caller through the MCP tool error. */
export const MODEL_APPROVAL_DENIED_CODE = 'MODEL_APPROVAL_DENIED';
export const MODEL_APPROVAL_TIMEOUT_CODE = 'MODEL_APPROVAL_TIMEOUT';

export function getModelApprovalDir(dir: string = getBridgeHome()): string {
  return path.join(dir, MODEL_APPROVAL_DIR);
}

function safeId(id: string): string {
  return String(id || '').replace(/[^A-Za-z0-9_.-]/g, '_');
}

function requestPath(id: string, dir: string): string {
  return path.join(getModelApprovalDir(dir), `${safeId(id)}.request.json`);
}

function decisionPath(id: string, dir: string): string {
  return path.join(getModelApprovalDir(dir), `${safeId(id)}.decision.json`);
}

function readJson<T>(file: string): T | undefined {
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8')) as T;
  } catch {
    return undefined;
  }
}

function isStatus(value: unknown): value is ModelApprovalStatus {
  return (
    value === 'approved' || value === 'use_default' || value === 'cancelled' || value === 'timeout'
  );
}

/** Publish a request for the extension host to pick up. */
export function writeModelApprovalRequest(
  request: ModelApprovalRequest,
  dir: string = getBridgeHome(),
): boolean {
  try {
    fs.mkdirSync(getModelApprovalDir(dir), { recursive: true });
    fs.writeFileSync(requestPath(request.id, dir), JSON.stringify(request), 'utf8');
    return true;
  } catch (e: any) {
    console.warn(`[ModelApproval] Could not publish request ${request.id}: ${e?.message || e}`);
    return false;
  }
}

export function readModelApprovalRequest(
  id: string,
  dir: string = getBridgeHome(),
): ModelApprovalRequest | undefined {
  const parsed = readJson<ModelApprovalRequest>(requestPath(id, dir));
  return parsed && typeof parsed.id === 'string' ? parsed : undefined;
}

export function readModelApprovalDecision(
  id: string,
  dir: string = getBridgeHome(),
): ModelApprovalDecision | undefined {
  const parsed = readJson<ModelApprovalDecision>(decisionPath(id, dir));
  if (!parsed || parsed.id !== id || !isStatus(parsed.status)) return undefined;
  return parsed;
}

/**
 * Requests waiting for an answer: a request file exists, no decision file does,
 * and it has not expired. `now` is injectable so staleness is testable.
 */
export function listPendingModelApprovals(
  dir: string = getBridgeHome(),
  now: number = Date.now(),
): ModelApprovalRequest[] {
  const pending: ModelApprovalRequest[] = [];
  let names: string[];
  try {
    names = fs.readdirSync(getModelApprovalDir(dir));
  } catch {
    return pending;
  }

  for (const name of names) {
    if (!name.endsWith('.request.json')) continue;
    const file = path.join(getModelApprovalDir(dir), name);
    const parsed = readJson<ModelApprovalRequest>(file);
    if (!parsed || typeof parsed.id !== 'string' || !parsed.requestedModel) continue;
    if (readModelApprovalDecision(parsed.id, dir)) continue;
    const expiresAt = parsed.expiresAt ? new Date(parsed.expiresAt).getTime() : NaN;
    if (Number.isFinite(expiresAt) && expiresAt <= now) {
      // Stale request: nobody can answer it anymore, so drop the file instead of
      // letting the directory grow forever.
      try {
        fs.unlinkSync(file);
      } catch {}
      continue;
    }
    pending.push(parsed);
  }

  pending.sort((a, b) => new Date(a.at).getTime() - new Date(b.at).getTime());
  return pending;
}

/** Answer a pending request (called by the extension host after the dialog). */
export function submitModelApprovalDecision(
  decision: ModelApprovalDecision,
  dir: string = getBridgeHome(),
): boolean {
  if (!isStatus(decision.status)) return false;
  try {
    fs.mkdirSync(getModelApprovalDir(dir), { recursive: true });
    fs.writeFileSync(decisionPath(decision.id, dir), JSON.stringify(decision), 'utf8');
    appendModelApprovalLog({ event: 'decision', ...decision }, dir);
    return true;
  } catch (e: any) {
    console.warn(`[ModelApproval] Could not record decision for ${decision.id}: ${e?.message || e}`);
    return false;
  }
}

/** Append one audit line; failures are never fatal. */
export function appendModelApprovalLog(
  entry: Record<string, unknown>,
  dir: string = getBridgeHome(),
): void {
  try {
    fs.mkdirSync(dir, { recursive: true });
    fs.appendFileSync(
      path.join(dir, MODEL_APPROVAL_LOG),
      `${JSON.stringify({ at: new Date().toISOString(), ...entry })}\n`,
      'utf8',
    );
  } catch {}
}

function clearModelApprovalFiles(id: string, dir: string): void {
  for (const file of [requestPath(id, dir), decisionPath(id, dir)]) {
    try {
      fs.unlinkSync(file);
    } catch {}
  }
}

/** Poll for a decision until it appears or `timeoutMs` elapses. */
export async function waitForModelApproval(
  id: string,
  options: { dir?: string; timeoutMs?: number; pollMs?: number } = {},
): Promise<ModelApprovalDecision> {
  const dir = options.dir ?? getBridgeHome();
  const timeoutMs = options.timeoutMs ?? DEFAULT_MODEL_APPROVAL_TIMEOUT_MS;
  const pollMs = options.pollMs ?? DEFAULT_MODEL_APPROVAL_POLL_MS;
  const deadline = Date.now() + timeoutMs;

  for (;;) {
    const decision = readModelApprovalDecision(id, dir);
    if (decision) return decision;
    if (Date.now() >= deadline) {
      return { id, status: 'timeout', at: new Date().toISOString(), by: 'bridge' };
    }
    await new Promise((resolve) => setTimeout(resolve, pollMs));
  }
}

/** Build the default request id: one approval per dispatch attempt. */
export function newModelApprovalId(): string {
  return `model-${crypto.randomUUID()}`;
}

/**
 * File-channel handler: publish the request, wait for the extension host's
 * answer, log it and clean up. Always resolves — an unanswered request is a
 * timeout, which the resolver turns into a fail-closed dispatch error.
 */
export async function requestModelApproval(
  request: ModelApprovalRequest,
  options: { dir?: string; timeoutMs?: number; pollMs?: number } = {},
): Promise<ModelApprovalDecision> {
  const dir = options.dir ?? getBridgeHome();
  appendModelApprovalLog({ event: 'request', ...request }, dir);

  if (!writeModelApprovalRequest(request, dir)) {
    clearModelApprovalFiles(request.id, dir);
    return {
      id: request.id,
      status: 'timeout',
      at: new Date().toISOString(),
      by: 'bridge',
      note: 'The approval channel is unavailable (no writable bridge home).',
      channelError: true,
    };
  }

  const decision = await waitForModelApproval(request.id, { ...options, dir });
  appendModelApprovalLog({ event: 'outcome', targetAgent: request.targetAgent, ...decision }, dir);
  clearModelApprovalFiles(request.id, dir);
  return decision;
}

export interface ResolveDispatchModelOptions {
  targetAgent: AgentId;
  /** Model the caller asked for; omitted means "use the configured default". */
  requestedModel?: string | null;
  /**
   * Whether the caller declares the user themselves named this model. Defaults
   * to `false`, so an agent guessing a model can never silently switch away from
   * the configured default.
   */
  requestedByUser?: boolean;
  initiatorAgent?: AgentId;
  title?: string;
  /** Bridge home; defaults to `~/.agent-mesh-bridge`. */
  dir?: string;
  /** Override the mirrored `requireModelApproval` policy (tests). */
  requireApproval?: boolean;
  timeoutMs?: number;
  pollMs?: number;
  /** Answering strategy; defaults to the file channel above. */
  handler?: ModelApprovalHandler;
  requestId?: string;
  now?: () => Date;
}

export interface ResolvedDispatchModel {
  /** The model the receiver will run with (`undefined` = the agent's own default). */
  model?: string;
  defaultModel?: string;
  /** Caller-facing explanation when the requested model was not used. */
  notice?: string;
  /** Set when a user decision was actually requested. */
  approval?: ModelApprovalRecord;
}

/** Thrown when the user declined a model switch, or answered nothing in time. */
export class ModelApprovalError extends Error {
  public readonly code: string;
  constructor(code: string, message: string) {
    super(message);
    this.name = 'ModelApprovalError';
    this.code = code;
  }
}

function defaultTimeoutMs(): number {
  const raw = Number(process.env.MESH_MODEL_APPROVAL_TIMEOUT_MS);
  return Number.isFinite(raw) && raw > 0 ? raw : DEFAULT_MODEL_APPROVAL_TIMEOUT_MS;
}

/**
 * Decide which model a new task runs with.
 *
 *   no request                  -> configured default
 *   request == default          -> configured default
 *   request != default, guessed -> configured default + notice
 *   request != default, declared, policy off -> requested model
 *   request != default, declared -> ask the user:
 *       approved     -> requested model
 *       use_default  -> configured default + notice
 *       cancelled    -> ModelApprovalError(MODEL_APPROVAL_DENIED)
 *       timeout      -> ModelApprovalError(MODEL_APPROVAL_TIMEOUT)
 */
export async function resolveDispatchModel(
  options: ResolveDispatchModelOptions,
): Promise<ResolvedDispatchModel> {
  const dir = options.dir ?? getBridgeHome();
  const requested = normalizeModelId(options.requestedModel);
  const configured = resolveConfiguredModel(options.targetAgent, dir);

  if (!requested) return { model: configured, defaultModel: configured };
  if (configured && requested === configured) {
    return { model: configured, defaultModel: configured };
  }

  const described = configured ?? '(agent default)';
  if (options.requestedByUser !== true) {
    return {
      model: configured,
      defaultModel: configured,
      notice:
        `Requested model ${requested} was ignored: it is not the configured default (${described}) ` +
        `and the caller did not declare it as an explicit user request. Using the configured default. ` +
        `Pass modelRequestedByUser: true only when the user named this model themselves; ` +
        `the bridge then asks the user to confirm the switch in VS Code.`,
    };
  }

  const requireApproval = options.requireApproval ?? loadModelApprovalRequired(dir);
  if (!requireApproval) {
    return { model: requested, defaultModel: configured };
  }

  const now = options.now ?? (() => new Date());
  const requestId = options.requestId ?? newModelApprovalId();
  const timeoutMs = options.timeoutMs ?? defaultTimeoutMs();
  const at = now();
  const request: ModelApprovalRequest = {
    id: requestId,
    targetAgent: options.targetAgent,
    requestedModel: requested,
    defaultModel: configured,
    initiatorAgent: options.initiatorAgent,
    title: options.title,
    at: at.toISOString(),
    expiresAt: new Date(at.getTime() + timeoutMs).toISOString(),
  };

  // The default handler owns the cross-process channel (and logs the request);
  // an injected handler is the test/extension double for the same decision.
  const decision: ModelApprovalDecision = options.handler
    ? await options.handler(request)
    : await requestModelApproval(request, { dir, timeoutMs, pollMs: options.pollMs });

  const record: ModelApprovalRecord = {
    id: requestId,
    status: decision.status,
    requestedModel: requested,
    defaultModel: configured,
    at: decision.at || new Date().toISOString(),
  };

  switch (decision.status) {
    case 'approved':
      appendModelApprovalLog({ event: 'outcome', ...record }, dir);
      return { model: requested, defaultModel: configured, approval: record };
    case 'use_default':
      appendModelApprovalLog({ event: 'outcome', ...record }, dir);
      return {
        model: configured,
        defaultModel: configured,
        approval: record,
        notice:
          `The user kept the default model for ${options.targetAgent}: requested ${requested} was not applied ` +
          `(configured default: ${described}).`,
      };
    case 'cancelled':
      appendModelApprovalLog({ event: 'outcome', ...record }, dir);
      throw new ModelApprovalError(
        MODEL_APPROVAL_DENIED_CODE,
        `The user declined the model switch for ${options.targetAgent} (${described} -> ${requested}). ` +
          `Re-dispatch without \`model\` (or with the configured default) to run on the default model.`,
      );
    case 'timeout':
    default:
      appendModelApprovalLog({ event: 'outcome', ...record }, dir);
      if (decision.channelError) {
        // Nobody could be asked, so nothing was declined: keep the default and
        // let the task run rather than blocking it on an unavailable dialog.
        return {
          model: configured,
          defaultModel: configured,
          approval: record,
          notice:
            `The model switch for ${options.targetAgent} (${described} -> ${requested}) could not be confirmed ` +
            `because the confirmation channel is unavailable; the task runs on the configured default ` +
            `(${described}). Start VS Code with the Agent Mesh Bridge extension to confirm switches.`,
        };
      }
      throw new ModelApprovalError(
        MODEL_APPROVAL_TIMEOUT_CODE,
        `The model switch for ${options.targetAgent} (${described} -> ${requested}) was not confirmed within ` +
          `${Math.round(timeoutMs / 1000)}s. Keep the VS Code window (Agent Mesh Bridge) open and answer the ` +
          `confirmation dialog, or re-dispatch without \`model\` to use the configured default.`,
      );
  }
}
