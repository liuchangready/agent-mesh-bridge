export type CompatibleClientOptions = {
  baseUrl: string;
  model: string;
  prompt: string;
  apiKey: string;
  signal?: AbortSignal;
  timeoutMs?: number;
  fetchImpl?: typeof fetch;
};

export type CompatibleToolMessage = {
  role: 'user' | 'assistant' | 'tool';
  content: string;
  tool_calls?: Array<{ id: string; type: 'function'; function: { name: string; arguments: string } }>;
  tool_call_id?: string;
  name?: string;
};

export async function completeCompatibleTools(options: {
  baseUrl: string;
  model: string;
  messages: CompatibleToolMessage[];
  tools: Array<{ name: string; description: string; inputSchema: Record<string, unknown> }>;
  apiKey: string;
  signal: AbortSignal;
  fetchImpl?: typeof fetch;
}): Promise<{ text: string; toolCalls: Array<{ id: string; name: string; arguments: Record<string, unknown> }>; finishReason: 'stop' | 'tool_calls'; usage?: { uncachedInputTokens: number; cacheReadTokens: number; cacheWriteTokens: number; promptTokens: number; outputTokens: number; totalTokens: number; nativeTotalTokens?: number } }> {
  if (options.messages.length > 500 || options.tools.length > 16) throw new Error('Model tool request exceeds its limits.');
  const base = validateCompatibleBaseUrl(options.baseUrl);
  const controller = new AbortController();
  const abort = () => controller.abort(options.signal.reason);
  options.signal.addEventListener('abort', abort, { once: true });
  if (options.signal.aborted) abort();
  const timer = setTimeout(() => controller.abort(new Error('timeout')), 120_000);
  try {
    const response = await (options.fetchImpl || fetch)(endpoint(base, 'chat/completions'), {
      method: 'POST', redirect: 'manual', signal: controller.signal,
      headers: { Authorization: `Bearer ${options.apiKey}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: options.model,
        stream: false,
        messages: options.messages,
        tools: options.tools.map((tool) => ({ type: 'function', function: { name: tool.name, description: tool.description, parameters: tool.inputSchema } })),
        tool_choice: 'auto',
      }),
    });
    if (response.status >= 300 && response.status < 400) throw new Error('Endpoint redirected; update the configured URL.');
    const raw = await readBounded(response);
    if (!response.ok) throw new Error(response.status === 401 || response.status === 403 ? 'Authentication failed (HTTP 401/403).' : `Model request failed (HTTP ${response.status}).`);
    let parsed: any;
    try { parsed = JSON.parse(raw); } catch { throw new Error('Endpoint returned invalid JSON.'); }
    const choice = parsed?.choices?.[0];
    const message = choice?.message;
    const text = typeof message?.content === 'string' ? safeError(message.content, options.apiKey) : '';
    const rawCalls = message?.tool_calls;
    const toolCalls = rawCalls === undefined ? [] : Array.isArray(rawCalls) ? rawCalls.map((call: any) => {
      if (typeof call?.id !== 'string' || typeof call?.function?.name !== 'string' || typeof call?.function?.arguments !== 'string') throw new Error('Endpoint returned an invalid structured tool call.');
      let args: unknown;
      try { args = JSON.parse(call.function.arguments); } catch { throw new Error('Endpoint returned invalid tool arguments.'); }
      if (!args || typeof args !== 'object' || Array.isArray(args)) throw new Error('Endpoint tool arguments must be an object.');
      return { id: call.id, name: call.function.name, arguments: args as Record<string, unknown> };
    }) : (() => { throw new Error('Endpoint returned invalid tool calls.'); })();
    const reason = choice?.finish_reason;
    if (reason !== 'stop' && reason !== 'tool_calls') throw new Error('Endpoint returned an unsupported finish reason.');
    if ((reason === 'tool_calls') !== (toolCalls.length > 0)) throw new Error('Endpoint finish reason does not match its tool calls.');
    const nativeUsage = parsed?.usage;
    const promptTokens = Number(nativeUsage?.prompt_tokens);
    const outputTokens = Number(nativeUsage?.completion_tokens);
    if (Number.isSafeInteger(promptTokens) && promptTokens >= 0 && Number.isSafeInteger(outputTokens) && outputTokens >= 0) {
      const cacheReadTokens = Number.isSafeInteger(nativeUsage?.prompt_tokens_details?.cached_tokens)
        ? Math.min(promptTokens, Math.max(0, Number(nativeUsage.prompt_tokens_details.cached_tokens))) : 0;
      const totalTokens = promptTokens + outputTokens;
      return { text, toolCalls, finishReason: reason, usage: {
        uncachedInputTokens: promptTokens - cacheReadTokens, cacheReadTokens, cacheWriteTokens: 0,
        promptTokens, outputTokens, totalTokens,
        ...(Number.isSafeInteger(nativeUsage?.total_tokens) && nativeUsage.total_tokens >= 0 ? { nativeTotalTokens: nativeUsage.total_tokens } : {}),
      } };
    }
    return { text, toolCalls, finishReason: reason };
  } catch (error) {
    if (controller.signal.aborted) throw new Error(options.signal.aborted ? 'Request cancelled.' : 'Request timed out.');
    throw new Error(safeError(error instanceof Error ? error.message : 'Network request failed.', options.apiKey));
  } finally {
    clearTimeout(timer);
    options.signal.removeEventListener('abort', abort);
  }
}

const MAX_RESPONSE_BYTES = 2 * 1024 * 1024;

export function validateCompatibleBaseUrl(value: string): URL {
  let url: URL;
  try {
    url = new URL(value.trim());
  } catch {
    throw new Error('Enter a valid API base URL.');
  }
  const local = ['localhost', '127.0.0.1', '[::1]'].includes(url.hostname.toLowerCase());
  if (
    (url.protocol !== 'https:' && !(local && url.protocol === 'http:')) ||
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    url.href.includes('?') ||
    url.href.includes('#')
  ) {
    throw new Error(
      'Use HTTPS (HTTP is allowed only for localhost) without URL credentials, query, or fragment.',
    );
  }
  return url;
}

function endpoint(base: URL, suffix: string): URL {
  const url = new URL(base.origin);
  url.pathname = `${base.pathname.replace(/\/+$/, '')}/${suffix}`;
  if (url.origin !== base.origin) throw new Error('Endpoint URL changed origin unexpectedly.');
  return url;
}

function safeError(message: string, secret: string): string {
  return message.replaceAll(secret, '[redacted]');
}

async function readBounded(response: Response, limit = MAX_RESPONSE_BYTES): Promise<string> {
  const declared = Number(response.headers.get('content-length'));
  if (Number.isFinite(declared) && declared > limit)
    throw new Error(`Response exceeded ${limit / (1024 * 1024)} MB.`);
  if (!response.body) return '';
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > limit) {
        await reader.cancel();
        throw new Error(`Response exceeded ${limit / (1024 * 1024)} MB.`);
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  const bytes = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return new TextDecoder().decode(bytes);
}

export async function fetchCompatibleModels(options: {
  baseUrl: string;
  apiKey: string;
  signal?: AbortSignal;
  fetchImpl?: typeof fetch;
}): Promise<string[]> {
  const base = validateCompatibleBaseUrl(options.baseUrl);
  const controller = new AbortController();
  const abort = () => controller.abort(options.signal?.reason);
  options.signal?.addEventListener('abort', abort, { once: true });
  if (options.signal?.aborted) abort();
  const timer = setTimeout(() => controller.abort(new Error('timeout')), 10_000);
  try {
    const response = await (options.fetchImpl || fetch)(endpoint(base, 'models'), {
      headers: { Authorization: `Bearer ${options.apiKey}` },
      signal: controller.signal,
      redirect: 'manual',
    });
    if (response.status >= 300 && response.status < 400)
      throw new Error('Endpoint redirected; update the configured URL.');
    const raw = await readBounded(response, 1024 * 1024);
    if (!response.ok)
      throw new Error(
        response.status === 401 || response.status === 403
          ? 'Authentication failed (HTTP 401/403).'
          : `Model list request failed (HTTP ${response.status}).`,
      );
    let parsed: any;
    try {
      parsed = JSON.parse(raw);
    } catch {
      throw new Error('Endpoint returned invalid JSON.');
    }
    if (!Array.isArray(parsed?.data))
      throw new Error('Endpoint response did not contain a data array.');
    const ids: string[] = parsed.data.flatMap((model: any) =>
      typeof model?.id === 'string' &&
      model.id.trim() &&
      model.id.length <= 256 &&
      (!options.apiKey || !model.id.includes(options.apiKey))
        ? [model.id.trim()]
        : [],
    );
    return [...new Set(ids)].slice(0, 200);
  } catch (error) {
    if (controller.signal.aborted)
      throw new Error(
        options.signal?.aborted ? 'Request cancelled.' : 'Request timed out after 10 seconds.',
      );
    throw new Error(
      safeError(error instanceof Error ? error.message : 'Network request failed.', options.apiKey),
    );
  } finally {
    clearTimeout(timer);
    options.signal?.removeEventListener('abort', abort);
  }
}

export async function completeCompatibleChat(options: CompatibleClientOptions): Promise<string> {
  if (!options.prompt.trim()) throw new Error('Enter a prompt.');
  if (options.prompt.length > 32_000) throw new Error('Prompt exceeds 32,000 characters.');
  const base = validateCompatibleBaseUrl(options.baseUrl);
  const controller = new AbortController();
  const abort = () => controller.abort(options.signal?.reason);
  options.signal?.addEventListener('abort', abort, { once: true });
  if (options.signal?.aborted) abort();
  const timer = setTimeout(
    () => controller.abort(new Error('timeout')),
    options.timeoutMs || 120_000,
  );
  try {
    const response = await (options.fetchImpl || fetch)(endpoint(base, 'chat/completions'), {
      method: 'POST',
      redirect: 'manual',
      signal: controller.signal,
      headers: { Authorization: `Bearer ${options.apiKey}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: options.model,
        stream: false,
        messages: [{ role: 'user', content: options.prompt }],
      }),
    });
    if (response.status >= 300 && response.status < 400)
      throw new Error('Endpoint redirected; update the configured URL.');
    const raw = await readBounded(response);
    if (!response.ok) {
      if (response.status === 401 || response.status === 403)
        throw new Error('Authentication failed (HTTP 401/403).');
      if (response.status === 429)
        throw new Error('Endpoint rate limited this request (HTTP 429).');
      throw new Error(`Model request failed (HTTP ${response.status}).`);
    }
    let parsed: any;
    try {
      parsed = JSON.parse(raw);
    } catch {
      throw new Error('Endpoint returned invalid JSON.');
    }
    const content = parsed?.choices?.[0]?.message?.content;
    if (typeof content !== 'string' || !content.trim())
      throw new Error('Endpoint returned no text response.');
    return safeError(content, options.apiKey);
  } catch (error) {
    if (controller.signal.aborted)
      throw new Error(options.signal?.aborted ? 'Request cancelled.' : 'Request timed out.');
    throw new Error(
      safeError(error instanceof Error ? error.message : 'Network request failed.', options.apiKey),
    );
  } finally {
    clearTimeout(timer);
    options.signal?.removeEventListener('abort', abort);
  }
}
