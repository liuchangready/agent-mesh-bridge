export type CompatibleClientOptions = {
  baseUrl: string;
  model: string;
  prompt: string;
  apiKey: string;
  signal?: AbortSignal;
  timeoutMs?: number;
  fetchImpl?: typeof fetch;
};

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
  const path = base.pathname.replace(/\/+$/, '');
  return new URL(`${path}/${suffix}`, base.origin);
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
