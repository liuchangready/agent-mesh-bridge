import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'node:http';
import { AddressInfo } from 'node:net';
import { completeCompatibleChat, completeCompatibleTools, fetchCompatibleModels } from '../copilot/compatible-model-client';

async function withServer(
  handler: (request: http.IncomingMessage, response: http.ServerResponse) => void,
  run: (baseUrl: string) => Promise<void>,
): Promise<void> {
  const server = http.createServer(handler);
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  try {
    await run(`http://127.0.0.1:${(server.address() as AddressInfo).port}/prefix`);
  } finally {
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
  }
}

test('real HTTP client posts only the selected model and current prompt to the preserved path', async () => {
  await withServer(
    (request, response) => {
      assert.equal(request.method, 'POST');
      assert.equal(request.url, '/prefix/chat/completions');
      assert.equal(request.headers.authorization, 'Bearer local-test-key');
      let body = '';
      request.setEncoding('utf8');
      request.on('data', (part) => (body += part));
      request.on('end', () => {
        assert.deepEqual(JSON.parse(body), {
          model: 'model-x',
          stream: false,
          messages: [{ role: 'user', content: 'hello' }],
        });
        response.setHeader('content-type', 'application/json');
        response.end(JSON.stringify({ choices: [{ message: { content: 'answer' } }] }));
      });
    },
    async (baseUrl) => {
      assert.equal(
        await completeCompatibleChat({
          baseUrl,
          model: 'model-x',
          prompt: 'hello',
          apiKey: 'local-test-key',
        }),
        'answer',
      );
    },
  );
});

test('explicit model listing uses GET /models, deduplicates ids, and never follows redirects', async () => {
  await withServer(
    (request, response) => {
      assert.equal(request.method, 'GET');
      assert.equal(request.url, '/prefix/models');
      assert.equal(request.headers.authorization, 'Bearer local-test-key');
      response.end(JSON.stringify({ data: [{ id: 'm1' }, { id: 'm1' }, { id: 'm2' }] }));
    },
    async (baseUrl) => {
      assert.deepEqual(await fetchCompatibleModels({ baseUrl, apiKey: 'local-test-key' }), [
        'm1',
        'm2',
      ]);
    },
  );
  await withServer(
    (_request, response) => {
      response.writeHead(302, { location: 'http://127.0.0.1:1/leak' });
      response.end();
    },
    async (baseUrl) => {
      await assert.rejects(
        completeCompatibleChat({ baseUrl, model: 'm', prompt: 'x', apiKey: 'local-test-key' }),
        /redirected/,
      );
    },
  );
});

test('structured tool requests preserve native usage with cached input normalized as a subset', async () => {
  await withServer((_request, response) => {
    response.end(JSON.stringify({
      choices: [{ finish_reason: 'tool_calls', message: { content: null, tool_calls: [{ id: 'call-1', type: 'function', function: { name: 'read_file', arguments: '{"path":"README.md"}' } }] } }],
      usage: { prompt_tokens: 10, completion_tokens: 4, total_tokens: 14, prompt_tokens_details: { cached_tokens: 3 } },
    }));
  }, async (baseUrl) => {
    const result = await completeCompatibleTools({
      baseUrl, model: 'model-x', messages: [{ role: 'user', content: 'read' }],
      tools: [{ name: 'read_file', description: 'Read a file', inputSchema: { type: 'object' } }],
      apiKey: 'local-test-key', signal: new AbortController().signal,
    });
    assert.equal(result.toolCalls[0].arguments.path, 'README.md');
    assert.deepEqual(result.usage, {
      uncachedInputTokens: 7, cacheReadTokens: 3, cacheWriteTokens: 0,
      promptTokens: 10, outputTokens: 4, totalTokens: 14, nativeTotalTokens: 14,
    });
  });
});

test('double slashes in a base path never become an authority for either API route', async () => {
  const seen: Array<{ url: string; authorization: string | null }> = [];
  const fetchImpl: typeof fetch = async (input, init) => {
    const url = input instanceof URL ? input : new URL(String(input));
    seen.push({ url: url.toString(), authorization: new Headers(init?.headers).get('authorization') });
    return new Response(
      url.pathname.endsWith('/models')
        ? JSON.stringify({ data: [] })
        : JSON.stringify({ choices: [{ message: { content: 'ok' } }] }),
      { headers: { 'content-type': 'application/json' } },
    );
  };
  const baseUrl = 'https://trusted.example//other.example/v1';
  await fetchCompatibleModels({ baseUrl, apiKey: 'origin-secret', fetchImpl });
  await completeCompatibleChat({
    baseUrl,
    model: 'model-x',
    prompt: 'hello',
    apiKey: 'origin-secret',
    fetchImpl,
  });
  assert.deepEqual(
    seen.map(({ url, authorization }) => ({ url: new URL(url).href, origin: new URL(url).origin, authorization })),
    [
      {
        url: 'https://trusted.example//other.example/v1/models',
        origin: 'https://trusted.example',
        authorization: 'Bearer origin-secret',
      },
      {
        url: 'https://trusted.example//other.example/v1/chat/completions',
        origin: 'https://trusted.example',
        authorization: 'Bearer origin-secret',
      },
    ],
  );
});

test('HTTP failures, empty responses, and echoed secrets are sanitized', async () => {
  await withServer(
    (_request, response) => {
      response.writeHead(401);
      response.end('Bearer private-secret');
    },
    async (baseUrl) => {
      await assert.rejects(
        completeCompatibleChat({ baseUrl, model: 'm', prompt: 'x', apiKey: 'private-secret' }),
        /Authentication failed/,
      );
    },
  );
  await withServer(
    (_request, response) => {
      response.end(JSON.stringify({ choices: [{ message: { content: 'private-secret' } }] }));
    },
    async (baseUrl) => {
      assert.equal(
        await completeCompatibleChat({
          baseUrl,
          model: 'm',
          prompt: 'x',
          apiKey: 'private-secret',
        }),
        '[redacted]',
      );
    },
  );
  await withServer(
    (_request, response) => {
      response.end(JSON.stringify({ choices: [{ message: { content: '' } }] }));
    },
    async (baseUrl) => {
      await assert.rejects(
        completeCompatibleChat({ baseUrl, model: 'm', prompt: 'x', apiKey: 'k' }),
        /no text response/,
      );
    },
  );
});

test('client distinguishes rate limits, invalid JSON, oversized replies, timeout, and cancellation', async () => {
  await withServer(
    (_request, response) => {
      response.writeHead(429);
      response.end('private body');
    },
    async (baseUrl) => {
      await assert.rejects(
        completeCompatibleChat({ baseUrl, model: 'm', prompt: 'x', apiKey: 'k' }),
        /rate limited/,
      );
    },
  );
  await withServer(
    (_request, response) => {
      response.end('{not json');
    },
    async (baseUrl) => {
      await assert.rejects(
        completeCompatibleChat({ baseUrl, model: 'm', prompt: 'x', apiKey: 'k' }),
        /invalid JSON/,
      );
    },
  );
  await withServer(
    (_request, response) => {
      response.end(' '.repeat(2 * 1024 * 1024 + 1));
    },
    async (baseUrl) => {
      await assert.rejects(
        completeCompatibleChat({ baseUrl, model: 'm', prompt: 'x', apiKey: 'k' }),
        /exceeded 2 MB/,
      );
    },
  );
  await withServer(
    (_request, response) => {
      setTimeout(() => response.end('{}'), 250);
    },
    async (baseUrl) => {
      await assert.rejects(
        completeCompatibleChat({ baseUrl, model: 'm', prompt: 'x', apiKey: 'k', timeoutMs: 25 }),
        /timed out/,
      );
    },
  );
  await withServer(
    (_request, response) => {
      setTimeout(() => response.end('{}'), 250);
    },
    async (baseUrl) => {
      const aborter = new AbortController();
      const request = completeCompatibleChat({
        baseUrl,
        model: 'm',
        prompt: 'x',
        apiKey: 'k',
        signal: aborter.signal,
      });
      setTimeout(() => aborter.abort(), 20);
      await assert.rejects(request, /cancelled/);
    },
  );
});
