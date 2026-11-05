# Bridge Copilot Custom Model Groups and Calls

> Date: 2026-11-06 (Asia/Shanghai)
> Status: design input for the implementation in this task.
> Baseline: `master` / `origin/master` at `8f697e9`. This is an addition to `COPILOT_VSCODE_AGENT_DESIGN.md`; it does not replace the requester-only task flow.

## 1. Goal and scope

Under the Bridge Copilot node, users can create named model groups, select a model, enter a prompt, and see a text result. Two sources are supported:

| Source                 | Discovery                                                                      | Request                                                    | Credentials                                                                |
| ---------------------- | ------------------------------------------------------------------------------ | ---------------------------------------------------------- | -------------------------------------------------------------------------- |
| VS Code host model     | Enumerate through public Language Model API only after the user clicks refresh | Call the selected model in the extension host              | Managed by VS Code/provider; Bridge never reads or copies provider secrets |
| Bridge custom endpoint | User enters model IDs or explicitly requests a model list                      | OpenAI-compatible Chat Completions from the extension host | User enters a key, stored only in VS Code `SecretStorage`                  |

This is a user initiated text request. It does not execute shell commands, edit the workspace, call Bridge control-plane tools, or create native Copilot conversations. Copilot remains requester-only. No model-group model is a mesh receiver or reviewer. Existing four-agent task execution, review, session, worktree, retry, token, archive, and MR contracts remain unchanged.

This release targets a trusted local desktop workspace. It does not change existing Copilot groups, accounts, CLI defaults, or old tasks. No model is chosen automatically and failures never silently switch model, source, or endpoint.

## 2. Baseline findings

- `agent-detector.ts` reports `vscode_chat` as offline, and the Copilot tree's status/auth children repeat that misleading state.
- `AgentNodesProvider` has no model-group hierarchy; `src/copilot/` only implements requester projection and notifications.
- The public host model API exposes identity fields such as `id` and `vendor`, but does not promise Copilot group names or exact membership.
- Previously observed local group names are evidence about one machine only; they must not become product defaults or prove a model call works.

## 3. User experience

Reuse the Agent Nodes tree, QuickPick, InputBox, and read-only text documents. Show requester configuration, host catalog state, unknown native Copilot login/quota, host models grouped by actual vendor, and user-created groups. Each group exposes its source, bound model count, and custom-endpoint key state (configured/missing), never key characters.

Users can add, rename, edit members, call, test, and delete groups. Endpoint groups also support changing the URL, setting/replacing a key, deleting the key, and explicitly fetching models. Command Palette commands provide the same actions as tree menus.

The first tree render uses cached state and performs no authorization prompt or network request. Only the refresh command enumerates host models. Model-change events invalidate the cache and prompt the user to refresh. Selecting a model only preselects it; it never sends automatically. A test command warns that the fixed short request may incur a charge and sends only after confirmation.

Calls show cancellable progress and open a read-only Markdown result document containing source, group, exact model ID, status, and duration. Returned text is displayed as inert content; it is not executed and links are not auto-opened. No workspace files, selection, chat history, or extra context are sent. Usage/cost is unknown unless a real provider reports it; do not invent mesh token metrics.

Show actionable distinctions for unavailable models, access denied, missing credentials, authentication failure, rate limit, timeout, cancellation, empty response, and incompatible protocol. Requester enablement, catalog visibility, API key presence, and last call result are independent states. Unknown login/quota remain unknown. `copilot.enabled=false` does not disable a user's independent model call.

## 4. Host models and imported group labels

Use public `vscode.lm.selectChatModels()` in response to a user action. Keep model objects and requests in the extension host. Persist only `{vendor,id}` references. Before calling, match both values exactly against the current enumeration; never parse opaque IDs or substitute by name. A changed or missing model is unavailable until the user refreshes and rebinds it.

Show the actual catalog grouped by returned vendor. Since the public API does not expose native group names, allow users to create their own host groups and bind exact enumerated model identities. Also allow explicit file selection of `chatLanguageModels.json`, parsing JSONC and projecting only top-level array entries' `name` and `vendor`. Imported entries create unbound groups; the user must bind real enumerated models. Never infer that all models for a vendor are members of the imported group. Duplicate imported names are offered individually and do not overwrite existing groups.

Limit import input to 1 MB; at most 50 groups and 200 models per group. Save only allowlisted fields. Do not persist the source file, settings, API keys, secret placeholders, or unrelated credentials. Do not scan VS Code databases, hardcode profile paths, or read another extension's SecretStorage.

## 5. Data and secret storage

Persist one versioned allowlisted group object in `context.globalState`; this is not Settings Sync and workspace configuration cannot override endpoint or secret references. Validate names, URLs, model IDs, counts, and sizes. Preserve unsupported-version data and report it rather than resetting it.

Store endpoint keys only in `context.secrets`, keyed by group identity and endpoint origin. The UI reports presence only. Cancelled input preserves the previous key. Renaming keeps the key. Changing origin drops the old association and requires a new key; never send the old secret to the new origin. Delete-group removes its key, and delete-key leaves the group. Report SecretStorage failures honestly. Cancel in-flight calls when a group is changed or deleted, and prevent stale results from reviving a deleted group.

Never put keys in settings, output, logs, diagnostics, ledger, bridge policy, MCP environment, CLI args, or webviews. Never read or copy credentials from the native `My OpenAI` provider. A custom endpoint has separate billing/auth from Copilot.

## 6. Endpoint protocol and request limits

Support only OpenAI-compatible Chat Completions. Label the protocol in UI. Model IDs may be typed without a model-list request. Default to HTTPS; allow HTTP only for `localhost`, `127.0.0.1`, or `[::1]`. Reject URL credentials, query, fragment, and other protocols. Append `/models` or `/chat/completions` to the provided base path without forcing another `/v1`. Use manual redirect handling and never forward Authorization to a new target.

Model-list discovery is explicit, has a 10-second timeout and 1 MB response limit, reads only `data[].id`, validates/deduplicates IDs, and returns at most 200. A failed list request does not prevent manual model entry.

For a call send `Authorization: Bearer …` and a body containing only the selected model, current user message, and required protocol fields. Use `stream:false`; one request only, with no automatic paid retries. Limit prompt to 32,000 characters, request to 120 seconds, and response body to 2 MB. Cancellation/timeout stops network work and prevents later result updates. Validate status, JSON, and a non-empty text `choices[0].message.content`; tool-only, reasoning-only, and metadata-only outputs are not success. Distinguish 401/403, 429, 5xx, network failures, empty output, timeout, and cancellation. Never dump remote body, HTML, cause, or request object. Redact the exact current secret from returned text and diagnostics. Do not persist prompts or outputs.

Do not add arbitrary headers, protocol probing, images, tool loops, native session restoration, or an endpoint-call tool exposed to models/MCP.

## 7. State and lifecycle

Use a Copilot-specific tree state rather than CLI missing-executable/offline and unauthenticated copy. Keep four CLI AgentInfo/status and active-agent routing compatible. Show requester enabled/reviewer, model-catalog state/count, per-group configuration completeness, and the last explicit call separately. Model discovery is not proof of login; a saved key is not proof of authentication; unknown quota and native selected model stay unknown.

`onDidChangeChatModels` invalidates the catalog. SecretStorage and group changes refresh tree state. Dispose subscriptions and cancel requests on extension shutdown. Do not use mesh locks or affect other tasks. Only allow new model network calls and key management in a trusted local desktop workspace; unsupported environments retain stored definitions and show a clear explanation.

## 8. Implementation boundaries

Suggested responsibilities are group validation/storage and import parsing under `src/copilot/`, a small endpoint client, a model service, QuickPick/InputBox commands, the Copilot branch in `src/ui/agent-nodes-provider.ts`, extension composition in `src/extension.ts`, and package command/menu contributions plus tests. Keep implementation aligned to existing layers; do not move everything into `extension.ts` or add unnecessary abstraction/dependencies.

Do not write `model-config.json`, add Copilot to `EXECUTABLE_AGENTS`, expose autonomous runner tools, or weaken requester-only MCP restrictions. Do not register a Chat Participant or publish Bridge groups into the native Copilot picker.

## 9. Acceptance and verification

1. Add, rename, edit members, and delete either source; verify stored non-secret definitions and credential presence survive reload, while cancelled setup does not leave partial configuration.
2. Exercise host models with multiple vendors, same names/different IDs, empty list, denied access, model-change invalidation, exact binding, and removal without drift.
3. Exercise JSONC strings containing comment-like text, trailing commas, malformed structure, size/count limits, duplicates, and secret fields; persist only name/vendor.
4. Exercise SecretStorage create/replace/delete/failure/cancel/rename/origin change. Verify the key never appears in persistent public data, UI result, logs, or errors, including server echo.
5. Use a local HTTP fake server for method/path/model/auth, manual model IDs, list discovery, redirect refusal, 401/403, 429, 5xx, invalid JSON, empty/tool-only result, oversized body, cancellation, timeout, and secret echo. No real user key in tests.
6. Exercise the real command/service/client path for user-triggered enumeration and calls, exact selection, prompt cancel, cancellable call, result display, and failure after partial output. Result content stays read-only/inert.
7. Copilot parent/status/auth children no longer infer offline/login from CLI probes; requester enablement remains independent; four other Agent nodes do not regress.
8. Compile, run new and existing Copilot requester/MCP/agent detector/active resolver/model catalog regressions, and check `git diff --check`.
9. Inspect packaging for missing runtime files or devDependency-only imports; do not evaluate configuration text.
10. Real GUI/provider/chargeable endpoint checks require an available GUI and explicit user action; report untested items honestly and do not use private APIs, existing secrets, or automatic paid calls.

The dedicated Codex reviewer checks these paths before approval. The receiver does not commit, push, create an MR, forward work, or run its own review loop. The caller integrates the approved batch and records the MR.

## 10. Official API references

- [Language Model API](https://code.visualstudio.com/api/extension-guides/ai/language-model)
- [VS Code API reference](https://code.visualstudio.com/api/references/vscode-api)
- [Language model configuration](https://code.visualstudio.com/docs/agent-customization/language-models)

Implement against the repository's actual engine and API baseline. Do not raise the VS Code engine without a necessary compatibility decision.
