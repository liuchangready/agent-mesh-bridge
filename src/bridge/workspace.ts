import * as crypto from 'crypto';
import * as fs from 'fs';
import * as path from 'path';

export interface BridgeWorkspace {
  id: string;
  environmentId: string;
  rootPath: string;
  canonicalRootKey: string;
  name: string;
  createdAt: string;
  createdBy: 'user' | 'agent';
}

export interface TaskWorkspaceBinding {
  id: string;
  environmentId: string;
  rootPath: string;
  canonicalRootKey: string;
  nameAtDispatch: string;
  source: 'explicit' | 'window-default' | 'host-root';
  origin?: { hostInstanceId: string; generation: string };
  executionPath: string;
  dispatchAccepted?: boolean;
}

export interface WindowWorkspaceContext {
  hostInstanceId: string;
  generation: string;
  folderRoots: string[];
  defaultWorkspaceId?: string;
  defaultSource?: 'manual' | 'first-dispatch';
  expiresAt: number;
}

/** Window records are written only by their owning extension host. */
export class WindowWorkspace {
  readonly context: WindowWorkspaceContext;
  private readonly file: string;
  constructor(storageDir: string, roots: string[]) {
    const dir = path.join(storageDir, 'window-contexts');
    fs.mkdirSync(dir, { recursive: true });
    this.context = { hostInstanceId: crypto.randomUUID(), generation: crypto.randomUUID(),
      folderRoots: roots.map(physicalDirectory), expiresAt: 0 };
    this.file = path.join(dir, `${this.context.hostInstanceId}.json`);
    this.publish();
  }
  publish(): void {
    this.context.expiresAt = Date.now() + 30000;
    const temp = `${this.file}.${crypto.randomUUID()}.tmp`;
    fs.writeFileSync(temp, JSON.stringify(this.context));
    fs.renameSync(temp, this.file);
  }
  setDefault(id?: string, source: 'manual' | 'first-dispatch' = 'manual'): void {
    this.context.defaultWorkspaceId = id;
    this.context.defaultSource = id ? source : undefined;
    this.publish();
  }
  adopt(binding: TaskWorkspaceBinding): boolean {
    if (this.context.defaultWorkspaceId || !binding.dispatchAccepted ||
      binding.origin?.hostInstanceId !== this.context.hostInstanceId ||
      binding.origin.generation !== this.context.generation) return false;
    this.setDefault(binding.id, 'first-dispatch');
    return true;
  }
  dispose(): void { try { fs.unlinkSync(this.file); } catch {} }
}

export function matchingWindows(storageDir: string, root: string): WindowWorkspaceContext[] {
  const dir = path.join(storageDir, 'window-contexts');
  let files: string[];
  try { files = fs.readdirSync(dir); } catch { return []; }
  const matches: WindowWorkspaceContext[] = [];
  for (const file of files.filter((name) => /^[a-f0-9-]+\.json$/.test(name))) {
    try {
      const context: WindowWorkspaceContext = JSON.parse(fs.readFileSync(path.join(dir, file), 'utf8'));
      if (context.expiresAt > Date.now() && context.folderRoots.includes(root)) matches.push(context);
    } catch {}
  }
  return matches;
}

export function findUniqueWindow(storageDir: string, root: string): WindowWorkspaceContext | undefined {
  const matches = matchingWindows(storageDir, root);
  return matches.length === 1 ? matches[0] : undefined;
}

export function readWindow(storageDir: string, origin: { hostInstanceId: string; generation: string }): WindowWorkspaceContext | undefined {
  if (!/^[a-f0-9-]+$/.test(origin.hostInstanceId)) return undefined;
  try {
    const context: WindowWorkspaceContext = JSON.parse(fs.readFileSync(path.join(storageDir, 'window-contexts', `${origin.hostInstanceId}.json`), 'utf8'));
    return context.generation === origin.generation && context.expiresAt > Date.now() ? context : undefined;
  } catch { return undefined; }
}

/** Resolve aliases before comparing paths; do not fold case on case-sensitive volumes. */
export function physicalDirectory(input: string): string {
  if (!input || !path.isAbsolute(input) || /^(?:file|vscode-remote):/i.test(input)) {
    throw new Error('WORKSPACE_PATH_INVALID: an absolute local directory is required');
  }
  let real: string;
  try {
    real = fs.realpathSync.native(input);
    if (!fs.statSync(real).isDirectory()) throw new Error('not a directory');
    fs.accessSync(real, fs.constants.R_OK | fs.constants.X_OK);
  } catch (error) {
    throw new Error(`WORKSPACE_PATH_INVALID: ${input}: ${error}`);
  }
  return path.normalize(real);
}

/** Existing hard links are the same file even when their canonical names differ. */
export function samePhysicalFile(left: string, right: string): boolean {
  if (!path.isAbsolute(left) || !path.isAbsolute(right)) throw new Error('File scope paths must be absolute');
  const leftStat = fs.statSync(fs.realpathSync.native(left), { bigint: true });
  const rightStat = fs.statSync(fs.realpathSync.native(right), { bigint: true });
  if (leftStat.ino === 0n || rightStat.ino === 0n) throw new Error('File identity is unavailable');
  return leftStat.dev === rightStat.dev && leftStat.ino === rightStat.ino;
}

export function groupWorkspaceTasks<T extends { workspace: TaskWorkspaceBinding }>(tasks: T[]): T[][] {
  const groups = new Map<string, T[]>();
  for (const task of tasks) {
    const group = groups.get(task.workspace.id) || [];
    group.push(task);
    groups.set(task.workspace.id, group);
  }
  return [...groups.values()];
}

/** Publish a complete record exactly once, even when independent processes race. */
function createOnce(file: string, value: unknown): void {
  const temp = `${file}.${crypto.randomUUID()}.tmp`;
  fs.writeFileSync(temp, JSON.stringify(value), { flag: 'wx' });
  try {
    try { fs.linkSync(temp, file); } catch (error: any) { if (error.code !== 'EEXIST') throw error; }
  } finally { fs.unlinkSync(temp); }
}

export class WorkspaceRegistry {
  private readonly environmentId: string;
  private readonly directory: string;

  constructor(private readonly storageDir: string) {
    fs.mkdirSync(storageDir, { recursive: true });
    const file = path.join(storageDir, 'environment.json');
    createOnce(file, { id: crypto.randomUUID() });
    this.environmentId = JSON.parse(fs.readFileSync(file, 'utf8')).id;
    if (typeof this.environmentId !== 'string' || !this.environmentId) throw new Error('Invalid workspace environment');
    this.directory = path.join(storageDir, 'workspaces');
    fs.mkdirSync(this.directory, { recursive: true });
  }

  register(root: string, createdBy: 'user' | 'agent' = 'agent'): BridgeWorkspace {
    const canonicalRootKey = physicalDirectory(root);
    const id = 'ws_' + crypto.createHash('sha256').update(this.environmentId + '\0' + canonicalRootKey).digest('hex');
    const value: BridgeWorkspace = { id, environmentId: this.environmentId, rootPath: canonicalRootKey,
      canonicalRootKey, name: path.basename(canonicalRootKey) || canonicalRootKey, createdAt: new Date().toISOString(), createdBy };
    const file = path.join(this.directory, `${id}.json`);
    createOnce(file, value);
    return this.get(id)!;
  }

  get(id: string): BridgeWorkspace | undefined {
    if (!/^ws_[a-f0-9]{64}$/.test(id)) return undefined;
    try {
      const workspace: BridgeWorkspace = JSON.parse(fs.readFileSync(path.join(this.directory, `${id}.json`), 'utf8'));
      if (workspace.id !== id || workspace.environmentId !== this.environmentId ||
          typeof workspace.canonicalRootKey !== 'string' || workspace.rootPath !== workspace.canonicalRootKey ||
          'ws_' + crypto.createHash('sha256').update(this.environmentId + '\0' + workspace.canonicalRootKey).digest('hex') !== id) {
        throw new Error(`Invalid workspace record ${id}`);
      }
      return workspace;
    }
    catch (error: any) { if (error.code === 'ENOENT') return undefined; throw error; }
  }

  list(): BridgeWorkspace[] {
    return fs.readdirSync(this.directory).filter((name) => /^ws_[a-f0-9]{64}\.json$/.test(name))
      .map((name) => this.get(name.slice(0, -5))!).filter(Boolean);
  }
}

export class WorkspaceResolver {
  constructor(private readonly registry: WorkspaceRegistry) {}
  resolve(id?: string, root?: string): BridgeWorkspace {
    const explicit = root ? this.registry.register(root) : undefined;
    const selected = id ? this.registry.get(id) : explicit;
    if (!selected) throw new Error(id ? 'WORKSPACE_NOT_FOUND' : 'WORKSPACE_REQUIRED');
    if (explicit && selected.id !== explicit.id) throw new Error('WORKSPACE_MISMATCH');
    physicalDirectory(selected.rootPath);
    return selected;
  }
}
