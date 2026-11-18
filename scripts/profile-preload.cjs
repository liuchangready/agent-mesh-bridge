const fs = require('node:fs');
const path = require('node:path');
const cp = require('node:child_process');
const Module = require('node:module');

const logFile = process.env.BENCHMARK_PROFILE_LOG;

const counts = {
  git: 0,
  lock_helper: 0,
  taskstore_cas_attempt: 0,
  taskstore_cas_success: 0,
  taskstore_cas_failure: 0,
  taskstore_write_attempt: 0,
  taskstore_write_success: 0,
  taskstore_write_failure: 0,
  taskstore_cas: 0,
  taskstore_write: 0,
};

function record(category) {
  if (counts[category] !== undefined) counts[category]++;
}

process.on('exit', () => {
  if (!logFile) return;
  counts.taskstore_cas = counts.taskstore_cas_success;
  counts.taskstore_write = counts.taskstore_write_success;
  const total = counts.git + counts.lock_helper + counts.taskstore_cas_attempt + counts.taskstore_write_attempt;
  if (total > 0) {
    try {
      fs.appendFileSync(logFile, JSON.stringify(counts) + '\n', 'utf8');
    } catch {}
  }
});

// Hook child_process to count git commands and powershell lock helper spawns
const origSpawn = cp.spawn;
cp.spawn = function (command, args, ...rest) {
  const cmdStr = String(command).toLowerCase();
  if (cmdStr.includes('git')) {
    record('git');
  } else if (cmdStr.includes('powershell') || cmdStr.includes('pwsh')) {
    if (args && args.some(a => String(a).includes('Mutex') || String(a).includes('Global\\'))) {
      record('lock_helper');
    }
  }
  return origSpawn.call(this, command, args, ...rest);
};

const origSpawnSync = cp.spawnSync;
cp.spawnSync = function (command, args, ...rest) {
  const cmdStr = String(command).toLowerCase();
  if (cmdStr.includes('git')) {
    record('git');
  } else if (cmdStr.includes('powershell') || cmdStr.includes('pwsh')) {
    if (args && args.some(a => String(a).includes('Mutex') || String(a).includes('Global\\'))) {
      record('lock_helper');
    }
  }
  return origSpawnSync.call(this, command, args, ...rest);
};

const origExecFile = cp.execFile;
cp.execFile = function (command, args, ...rest) {
  const cmdStr = String(command).toLowerCase();
  if (cmdStr.includes('git')) {
    record('git');
  }
  return origExecFile.call(this, command, args, ...rest);
};

const origExecFileSync = cp.execFileSync;
cp.execFileSync = function (command, args, ...rest) {
  const cmdStr = String(command).toLowerCase();
  if (cmdStr.includes('git')) {
    record('git');
  }
  return origExecFileSync.call(this, command, args, ...rest);
};

// Hook Module._load to count TaskStore CAS and persist calls
const origLoad = Module._load;
Module._load = function (...args) {
  const mod = origLoad.apply(this, args);
  if (mod?.TaskStore?.prototype) {
    if (!mod.TaskStore.__profiled) {
      mod.TaskStore.__profiled = true;
      const origTrans = mod.TaskStore.prototype.transitionExecution;
      if (origTrans) {
        mod.TaskStore.prototype.transitionExecution = async function (...params) {
          record('taskstore_cas_attempt');
          try {
            const res = await origTrans.apply(this, params);
            if (res) record('taskstore_cas_success');
            return res;
          } catch (err) {
            record('taskstore_cas_failure');
            throw err;
          }
        };
      }
      const origPersist = mod.TaskStore.prototype.persistThread;
      if (origPersist) {
        mod.TaskStore.prototype.persistThread = function (...params) {
          record('taskstore_write_attempt');
          try {
            const res = origPersist.apply(this, params);
            record('taskstore_write_success');
            return res;
          } catch (err) {
            record('taskstore_write_failure');
            throw err;
          }
        };
      }
    }
  }
  if (mod?.MeshFileLock?.prototype) {
    if (!mod.MeshFileLock.__profiled) {
      mod.MeshFileLock.__profiled = true;
      const origStartHelper = mod.MeshFileLock.prototype.startLockHelper;
      if (origStartHelper) {
        mod.MeshFileLock.prototype.startLockHelper = function (...params) {
          record('lock_helper');
          return origStartHelper.apply(this, params);
        };
      }
    }
  }
  return mod;
};
