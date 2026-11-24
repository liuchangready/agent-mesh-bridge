const { execFileSync } = require('node:child_process');
const { resolve } = require('node:path');

if (process.platform === 'win32') {
  const root = resolve(__dirname, '..');
  const compiler = resolve(process.env.SystemRoot || 'C:\\Windows', 'Microsoft.NET/Framework64/v4.0.30319/csc.exe');
  execFileSync(compiler, [
    '/nologo', '/optimize+', '/target:exe', '/platform:anycpu',
    '/out:' + resolve(root, 'bin/agent-bridge-windows-job.exe'),
    resolve(root, 'src/bridge/windows-job-host.cs'),
  ], { stdio: 'inherit', windowsHide: true });
}
