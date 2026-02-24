/**
 * Run E2E integration test (UI → CLI → live env). When required env vars are missing
 * and stdin is a TTY, prompts the user interactively and then runs Jest with those vars set.
 *
 * If you set the env vars (e.g. export SCHEMAX_RUN_LIVE_COMMAND_TESTS=1, or use a .env file
 * loaded by your shell), prompts are skipped and the test runs with those values.
 *
 * Required for live run: SCHEMAX_RUN_LIVE_COMMAND_TESTS=1, and either
 * (SCHEMAX_E2E_PROFILE or DATABRICKS_CONFIG_PROFILE) and
 * (SCHEMAX_E2E_WAREHOUSE_ID or DATABRICKS_WAREHOUSE_ID).
 *
 * Usage: node scripts/prompt-e2e-env.js
 * Or: npm run test:integration
 */

const readline = require('readline');
const { spawn } = require('child_process');
const path = require('path');

const envVars = [
  {
    key: 'SCHEMAX_RUN_LIVE_COMMAND_TESTS',
    prompt: 'Enable live E2E (run schemax apply against Databricks)? Set to 1 to enable (Enter = skip live): ',
    default: '',
  },
  {
    key: 'SCHEMAX_E2E_PROFILE',
    altKey: 'DATABRICKS_CONFIG_PROFILE',
    prompt: 'Databricks profile (e.g. DEFAULT): ',
    default: '',
  },
  {
    key: 'SCHEMAX_E2E_WAREHOUSE_ID',
    altKey: 'DATABRICKS_WAREHOUSE_ID',
    prompt: 'SQL warehouse ID: ',
    default: '',
  },
];

function getEnv(key, altKey) {
  return process.env[key] || (altKey && process.env[altKey]) || '';
}

function ask(rl, prompt, defaultValue) {
  const suffix = defaultValue ? ` (default: ${defaultValue})` : '';
  return new Promise((resolve) => {
    rl.question(prompt + suffix + ' ', (answer) => {
      const trimmed = typeof answer === 'string' ? answer.trim() : '';
      resolve(trimmed !== '' ? trimmed : defaultValue || '');
    });
  });
}

async function main() {
  const isTTY = process.stdin.isTTY;
  const env = { ...process.env };

  if (isTTY) {
    const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
    for (const { key, altKey, prompt, default: def } of envVars) {
      const current = getEnv(key, altKey);
      if (current) continue; /* already set: skip prompt, use existing env */
      const value = await ask(rl, prompt, def);
      if (value) env[key] = value;
    }
    rl.close();
  }

  const rootDir = path.resolve(__dirname, '..');
  const jestArgs = ['jest', '--testPathPattern=App.e2e-ui-to-live', '--runInBand'];
  const child = spawn('npx', jestArgs, {
    cwd: rootDir,
    stdio: 'inherit',
    env,
    shell: true,
  });
  child.on('close', (code) => process.exit(code ?? 0));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
