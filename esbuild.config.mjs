import * as esbuild from 'esbuild';

const isWatch = process.argv.includes('--watch');

const ctx = await esbuild.context({
  entryPoints: ['src/extension.ts'],
  bundle: true,
  outfile: 'dist/extension.js',
  external: ['vscode'],
  format: 'cjs',
  platform: 'node',
  sourcemap: true,
  target: 'node18',
  logLevel: 'info',
});

if (isWatch) {
  await ctx.watch();
  console.log('Watching extension...');
} else {
  await ctx.rebuild();
  await ctx.dispose();
  console.log('Extension built successfully');
}

