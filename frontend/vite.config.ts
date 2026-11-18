import { readFileSync } from 'node:fs';
import { defineConfig, loadEnv } from 'vite';
import react from '@vitejs/plugin-react';

const pkg = JSON.parse(readFileSync(new URL('./package.json', import.meta.url), 'utf8')) as { version: string };

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '');
  const devApiTarget = env.VITE_DEV_API_TARGET || 'http://127.0.0.1:8000';
  return {
    plugins: [react()],
    define: {
      __APP_VERSION__: JSON.stringify(pkg.version),
    },
    base: env.VITE_BASE_PATH || '/',
    server: {
      host: '127.0.0.1',
      port: 4173,
      proxy: {
        '/v1': devApiTarget,
        '/health': devApiTarget,
      },
    },
    preview: {
      host: '127.0.0.1',
      port: 4173,
    },
  };
});
