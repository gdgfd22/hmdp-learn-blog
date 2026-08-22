import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import { resolve } from 'node:path';

export default defineConfig({
  base: './',
  plugins: [react()],
  build: {
    outDir: '../doc/nginx-1.18.0/html/hmdp',
    emptyOutDir: false,
    rollupOptions: {
      input: {
        home: resolve(import.meta.dirname, 'index.html'),
        analytics: resolve(import.meta.dirname, 'analytics.html')
      }
    }
  }
});
