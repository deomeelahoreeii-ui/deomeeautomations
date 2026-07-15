// @ts-check
import { defineConfig } from 'astro/config';

import tailwindcss from '@tailwindcss/vite';

import cloudflare from '@astrojs/cloudflare';

import clerk from '@clerk/astro';

// https://astro.build/config
export default defineConfig({
  output: 'server',

  integrations: [clerk()],

  vite: {
    plugins: [tailwindcss()],
    server: {
      watch: {
        ignored: [
          '**/.wrangler/**',
          '**/.astro/**',
          '**/dist/**',
          '**/data/*.generated.sql'
        ]
      }
    },
    optimizeDeps: {
      exclude: []
    }
  },

  adapter: cloudflare()
});
