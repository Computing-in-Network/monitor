import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import { viteStaticCopy } from 'vite-plugin-static-copy';

export default defineConfig({
  plugins: [
    react(),
    viteStaticCopy({
      targets: [
        {
          src: 'node_modules/cesium/Build/Cesium/*',
          dest: 'cesium'
        }
      ]
    })
  ],
  define: {
    CESIUM_BASE_URL: JSON.stringify('/cesium')
  },
  server: {
    host: '0.0.0.0',
    port: 5173
  },
  build: {
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (!id.includes('node_modules')) {
            return undefined;
          }
          const seg = id.split('node_modules/')[1];
          if (!seg) {
            return 'vendor';
          }
          const pkg = seg.startsWith('@')
            ? seg.split('/').slice(0, 2).join('_')
            : seg.split('/')[0];
          if (pkg === 'cesium' || pkg.startsWith('@cesium')) {
            return 'vendor-cesium';
          }
          return 'vendor';
        }
      }
    }
  }
});
