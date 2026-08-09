import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// Dev-only convenience: `npm run dev` serves the UI from Vite's own dev
// server (with HMR), so requests to the same-origin "/api/..." paths this
// app's src/api.ts hits need to be forwarded somewhere -- trading_server on
// its default --http-port. `npm run build` output (dist/) has no such
// proxy and instead expects to be served *by* trading_server itself via
// --static-dir, at which point "/api/..." is already same-origin for
// real (see ui/README.md).
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:8080',
        changeOrigin: true,
      },
    },
  },
})
