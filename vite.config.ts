import { defineConfig } from 'vite'

// GitHub Pages needs a non-root base when deployed under /<repo>/
// In CI we set BASE_PATH=/<repo>/, locally it's '/' by default.
const base = process.env.BASE_PATH || '/'

export default defineConfig({
  base,
})
