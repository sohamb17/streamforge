import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// base "./" makes the build work both at the console's root and under
// https://<user>.github.io/streamforge/.
export default defineConfig({
  base: "./",
  plugins: [react()],
  server: {
    proxy: { "/api": "http://localhost:8080" },
  },
});
