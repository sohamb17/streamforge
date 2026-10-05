// Where snapshots come from: the live cluster (Server-Sent Events from the
// console) or the in-browser simulation (the real Go code compiled to
// WebAssembly). The UI does not care which.

import type { Snapshot } from "./types";

export type Mode = "live" | "sim";

export interface Source {
  mode: Mode;
  liveBase?: string;
  chaos(action: string): Promise<string>; // "" = ok, otherwise a reason
  setSpeed?(x: number): void;
  close(): void;
}

declare global {
  interface Window {
    Go: any;
    streamforgeSim?: {
      advance(ms: number): void;
      snapshot(): string;
      chaos(action: string): string;
      setSpeed(x: number): void;
    };
  }
}

async function configLiveUrl(): Promise<string> {
  // Same origin first (the console serves this page), then config.json
  // (the GitHub Pages copy points at the live VM, if one is running).
  try {
    const r = await fetch("config.json", { cache: "no-store" });
    if (r.ok) {
      const c = await r.json();
      if (c.liveUrl) return String(c.liveUrl).replace(/\/?$/, "/");
    }
  } catch {
    /* no config */
  }
  return "";
}

function tryLive(base: string, onSnap: (s: Snapshot) => void, timeoutMs: number): Promise<EventSource | null> {
  return new Promise((resolve) => {
    let es: EventSource;
    try {
      es = new EventSource(base + "api/stream");
    } catch {
      resolve(null);
      return;
    }
    let settled = false;
    const timer = setTimeout(() => {
      if (!settled) {
        settled = true;
        es.close();
        resolve(null);
      }
    }, timeoutMs);
    es.onmessage = (ev) => {
      try {
        const s = JSON.parse(ev.data) as Snapshot;
        onSnap(s);
        if (!settled) {
          settled = true;
          clearTimeout(timer);
          resolve(es);
        }
      } catch {
        /* ignore malformed frame */
      }
    };
    es.onerror = () => {
      if (!settled) {
        settled = true;
        clearTimeout(timer);
        es.close();
        resolve(null);
      }
    };
  });
}

export async function connectLive(onSnap: (s: Snapshot) => void): Promise<Source | null> {
  const candidates = [""];
  const cfg = await configLiveUrl();
  if (cfg) candidates.push(cfg);
  for (const base of candidates) {
    const es = await tryLive(base, onSnap, 3500);
    if (es) {
      return {
        mode: "live",
        liveBase: base,
        async chaos(action: string) {
          try {
            const r = await fetch(base + "api/chaos/" + action, { method: "POST" });
            if (r.ok) return "";
            const j = await r.json().catch(() => ({}));
            return j.error || `request failed (${r.status})`;
          } catch (e) {
            return String(e);
          }
        },
        close: () => es.close(),
      };
    }
  }
  return null;
}

let wasmLoaded: Promise<void> | null = null;

function loadScript(src: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const s = document.createElement("script");
    s.src = src;
    s.onload = () => resolve();
    s.onerror = () => reject(new Error("failed to load " + src));
    document.head.appendChild(s);
  });
}

async function loadWasm(onProgress: (msg: string) => void): Promise<void> {
  if (!wasmLoaded) {
    wasmLoaded = (async () => {
      onProgress("Loading the Go runtime…");
      await loadScript("wasm_exec.js");
      const go = new window.Go();
      onProgress("Downloading the simulator (the real Raft + feature code, compiled to WebAssembly)…");
      const ready = new Promise<void>((res) => window.addEventListener("streamforge-sim-ready", () => res(), { once: true }));
      const resp = await fetch("sim.wasm");
      let result: WebAssembly.WebAssemblyInstantiatedSource;
      if ("instantiateStreaming" in WebAssembly && resp.headers.get("content-type")?.includes("wasm")) {
        result = await WebAssembly.instantiateStreaming(resp, go.importObject);
      } else {
        result = await WebAssembly.instantiate(await resp.arrayBuffer(), go.importObject);
      }
      go.run(result.instance);
      onProgress("Starting a 5-node cluster…");
      await ready;
    })();
  }
  return wasmLoaded;
}

export async function connectSim(onSnap: (s: Snapshot) => void, onProgress: (msg: string) => void): Promise<Source> {
  await loadWasm(onProgress);
  const sim = window.streamforgeSim!;
  let raf = 0;
  let last = performance.now();
  let lastSnap = 0;
  let stopped = false;
  const frame = (t: number) => {
    if (stopped) return;
    const dt = Math.min(250, t - last);
    last = t;
    sim.advance(dt);
    if (t - lastSnap > 50) {
      lastSnap = t;
      onSnap(JSON.parse(sim.snapshot()) as Snapshot);
    }
    raf = requestAnimationFrame(frame);
  };
  raf = requestAnimationFrame(frame);
  return {
    mode: "sim",
    async chaos(action: string) {
      return sim.chaos(action);
    },
    setSpeed: (x: number) => sim.setSpeed(x),
    close() {
      stopped = true;
      cancelAnimationFrame(raf);
    },
  };
}
