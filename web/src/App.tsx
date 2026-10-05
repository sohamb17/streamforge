import { useEffect, useMemo, useRef, useState } from "react";
import { connectLive, connectSim, type Source } from "./source";
import type { Snapshot, ZoneRow } from "./types";
import { ZoneMap, type Metric } from "./components/ZoneMap";
import { RaftRing } from "./components/RaftRing";
import { ChaosPanel, ChecksPanel, EventLog, Metrics, ZoneDetail } from "./components/Panels";
import { HowItWorks } from "./components/HowItWorks";

const REPO = "https://github.com/sohamb17/streamforge";

function fmtEventTime(ms: number) {
  if (!ms) return "-";
  // TLC timestamps are New York local time stored without a zone; they are
  // treated as UTC throughout, so format them as UTC to show the NYC clock.
  const d = new Date(ms);
  return d.toLocaleString("en-US", { timeZone: "UTC", weekday: "short", month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", hour12: false });
}

export default function App() {
  const [snap, setSnap] = useState<Snapshot | null>(null);
  const [src, setSrc] = useState<Source | null>(null);
  const [status, setStatus] = useState("Looking for the live cluster…");
  const [metric, setMetric] = useState<Metric>("t5");
  const [selected, setSelected] = useState<number | null>(161);
  const [liveAvailable, setLiveAvailable] = useState(false);
  const history = useRef(new Map<number, { asOf: number; v: number }[]>());

  useEffect(() => {
    let cancelled = false;
    let cur: Source | null = null;
    const forceSim = new URLSearchParams(location.search).get("mode") === "sim";
    (async () => {
      if (!forceSim) {
        const live = await connectLive((s) => !cancelled && setSnap(s));
        if (live) {
          cur = live;
          setSrc(live);
          setLiveAvailable(true);
          return;
        }
      }
      setStatus("No live cluster reachable. Starting the in-browser simulation…");
      try {
        const sim = await connectSim((s) => !cancelled && setSnap(s), (m) => setStatus(m));
        cur = sim;
        setSrc(sim);
      } catch (e) {
        setStatus("Could not start the simulation: " + String(e));
      }
    })();
    return () => {
      cancelled = true;
      cur?.close();
    };
  }, []);

  const rows = useMemo(() => {
    const m = new Map<number, ZoneRow>();
    (snap?.zones ?? []).forEach((z) => m.set(z.id, z));
    return m;
  }, [snap?.zones]);

  // Per-zone trips_5m history, one point per new window.
  useEffect(() => {
    rows.forEach((r) => {
      const h = history.current.get(r.id) ?? [];
      if (!h.length || h[h.length - 1].asOf !== r.asOf) {
        h.push({ asOf: r.asOf, v: r.t5 });
        if (h.length > 120) h.shift();
        history.current.set(r.id, h);
      }
    });
  }, [rows]);

  if (!snap || !src) {
    return (
      <div className="wrap">
        <div className="loading">
          <div>
            <div className="spin" />
            <div>{status}</div>
          </div>
        </div>
      </div>
    );
  }

  const isLive = snap.mode === "live";
  const s = snap;
  const busiest = [...rows.values()].sort((a, b) => b.t5 - a.t5)[0];
  const lzOK = (s.checks.linz ?? []).filter((w) => w.verdict === "ok").reduce((a, w) => a + w.ops, 0);

  return (
    <div className="wrap">
      <header className="top">
        <div className="brand">
          <svg className="logo" viewBox="0 0 32 32" aria-hidden>
            <circle cx="16" cy="16" r="13" fill="#0b1020" stroke="#2dd4bf" strokeWidth="3" />
            {[0, 1, 2, 3, 4].map((i) => {
              const a = -Math.PI / 2 + (i * 2 * Math.PI) / 5;
              return <circle key={i} cx={16 + 7 * Math.cos(a)} cy={16 + 7 * Math.sin(a)} r={i === 0 ? 2.6 : 1.9} fill={i === 0 ? "#fbbf24" : "#2dd4bf"} />;
            })}
          </svg>
          <h1>StreamForge</h1>
        </div>
        <span className={"badge " + (isLive ? "" : "sim")}>
          <span className="dot" />
          {isLive ? "Live cluster" : "Simulation in your browser"}
        </span>
        <div className="tag">A real-time feature store on a Raft cluster written from scratch in Go. Break it and watch the answers stay correct.</div>
        <nav className="top-links">
          <a href="#how">How it works</a>
          <a href={REPO} target="_blank" rel="noreferrer">GitHub</a>
          {isLive && <a href={(src.liveBase ?? "") + "grafana/"} target="_blank" rel="noreferrer">Grafana</a>}
          <a href={REPO + "/blob/main/bench/results/gate4/REPORT.md"} target="_blank" rel="noreferrer">Results</a>
        </nav>
      </header>

      {!isLive && (
        <div className="banner">
          <span>
            You are running the <b>actual Raft, state-machine and windowing code</b>, compiled to WebAssembly, on a simulated network with
            {" "}{(5).toString()} nodes. Real NYC taxi trips from Friday, March 6, 2026, replay at 60x.
            {liveAvailable ? "" : " The full Docker deployment (Kafka, Redis, Postgres, Grafana) runs from the repo with one command."}
          </span>
          <span className="speed">
            speed
            <select defaultValue="60" onChange={(e) => src.setSpeed?.(Number(e.target.value))} style={{ background: "#0d1426", color: "#e7ecf6", border: "1px solid #22304f", borderRadius: 6 }}>
              <option value="30">30x</option>
              <option value="60">60x</option>
              <option value="180">180x</option>
              <option value="600">600x</option>
            </select>
          </span>
        </div>
      )}

      <section className="story" aria-label="How data flows">
        <div className="step">
          <div className="k">1 · Events in</div>
          <div className="t">{isLive ? "Taxi trips stream into Kafka" : "Real March 2026 taxi trips, replayed"}</div>
          <div className="v"><b className="mono">{Math.round(s.ingest.eventsPerSec).toLocaleString()}</b> trips/s · NYC clock <b className="mono">{fmtEventTime(s.eventTimeMs)}</b>{s.speedup ? <> ({Math.round(s.speedup)}x)</> : null}</div>
        </div>
        <div className="arrow">→</div>
        <div className="step">
          <div className="k">2 · Features out</div>
          <div className="t">Per-zone demand, every event-time minute</div>
          <div className="v">{busiest ? <>busiest: <b>{busiest.t5} trips</b> in 5 min</> : "waiting for the first window…"} · {rows.size} zones live</div>
        </div>
        <div className="arrow">→</div>
        <div className="step">
          <div className="k">3 · Replicated and served</div>
          <div className="t">5-node Raft, readable in milliseconds</div>
          <div className="v">term <b className="mono">{s.term}</b> · leader <b>{s.leader ? `node ${s.leader}` : "electing"}</b> · <b className="mono">{lzOK.toLocaleString()}</b> ops checked linearizable</div>
        </div>
      </section>

      <div className="grid">
        <div className="card span-7">
          <h2>Taxi demand right now, per zone</h2>
          <p className="sub">Each zone's color is a feature value served by the store. Hover a zone for its full feature vector; click to follow it.</p>
          <ZoneMap rows={rows} metric={metric} onMetric={setMetric} selected={selected} onSelect={setSelected} />
        </div>
        <div className="card span-5">
          <h2>The 5-node Raft cluster</h2>
          <p className="sub">Every feature update is a Raft log entry, committed once a majority has it on disk. Dots are messages between nodes.</p>
          <RaftRing s={s} />
        </div>

        <div className="card span-5">
          <h2>Try to break it</h2>
          <p className="sub">These inject real faults{isLive ? " into the running containers" : " into the simulated cluster"}. Each one heals itself.</p>
          <ChaosPanel s={s} onChaos={(a) => src.chaos(a)} />
        </div>
        <div className="card span-4">
          <h2>Is it still correct?</h2>
          <p className="sub">Checked continuously, during faults too.</p>
          <ChecksPanel s={s} />
        </div>
        <div className="card span-3">
          <h2>What just happened</h2>
          <p className="sub">Faults, elections and checks.</p>
          <EventLog s={s} />
        </div>

        <div className="card span-12">
          <Metrics s={s} />
        </div>

        {selected !== null && (
          <div className="card span-12">
            <ZoneDetail id={selected} row={rows.get(selected)} history={(history.current.get(selected) ?? []).map((h) => h.v)} />
          </div>
        )}
      </div>

      <HowItWorks />

      <footer className="footer">
        <span>StreamForge by Soham Belurgikar · <a href={REPO} target="_blank" rel="noreferrer">source on GitHub</a></span>
        <span>Trip data: NYC Taxi &amp; Limousine Commission, March 2026.</span>
      </footer>
    </div>
  );
}
