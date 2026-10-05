import { useState } from "react";
import type { Snapshot, ZoneRow } from "../types";
import { zoneById } from "./ZoneMap";

const ACTIONS: { id: string; title: string; icon: string; desc: string; modes: ("live" | "sim")[] }[] = [
  { id: "kill-leader", icon: "☠", title: "Kill the leader", modes: ["live", "sim"], desc: "SIGKILL the Raft leader. A new one should take over in about a second, with no lost or doubled writes." },
  { id: "partition", icon: "✂", title: "Partition the network", modes: ["live", "sim"], desc: "Cut the leader and one follower off from the other three. Only the majority side may commit." },
  { id: "pause-leader", icon: "❄", title: "Freeze the leader", modes: ["live", "sim"], desc: "Pause it like a long GC stall. The others elect a new leader; the old one steps down when it wakes." },
  { id: "kill-follower", icon: "⚡", title: "Kill a follower", modes: ["live", "sim"], desc: "Writes continue on 4 of 5 nodes. When it comes back it catches up, by snapshot if it fell far behind." },
  { id: "kill-worker", icon: "⟳", title: "Crash the stream worker", modes: ["live", "sim"], desc: "It restarts from the offset stored in Raft, not from Kafka, so no trip is counted twice." },
  { id: "redeliver", icon: "⧉", title: "Deliver duplicates", modes: ["live", "sim"], desc: "Re-send an already committed batch and replay Kafka offsets. Both must be ignored." },
  { id: "stop-redis", icon: "⛁", title: "Take down the cache", modes: ["live"], desc: "Kill Redis. Reads fall back to the Raft leader; only latency should change." },
  { id: "lossy-network", icon: "≈", title: "Drop 30% of messages", modes: ["sim"], desc: "Raft retries; progress slows, nothing breaks." },
];

export function ChaosPanel({ s, onChaos }: { s: Snapshot; onChaos(a: string): Promise<string> }) {
  const [err, setErr] = useState("");
  const [busy, setBusy] = useState(false);
  const a = s.chaos.active;
  const cooling = !a && s.chaos.cooldownUntil > s.now;
  const run = async (id: string) => {
    setBusy(true);
    setErr("");
    const e = await onChaos(id);
    setBusy(false);
    if (e) setErr(e);
  };
  if (!s.chaos.enabled) {
    return <p className="sub">Fault injection is turned off on this deployment.</p>;
  }
  const total = a ? a.healAt - a.started : 1;
  const left = a ? Math.max(0, a.healAt - s.now) : 0;
  return (
    <div>
      {a && (
        <div className="active-fault">
          <div style={{ display: "flex", justifyContent: "space-between", gap: 8, alignItems: "center" }}>
            <span><b>{ACTIONS.find((x) => x.id === a.name)?.title ?? a.name}</b>: {a.target}</span>
            <button className="btn-ghost" onClick={() => run("heal")}>Heal now</button>
          </div>
          <div className="muted" style={{ fontSize: 12, marginTop: 4 }}>Heals itself in {Math.ceil(left / 1000)} s. Watch the cluster and the checks.</div>
          <div className="bar-track"><div className="bar-fill" style={{ width: `${100 - (left / total) * 100}%` }} /></div>
        </div>
      )}
      <div className="chaos-grid">
        {ACTIONS.filter((x) => x.modes.includes(s.mode)).map((x) => (
          <button key={x.id} className="cbtn" disabled={busy || !!a || cooling} onClick={() => run(x.id)} title={cooling ? "Cooling down after the last fault" : x.desc}>
            <div className="ct"><span aria-hidden>{x.icon}</span>{x.title}</div>
            <div className="cd">{x.desc}</div>
          </button>
        ))}
      </div>
      <div className="err">{err || (cooling ? "Cooling down for a few seconds after the last fault…" : "")}</div>
    </div>
  );
}

export function ChecksPanel({ s }: { s: Snapshot }) {
  const lz = s.checks.linz ?? [];
  const last = lz[lz.length - 1];
  const anyBad = lz.some((w) => w.verdict === "illegal");
  const linzOK = last && !anyBad;
  const o = s.checks.oracle;
  const oracleState = !o.at ? "wait" : o.warmingUp ? "wait" : o.mismatches > 0 ? "bad" : "ok";
  const chaosWindows = lz.filter((w) => w.duringChaos && w.verdict === "ok").length;
  return (
    <div>
      <div className="check">
        <div className={"ic " + (!last ? "wait" : linzOK ? "ok" : "bad")}>{!last ? "…" : linzOK ? "✓" : "!"}</div>
        <div>
          <div className="ttl">Reads and writes are linearizable</div>
          <div className="dsc">
            {last
              ? <>Two clients read and write through Raft nonstop; every {last.seconds} s their full history is checked with Porcupine. Last window: <b className="mono">{last.ops}</b> ops. {chaosWindows > 0 && <>{chaosWindows} window{chaosWindows > 1 ? "s" : ""} overlapped a fault and still passed.</>}</>
              : "Recording the first window of client operations…"}
          </div>
          <div className="squares" aria-label="Recent linearizability windows">
            {lz.slice(-28).map((w) => (
              <span key={w.end} className={"sq " + (w.verdict === "illegal" ? "bad" : w.verdict === "unknown" ? "unknown" : "") + (w.duringChaos ? " chaos" : "")} title={`${w.ops} ops, ${w.verdict}${w.duringChaos ? ", during a fault" : ""}`} />
            ))}
          </div>
        </div>
      </div>
      <div className="check">
        <div className={"ic " + oracleState}>{oracleState === "ok" ? "✓" : oracleState === "bad" ? "!" : "…"}</div>
        <div>
          <div className="ttl">Features match a from-scratch recomputation</div>
          <div className="dsc">
            {!o.at ? "Starting an independent replay of the Kafka log…" : o.warmingUp ? "The independent replay is catching up…" : (
              <>An independent consumer recomputes every window from the Kafka log (no Raft, no crashes). Latest comparison: <b className="mono">{o.zonesChecked - o.mismatches}/{o.zonesChecked}</b> zones identical{o.pending ? `, ${o.pending} not yet replayed` : ""}. Total mismatches so far: <b className="mono">{o.totalBad}</b>.</>
            )}
          </div>
        </div>
      </div>
      <div className="check">
        <div className="ic ok">⧉</div>
        <div>
          <div className="ttl">Duplicates ignored: <span className="mono">{Math.round(s.ingest.duplicatesIgnored).toLocaleString()}</span></div>
          <div className="dsc">Each batch commits with the Kafka offset that produced it, in the same Raft entry. A batch at or below the stored offset is dropped, so redelivery cannot double-count.</div>
        </div>
      </div>
      <div className="check">
        <div className="ic wait">⏱</div>
        <div>
          <div className="ttl">Late events dropped: <span className="mono">{Math.round(s.ingest.lateDropped).toLocaleString()}</span></div>
          <div className="dsc">Windows use event time. Events up to 30 s late still count; later ones are counted here instead of silently changing a window that was already served.</div>
        </div>
      </div>
    </div>
  );
}

export function EventLog({ s }: { s: Snapshot }) {
  const lines = [...(s.log ?? [])].reverse();
  const fmt = (t: number) => {
    if (s.mode === "sim") return `${(t / 1000).toFixed(1)}s`;
    const d = new Date(t);
    return d.toLocaleTimeString([], { hour12: false });
  };
  return (
    <div className="log">
      <div>
        {lines.map((l, i) => (
          <div className={"log-line " + l.kind} key={i}>
            <span className="lt">{fmt(l.t)}</span>
            <span className="lm">{l.msg}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

function fmtNum(v: number, d = 0) {
  if (!isFinite(v)) return "-";
  if (v >= 1e6) return (v / 1e6).toFixed(1) + "M";
  if (v >= 1e4) return (v / 1e3).toFixed(1) + "k";
  return v.toFixed(d);
}

export function Metrics({ s }: { s: Snapshot }) {
  const live = s.mode === "live";
  const m: [string, string, string][] = [
    ["Events / s", fmtNum(s.ingest.eventsPerSec), live ? "consumed from Kafka" : "into the simulated log"],
    ["Consumer lag", fmtNum(s.ingest.lag), "events not yet windowed"],
    ["Events processed", fmtNum(s.ingest.eventsTotal), "since start"],
    ["Freshness p50", live ? fmtNum(s.ingest.freshnessP50Ms, 1) + " ms" : "-", "event published → committed"],
    ["Freshness p99", live ? fmtNum(s.ingest.freshnessP99Ms, 1) + " ms" : "-", "event published → committed"],
    ["Raft commit p99", live ? fmtNum(s.ingest.commitP99Ms, 1) + " ms" : "-", "propose → applied (leader)"],
    ["Serving p99", live && s.serving.rps > 0 ? fmtNum(s.serving.p99Ms, 1) + " ms" : "-", live ? `${fmtNum(s.serving.rps)} req/s` : "live cluster only"],
    ["Cache hit ratio", live && s.serving.rps > 0 ? (s.serving.hitRatio * 100).toFixed(0) + "%" : "-", "Redis, 2 s TTL"],
  ];
  return (
    <div className="metrics">
      {m.map(([l, v, h]) => (
        <div className="metric" key={l}>
          <div className="ml">{l}</div>
          <div className="mv">{v}</div>
          <div className="mh">{h}</div>
        </div>
      ))}
    </div>
  );
}

export function ZoneDetail({ id, row, history }: { id: number; row?: ZoneRow; history: number[] }) {
  const z = zoneById.get(id);
  const max = Math.max(1, ...history);
  const pts = history.map((v, i) => `${(i / Math.max(1, history.length - 1)) * 300},${60 - (v / max) * 54}`).join(" ");
  return (
    <div className="zone-detail">
      <div className="zh">
        <h3>{z?.name ?? `Zone ${id}`}</h3>
        <span className="muted">{z?.borough} · zone {id}</span>
        {row && <span className="faint mono" style={{ fontSize: 12 }}>as of {new Date(row.asOf).toISOString().slice(0, 16).replace("T", " ")} (event time)</span>}
      </div>
      {row ? (
        <div className="feat-grid">
          <div className="feat"><div className="fl">trips_5m</div><div className="fv">{row.t5}</div></div>
          <div className="feat"><div className="fl">trips_30m</div><div className="fv">{row.t30}</div></div>
          <div className="feat"><div className="fl">trips_60m</div><div className="fv">{row.t60}</div></div>
          <div className="feat"><div className="fl">demand_spike_ratio</div><div className="fv">{row.spike.toFixed(2)}</div></div>
          <div className="feat"><div className="fl">fare_mean_15m</div><div className="fv">${row.fare.toFixed(2)}</div></div>
          <div className="feat"><div className="fl">distance_mean_15m</div><div className="fv">{row.dist.toFixed(2)}</div></div>
        </div>
      ) : (
        <p className="muted">No trips started here in the last hour of event time.</p>
      )}
      {history.length > 1 && (
        <svg className="spark" viewBox="0 0 300 64" preserveAspectRatio="none" aria-label="trips_5m over time">
          <polyline points={pts} fill="none" stroke="#2dd4bf" strokeWidth="2" vectorEffect="non-scaling-stroke" />
        </svg>
      )}
      <div className="faint" style={{ fontSize: 11.5 }}>trips_5m since you opened this page. This is exactly what <span className="mono">GetFeatures</span> returns for this zone.</div>
    </div>
  );
}
