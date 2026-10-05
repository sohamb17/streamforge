import { useEffect, useRef, useState } from "react";
import type { Snapshot } from "../types";

const W = 460;
const H = 390;
const CX = W / 2;
const CY = H / 2 + 6;
const R = 140;

const msgColor: Record<string, string> = {
  App: "#38bdf8",
  AppResp: "#38bdf8",
  Heartbeat: "#2dd4bf",
  HeartbeatResp: "#2dd4bf",
  Vote: "#fb923c",
  VoteResp: "#fb923c",
  PreVote: "#fbbf24",
  PreVoteResp: "#fbbf24",
  Snap: "#a78bfa",
};

function pos(i: number, n: number) {
  const a = -Math.PI / 2 + (i * 2 * Math.PI) / n;
  return { x: CX + R * Math.cos(a), y: CY + R * Math.sin(a) };
}

interface Particle {
  from: number;
  to: number;
  t0: number;
  dur: number;
}

export function RaftRing({ s }: { s: Snapshot }) {
  const nodes = [...s.nodes].sort((a, b) => a.id - b.id);
  const idx = new Map(nodes.map((n, i) => [n.id, i]));
  const P = (id: number) => pos(idx.get(id) ?? 0, nodes.length || 5);
  const [, force] = useState(0);
  const parts = useRef<Particle[]>([]);
  const linksRef = useRef(s.links);
  linksRef.current = s.links;
  const live = s.mode === "live";

  // Live mode: the console reports per-link message rates, so animate
  // particles at a rate proportional to them (capped for readability).
  useEffect(() => {
    if (!live) return;
    let raf = 0;
    let last = performance.now();
    const loop = (t: number) => {
      const dt = (t - last) / 1000;
      last = t;
      for (const l of linksRef.current) {
        if (l.blocked || l.rate <= 0) continue;
        const shown = Math.min(8, 1.2 + Math.log2(1 + l.rate));
        if (Math.random() < shown * dt) parts.current.push({ from: l.from, to: l.to, t0: t, dur: 520 });
      }
      parts.current = parts.current.filter((p) => t - p.t0 < p.dur);
      force((x) => (x + 1) % 1e9);
      raf = requestAnimationFrame(loop);
    };
    raf = requestAnimationFrame(loop);
    return () => cancelAnimationFrame(raf);
  }, [live]);

  const blocked = (a: number, b: number) =>
    s.links.some((l) => ((l.from === a && l.to === b) || (l.from === b && l.to === a)) && l.blocked) ||
    !!nodes.find((n) => n.id === a)?.blocked?.includes(b);
  const rate = (a: number, b: number) =>
    s.links.filter((l) => (l.from === a && l.to === b) || (l.from === b && l.to === a)).reduce((x, l) => x + l.rate, 0);

  const edges: JSX.Element[] = [];
  for (let i = 0; i < nodes.length; i++)
    for (let j = i + 1; j < nodes.length; j++) {
      const a = nodes[i], b = nodes[j];
      const pa = P(a.id), pb = P(b.id);
      const bl = blocked(a.id, b.id);
      const act = rate(a.id, b.id) > 0;
      edges.push(
        <line
          key={`e${a.id}-${b.id}`}
          x1={pa.x}
          y1={pa.y}
          x2={pb.x}
          y2={pb.y}
          stroke={bl ? "#f87171" : act ? "#2a4b6e" : "#1b2742"}
          strokeWidth={bl ? 2 : act ? 1.6 : 1}
          strokeDasharray={bl ? "6 6" : undefined}
          opacity={bl ? 0.9 : 1}
        />,
      );
    }

  const dots: JSX.Element[] = [];
  const now = performance.now();
  const drawDot = (from: number, to: number, p: number, color: string, key: string, r = 3.6) => {
    const a = P(from), b = P(to);
    // offset each direction to its own lane
    const dx = b.x - a.x, dy = b.y - a.y;
    const len = Math.hypot(dx, dy) || 1;
    const ox = (-dy / len) * 5, oy = (dx / len) * 5;
    dots.push(<circle key={key} cx={a.x + dx * p + ox} cy={a.y + dy * p + oy} r={r} fill={color} opacity={0.95} />);
  };
  if (live) {
    parts.current.forEach((pt, i) => {
      const p = (now - pt.t0) / pt.dur;
      const isLeader = pt.from === s.leader;
      drawDot(pt.from, pt.to, Math.max(0, Math.min(1, p)), isLeader ? "#38bdf8" : "#2dd4bf", "p" + i, isLeader ? 3.6 : 2.8);
    });
  } else {
    (s.msgs ?? []).slice(0, 160).forEach((m, i) => drawDot(m.from, m.to, m.p, msgColor[m.type] ?? "#94a3b8", "m" + i, m.type === "Snap" ? 5 : 3.4));
  }

  const leaderNode = nodes.find((n) => n.id === s.leader);
  return (
    <div>
      <svg className="ring-svg" viewBox={`0 0 ${W} ${H}`} role="img" aria-label="Raft cluster of five nodes">
        <defs>
          <radialGradient id="gLeader" cx="50%" cy="40%" r="60%">
            <stop offset="0%" stopColor="#fde68a" />
            <stop offset="100%" stopColor="#d97706" />
          </radialGradient>
          <radialGradient id="gFollower" cx="50%" cy="40%" r="60%">
            <stop offset="0%" stopColor="#1f6f78" />
            <stop offset="100%" stopColor="#0f3b4a" />
          </radialGradient>
          <radialGradient id="gCand" cx="50%" cy="40%" r="60%">
            <stop offset="0%" stopColor="#fdba74" />
            <stop offset="100%" stopColor="#c2410c" />
          </radialGradient>
          <filter id="glow" x="-50%" y="-50%" width="200%" height="200%">
            <feGaussianBlur stdDeviation="6" result="b" />
            <feMerge>
              <feMergeNode in="b" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
        </defs>
        {edges}
        {dots}
        <text x={CX} y={CY - 6} textAnchor="middle" fontSize="12" fill="#8f9cb8">term</text>
        <text x={CX} y={CY + 22} textAnchor="middle" fontSize="30" fontWeight="800" fill="#e7ecf6" className="mono">{s.term || "-"}</text>
        {nodes.map((n) => {
          const p = P(n.id);
          const role = n.state !== "up" ? n.state : n.role;
          const isLeader = n.id === s.leader && n.state === "up";
          const cand = role === "candidate" || role === "pre-candidate";
          let fill = "url(#gFollower)", stroke = "#2dd4bf", tagColor = "#2dd4bf";
          if (isLeader) { fill = "url(#gLeader)"; stroke = "#fbbf24"; tagColor = "#fbbf24"; }
          else if (cand) { fill = "url(#gCand)"; stroke = "#fb923c"; tagColor = "#fb923c"; }
          if (n.state === "down" || n.state === "unreachable") { fill = "#1a2235"; stroke = "#475569"; tagColor = "#94a3b8"; }
          if (n.state === "paused") { fill = "#1e3a5f"; stroke = "#60a5fa"; tagColor = "#93c5fd"; }
          const label = isLeader ? "leader" : n.state === "up" ? n.role : n.state;
          return (
            <g key={n.id}>
              {isLeader && <circle cx={p.x} cy={p.y} r={40} fill="none" stroke="#fbbf24" strokeOpacity={0.35} strokeWidth={6} filter="url(#glow)" />}
              {cand && <circle cx={p.x} cy={p.y} r={38} fill="none" stroke="#fb923c" strokeWidth={2} strokeDasharray="4 5"><animateTransform attributeName="transform" type="rotate" from={`0 ${p.x} ${p.y}`} to={`360 ${p.x} ${p.y}`} dur="2.5s" repeatCount="indefinite" /></circle>}
              <circle cx={p.x} cy={p.y} r={30} fill={fill} stroke={stroke} strokeWidth={2} strokeDasharray={n.state === "unreachable" ? "4 4" : undefined} />
              <text x={p.x} y={p.y + 5} className="node-label" fill={isLeader ? "#1c1300" : undefined} style={isLeader ? { fill: "#1c1300" } : undefined}>
                {n.state === "down" ? "✕" : n.state === "paused" ? "❚❚" : `N${n.id}`}
              </text>
              <text x={p.x} y={p.y + 48} className="role-tag" fill={tagColor}>{label}</text>
              <text x={p.x} y={p.y + 62} className="node-sub">{n.state === "up" ? `commit ${n.commit.toLocaleString()}` : " "}</text>
            </g>
          );
        })}
      </svg>
      <div className="ring-meta">
        <span>Leader: <b>{leaderNode ? `node ${leaderNode.id}` : "electing…"}</b></span>
        <span>Commit index: <b className="mono">{leaderNode ? leaderNode.commit.toLocaleString() : "-"}</b></span>
        <span>Last failover: <b className="mono">{s.chaos.lastFailoverMs ? `${s.chaos.lastFailoverMs} ms` : "-"}</b></span>
      </div>
      <div className="legend-row">
        {live ? (
          <>
            <span><i style={{ background: "#38bdf8" }} />from the leader (entries, heartbeats)</span>
            <span><i style={{ background: "#2dd4bf" }} />responses</span>
          </>
        ) : (
          <>
            <span><i style={{ background: "#38bdf8" }} />AppendEntries</span>
            <span><i style={{ background: "#2dd4bf" }} />Heartbeat</span>
            <span><i style={{ background: "#fbbf24" }} />PreVote</span>
            <span><i style={{ background: "#fb923c" }} />Vote</span>
            <span><i style={{ background: "#a78bfa" }} />Snapshot</span>
          </>
        )}
        <span><i style={{ background: "#f87171" }} />cut link</span>
      </div>
      <table className="nodes-tbl" aria-label="Per-node Raft state">
        <thead>
          <tr><th>node</th><th>role</th><th>term</th><th>last index</th><th>commit</th><th>applied</th><th>snapshot</th></tr>
        </thead>
        <tbody>
          {nodes.map((n) => {
            const up = n.state === "up";
            const isLeader = n.id === s.leader && up;
            const lag = leaderNode && up ? leaderNode.commit - n.commit : 0;
            return (
              <tr key={n.id} className={isLeader ? "lead" : up ? "" : "off"}>
                <td>N{n.id}</td>
                <td>{up ? (isLeader ? "leader" : n.role) : n.state}</td>
                <td className="n">{up ? n.term : "-"}</td>
                <td className="n">{up ? n.lastIndex.toLocaleString() : "-"}</td>
                <td className="n">{up ? n.commit.toLocaleString() : "-"}{up && lag > 0 ? <span className="lag"> −{lag.toLocaleString()}</span> : null}</td>
                <td className="n">{up ? n.applied.toLocaleString() : "-"}</td>
                <td className="n">{up ? (n.snapIndex ? n.snapIndex.toLocaleString() : "-") : "-"}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
