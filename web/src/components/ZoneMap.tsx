import { useMemo, useRef, useState } from "react";
import zonesData from "../data/zones.json";
import type { ZoneRow, ZoneShape } from "../types";

const shapes = zonesData.zones as ZoneShape[];
export const zoneById = new Map<number, ZoneShape>(shapes.map((z) => [z.id, z]));

export type Metric = "t5" | "spike" | "fare";

// Sequential ramp for counts, diverging for the spike ratio (1 = normal).
function seqColor(v: number, max: number): string {
  if (v <= 0) return "#16203a";
  const t = Math.min(1, Math.log1p(v) / Math.log1p(max));
  const stops = [
    [22, 48, 78],
    [17, 94, 116],
    [20, 150, 140],
    [45, 212, 191],
    [190, 242, 100],
    [251, 191, 36],
  ];
  const x = t * (stops.length - 1);
  const i = Math.min(stops.length - 2, Math.floor(x));
  const f = x - i;
  const c = stops[i].map((a, k) => Math.round(a + (stops[i + 1][k] - a) * f));
  return `rgb(${c[0]},${c[1]},${c[2]})`;
}

function spikeColor(v: number, has: boolean): string {
  if (!has) return "#16203a";
  // below 1: calmer than the last hour (blue); above 1: busier (orange/red)
  if (v < 1) {
    const t = Math.max(0, v);
    return `rgb(${Math.round(30 + 60 * t)},${Math.round(70 + 80 * t)},${Math.round(140 + 30 * t)})`;
  }
  const t = Math.min(1, (v - 1) / 2);
  return `rgb(${Math.round(110 + 138 * t)},${Math.round(150 - 60 * t)},${Math.round(170 - 120 * t)})`;
}

interface Props {
  rows: Map<number, ZoneRow>;
  metric: Metric;
  onMetric(m: Metric): void;
  selected: number | null;
  onSelect(id: number | null): void;
}

export function ZoneMap({ rows, metric, onMetric, selected, onSelect }: Props) {
  const [hover, setHover] = useState<{ id: number; x: number; y: number } | null>(null);
  const wrap = useRef<HTMLDivElement>(null);
  const max = useMemo(() => {
    let m = 1;
    rows.forEach((r) => {
      if (metric === "t5") m = Math.max(m, r.t5);
      if (metric === "fare") m = Math.max(m, r.fare);
    });
    return m;
  }, [rows, metric]);

  const fill = (id: number) => {
    const r = rows.get(id);
    if (metric === "spike") return spikeColor(r?.spike ?? 0, !!r && r.t60 > 0);
    if (!r) return "#16203a";
    return seqColor(metric === "t5" ? r.t5 : r.fare, max);
  };

  const hz = hover ? zoneById.get(hover.id) : undefined;
  const hr = hover ? rows.get(hover.id) : undefined;

  return (
    <div className="map-wrap" ref={wrap}>
      <div className="map-tools">
        <div className="seg" role="tablist" aria-label="Map metric">
          <button className={metric === "t5" ? "on" : ""} onClick={() => onMetric("t5")}>Trips, last 5 min</button>
          <button className={metric === "spike" ? "on" : ""} onClick={() => onMetric("spike")}>Demand spike</button>
          <button className={metric === "fare" ? "on" : ""} onClick={() => onMetric("fare")}>Mean fare, 15 min</button>
        </div>
        <div className="legend">
          {metric === "spike" ? (
            <>
              <span>quieter</span>
              <span className="bar" style={{ background: "linear-gradient(90deg,#1e468c,#5a96aa,#6e96aa,#f85a32)" }} />
              <span>busier than its last hour</span>
            </>
          ) : (
            <>
              <span>0</span>
              <span className="bar" style={{ background: "linear-gradient(90deg,#16304e,#115e74,#14968c,#2dd4bf,#bef264,#fbbf24)" }} />
              <span>{metric === "t5" ? Math.round(max) : "$" + max.toFixed(0)}</span>
            </>
          )}
        </div>
      </div>
      <svg className="map-svg" viewBox={`0 0 ${zonesData.width} ${zonesData.height}`} role="img" aria-label="Map of NYC taxi zones colored by feature value">
        {shapes.map((z) => (
          <path
            key={z.id}
            d={z.d}
            className={"zone" + (selected === z.id ? " sel" : "")}
            fill={fill(z.id)}
            onMouseMove={(e) => {
              const b = wrap.current?.getBoundingClientRect();
              if (b) setHover({ id: z.id, x: e.clientX - b.left, y: e.clientY - b.top });
            }}
            onMouseLeave={() => setHover(null)}
            onClick={() => onSelect(selected === z.id ? null : z.id)}
          />
        ))}
      </svg>
      {hover && hz && (
        <div className="tip" style={{ left: Math.min(hover.x + 14, (wrap.current?.clientWidth ?? 600) - 220), top: hover.y + 14 }}>
          <div className="zn">{hz.name}</div>
          <div className="muted" style={{ fontSize: 11.5, marginBottom: 6 }}>{hz.borough} · zone {hz.id}</div>
          {hr ? (
            <div className="kv">
              <span>trips 5 / 30 / 60 min</span>
              <span>{hr.t5} / {hr.t30} / {hr.t60}</span>
              <span>demand spike</span>
              <span>{hr.spike.toFixed(2)}x</span>
              <span>mean fare (15 min)</span>
              <span>${hr.fare.toFixed(2)}</span>
              <span>mean distance</span>
              <span>{hr.dist.toFixed(2)} mi</span>
            </div>
          ) : (
            <div className="muted">No trips in the last hour.</div>
          )}
        </div>
      )}
    </div>
  );
}
