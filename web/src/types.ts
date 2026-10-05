// Mirrors internal/console/model.go.

export interface NodeView {
  id: number;
  state: "up" | "down" | "paused" | "unreachable";
  role: string;
  term: number;
  lead: number;
  commit: number;
  applied: number;
  lastIndex: number;
  snapIndex: number;
  snapshotsInstalled: number;
  blocked?: number[];
}

export interface LinkView {
  from: number;
  to: number;
  rate: number;
  blocked: boolean;
}

export interface MsgView {
  from: number;
  to: number;
  type: string;
  p: number;
}

export interface ZoneRow {
  id: number;
  t5: number;
  t30: number;
  t60: number;
  fare: number;
  dist: number;
  spike: number;
  asOf: number;
}

export interface LinzWindow {
  end: number;
  seconds: number;
  ops: number;
  verdict: "ok" | "illegal" | "unknown";
  duringChaos: boolean;
}

export interface OracleView {
  at: number;
  zonesChecked: number;
  mismatches: number;
  pending: number;
  warmingUp: boolean;
  events: number;
  totalChecks: number;
  totalBad: number;
}

export interface ChaosAction {
  name: string;
  target: string;
  started: number;
  healAt: number;
}

export interface Snapshot {
  mode: "live" | "sim";
  now: number;
  eventTimeMs: number;
  speedup: number;
  term: number;
  leader: number;
  nodes: NodeView[];
  links: LinkView[];
  msgs?: MsgView[];
  ingest: {
    eventsPerSec: number;
    eventsTotal: number;
    lag: number;
    lateDropped: number;
    duplicatesIgnored: number;
    recordsSkipped: number;
    freshnessP50Ms: number;
    freshnessP99Ms: number;
    commitP99Ms: number;
    batchesPerSec: number;
  };
  serving: { rps: number; p50Ms: number; p99Ms: number; hitRatio: number };
  zones?: ZoneRow[];
  checks: { linz: LinzWindow[] | null; oracle: OracleView };
  chaos: { enabled: boolean; active?: ChaosAction; cooldownUntil: number; lastFailoverMs: number };
  workers: { name: string; state: string }[] | null;
  log: { t: number; kind: string; msg: string }[] | null;
}

export interface ZoneShape {
  id: number;
  name: string;
  borough: string;
  d: string;
  cx: number;
  cy: number;
}
