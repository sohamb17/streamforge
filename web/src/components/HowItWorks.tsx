import results from "../data/results.json";

function Box({ x, y, w, h, title, sub, color = "#2dd4bf" }: { x: number; y: number; w: number; h: number; title: string; sub: string; color?: string }) {
  return (
    <g>
      <rect x={x} y={y} width={w} height={h} rx={12} fill="#111a30" stroke={color} strokeOpacity={0.7} strokeWidth={1.5} />
      <text x={x + w / 2} y={y + 24} textAnchor="middle" fontSize="14" fontWeight="700" fill="#e7ecf6">{title}</text>
      {sub.split("\n").map((line, i) => (
        <text key={i} x={x + w / 2} y={y + 43 + i * 15} textAnchor="middle" fontSize="11.5" fill="#8f9cb8">{line}</text>
      ))}
    </g>
  );
}

function Arrow({ x1, y1, x2, y2, label, dashed }: { x1: number; y1: number; x2: number; y2: number; label?: string; dashed?: boolean }) {
  return (
    <g>
      <line x1={x1} y1={y1} x2={x2} y2={y2} stroke="#5b6889" strokeWidth={1.6} markerEnd="url(#ah)" strokeDasharray={dashed ? "5 5" : undefined} />
      {label && (
        <text x={(x1 + x2) / 2} y={(y1 + y2) / 2 - 7} textAnchor="middle" fontSize="11" fill="#8f9cb8">{label}</text>
      )}
    </g>
  );
}

export function Architecture() {
  return (
    <svg className="arch" viewBox="0 0 1180 330" role="img" aria-label="StreamForge architecture">
      <defs>
        <marker id="ah" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
          <path d="M0,0 L10,5 L0,10 z" fill="#5b6889" />
        </marker>
      </defs>
      <Box x={10} y={30} w={160} h={78} title="Replayer" sub={"NYC TLC trips\n(or synthetic), Go"} color="#38bdf8" />
      <Box x={230} y={30} w={180} h={78} title="Kafka" sub={"topic trips, 6 partitions\nkeyed by pickup zone"} color="#38bdf8" />
      <Box x={470} y={30} w={200} h={78} title="Stream workers" sub={"event-time windows,\nwatermark, late policy (Go)"} />
      <Box x={730} y={20} w={230} h={98} title="Online store: 5-node Raft" sub={"Raft written from scratch in Go\nfeatures + window state + offset\ncommitted in one log entry"} color="#fbbf24" />
      <Box x={1010} y={30} w={160} h={78} title="Redis" sub={"read-through cache\nTTL = staleness bound"} color="#a78bfa" />
      <Box x={730} y={200} w={230} h={78} title="FeatureService (gRPC)" sub={"deadlines propagated;\nlinearizable or bounded-stale"} />
      <Box x={1010} y={200} w={160} h={78} title="Model / this page" sub={"GetFeatures(zone ids)"} color="#38bdf8" />
      <Box x={470} y={200} w={200} h={78} title="Postgres history" sub={"append-only, idempotent;\npoint-in-time joins"} color="#a78bfa" />
      <Box x={10} y={200} w={400} h={78} title="Prometheus + Grafana + checks" sub={"freshness, lag, Raft and serving metrics; Porcupine\nlinearizability checks; oracle recomputation"} color="#5b6889" />
      <Arrow x1={170} y1={69} x2={228} y2={69} label="produce" />
      <Arrow x1={410} y1={69} x2={468} y2={69} label="consume" />
      <Arrow x1={670} y1={69} x2={728} y2={69} label="propose" />
      <Arrow x1={960} y1={69} x2={1008} y2={69} label="invalidate" dashed />
      <Arrow x1={845} y1={118} x2={845} y2={198} label="" />
      <Arrow x1={1060} y1={108} x2={962} y2={212} label="" dashed />
      <Arrow x1={960} y1={239} x2={1008} y2={239} label="" />
      <text x={1030} y={170} fontSize="11" fill="#8f9cb8">cache reads</text>
      <Arrow x1={570} y1={108} x2={570} y2={198} label="rows" />
      <text x={860} y={165} fontSize="11" fill="#8f9cb8">ReadIndex reads</text>
    </svg>
  );
}

export function HowItWorks() {
  const r = results as any;
  return (
    <section className="how" id="how">
      <h2>What this is</h2>
      <p>
        StreamForge is a small <b>real-time feature store</b>. A model that dispatches taxis wants to ask, every few milliseconds,
        "how busy is Midtown right now compared with its last hour?" StreamForge turns a stream of trip events into exactly that
        kind of per-zone feature, keeps it fresh within milliseconds of the trip, and serves it over gRPC. It also keeps a
        history of every value so training data can be built with what a model could actually have seen at the time.
      </p>
      <p>
        The point is not that feature stores are new. The point is the hard part underneath: the online store is a
        <b> 5-node Raft cluster implemented from scratch in Go</b>, and the answers must stay correct while processes crash,
        messages are duplicated and the network splits. The buttons above do exactly that: to the running containers on the live cluster, or to the same Go code on a simulated network in the browser version.
      </p>
      <Architecture />
      <div className="qa">
        <div className="card">
          <h3>Can values stay correct when processes crash or retry?</h3>
          <p className="muted">
            Kafka delivers at least once. Each worker batch is committed to Raft <i>together with</i> the Kafka offset that produced
            it and the window state, in a single log entry. A restarted worker resumes from that stored offset, and the state
            machine drops any batch at or below it. At-least-once delivery plus idempotent apply gives effectively-once effects,
            checked by recomputing everything from the log.
          </p>
        </div>
        <div className="card">
          <h3>Can reads stay fast and correct while writes replicate?</h3>
          <p className="muted">
            Linearizable reads use Raft's ReadIndex: the leader confirms with a heartbeat round that it is still the leader before
            answering, so a deposed leader cannot return stale data. Cached reads are labelled as bounded-staleness, with the bound
            (the TTL) in every response. They are never called linearizable.
          </p>
        </div>
        <div className="card">
          <h3>Can the measurements be defended?</h3>
          <p className="muted">
            Recorded client histories are checked with Porcupine under real kills, pauses and iptables partitions, plus a negative
            control that the checker must reject. Latency comes from an open-loop load generator that measures from the scheduled
            send time (no coordinated omission), and every number names its hardware.
          </p>
        </div>
      </div>
      {r.rows?.length > 0 && (
        <>
          <h2>Measured results</h2>
          <p>{r.note}</p>
          <div className="card" style={{ overflowX: "auto" }}>
            <table className="res">
              <thead>
                <tr><th>What</th><th>Result</th><th>Conditions</th></tr>
              </thead>
              <tbody>
                {r.rows.map((x: any, i: number) => (
                  <tr key={i}><td>{x.what}</td><td className="n">{x.value}</td><td className="muted">{x.how}</td></tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </section>
  );
}
