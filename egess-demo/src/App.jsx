import { useState, useEffect, useCallback, useRef, useMemo } from "react";

const GRID = 8;
const NN = GRID * GRID;
const BASE = 9000;
const SQRT3 = Math.sqrt(3);

function hexXY(r, c) {
  return { x: SQRT3 * (c + (r % 2 === 1 ? 0.5 : 0)), y: 1.5 * r };
}

const NODES = [];
for (let i = 0; i < NN; i++) {
  const r = Math.floor(i / GRID), c = i % GRID;
  const { x, y } = hexXY(r, c);
  NODES.push({ port: BASE + i, row: r, col: c, x, y });
}

function nbrRC(c, r) {
  if (r % 2 === 0) return [[c-1,r],[c+1,r],[c,r-1],[c-1,r-1],[c,r+1],[c-1,r+1]];
  return [[c-1,r],[c+1,r],[c+1,r-1],[c,r-1],[c+1,r+1],[c,r+1]];
}

const NBRS = {};
NODES.forEach(n => {
  NBRS[n.port] = [];
  nbrRC(n.col, n.row).forEach(([c, r]) => {
    if (c >= 0 && c < GRID && r >= 0 && r < GRID) {
      const p = BASE + r * GRID + c;
      if (p !== n.port) NBRS[n.port].push(p);
    }
  });
});

function sectorOf(mp, np) {
  const m = NODES.find(n => n.port === mp);
  const nb = NODES.find(n => n.port === np);
  if (!m || !nb) return 0;
  const dx = nb.x - m.x, dy = nb.y - m.y;
  if (Math.abs(dx) < 1e-9 && Math.abs(dy) < 1e-9) return 0;
  return Math.floor((((Math.atan2(dy, dx) * 180 / Math.PI) + 360) % 360 + 30) % 360 / 60) + 1;
}

function hexPts(cx, cy, sz) {
  const p = [];
  for (let i = 0; i < 6; i++) {
    const a = Math.PI / 3 * i + Math.PI / 6;
    p.push(`${cx + sz * Math.cos(a)},${cy + sz * Math.sin(a)}`);
  }
  return p.join(" ");
}

const S = {
  NORMAL: "NORMAL", WATCH: "WATCH", WARNING: "WARNING",
  IMPACT: "IMPACT", CONTAINED: "CONTAINED", RECOVERING: "RECOVERING"
};

const FILL = {
  NORMAL: "#4682B4", WATCH: "#DAA520", WARNING: "#CD853F",
  IMPACT: "#CD3333", CONTAINED: "#8B668B", RECOVERING: "#3CB371",
  OFFLINE: "#BABABA"
};

const hintStyle = { fontSize: "8px", color: "#999", fontStyle: "italic", marginBottom: 1, lineHeight: "1.35" };
const cardStyle = { background: "#fff", padding: "5px 8px", border: "1px solid #e0e0e0", borderRadius: "3px", marginTop: "5px" };
const refRow = { padding: "2px 0", lineHeight: "1.5", display: "flex", alignItems: "center", gap: 6 };
const refBadge = { display: "inline-block", padding: "1px 6px", fontSize: "10px", fontWeight: "bold", background: "#FFF3E0", border: "1px solid #ddd", borderRadius: 3, minWidth: 28, textAlign: "center" };
const refHint = { fontSize: "8px", color: "#999", fontStyle: "italic", marginBottom: 2, paddingLeft: 34 };

function mkNode() {
  return {
    online: true, state: S.NORMAL, T: 0, prevT: 0, sensor: "NORMAL",
    cycles: 0, noProg: 0, nbrSt: {}, missK: {}, known: [],
    tHist: [], slope: 0,
    frontScore: 0, impactScore: 0, arrestScore: 0, coherence: 0,
    dom: "none", cAdj: 0, cPers: 0, cProg: 0,
    pd: { nm: [], pm: [], rc: [], dg: [], tm: 0 },
    dSec: 0, sHist: []
  };
}

function pullNode(port, nodes) {
  const me = nodes[port];
  if (!me.online) return { ...me, cycles: me.cycles + 1 };

  const nbrs = NBRS[port] || [];
  const nSt = {};
  const mK = {};
  let nM = 0, pM = 0, dg = 0, rc = 0;
  const known = [];
  const pnm = [], ppm = [], prc = [], pdg = [];

  nbrs.forEach(np => {
    const nb = nodes[np];
    const prev = me.nbrSt[np] || "alive";
    const prevM = me.missK[np] || 0;

    if (!nb.online) {
      mK[np] = prevM + 1;
      nSt[np] = "missing";
      if (prev === "missing") { pM++; ppm.push(np); }
      else { nM++; pnm.push(np); }
    } else {
      mK[np] = 0;
      known.push(np);
      if (prev === "missing") { nSt[np] = "recovered"; rc++; prc.push(np); }
      else { nSt[np] = "alive"; }
      // Disagreement: neighbor is elevated (T>=3, any WATCH or above) and higher than me.
      // This lets early warnings propagate outward naturally.
      if (nb.T >= 3 && nb.T > me.T + 1) { dg++; pdg.push(np); }
    }
  });

  // Cap disagreement at 2 to prevent cascade.
  // Even if all 6 neighbors disagree, max contribution = 2, which alone < WATCH threshold (3).
  // You need actual missing neighbors to escalate — disagreement alone just raises awareness.
  const dgCapped = Math.min(dg, 2);

  const tM = nM + pM;
  const pd = { nm: pnm, pm: ppm, rc: prc, dg: pdg, tm: tM };

  // Sectors
  const sM = {};
  pnm.forEach(np => { const s = sectorOf(port, np); if (s > 0) sM[s] = (sM[s] || 0) + 1; });
  let dSec = 0, dV = 0;
  Object.entries(sM).forEach(([s, v]) => { if (v > dV) { dV = v; dSec = +s; } });
  const sHist = [...(me.sHist || []), dSec].slice(-3);

  const mSec = new Set();
  pnm.forEach(np => { const s = sectorOf(port, np); if (s > 0) mSec.add(s); });
  let cB = 0;
  mSec.forEach(s => { if (mSec.has(s === 1 ? 6 : s - 1) || mSec.has(s === 6 ? 1 : s + 1)) cB = 1; });

  // Scores
  const mom = (me.T > 0 && (nM > 0 || dgCapped > 0)) ? 1 : 0;
  const fs = 2 * nM + dgCapped + 0.5 * pM + mom;
  const is = 3 * tM + 2 * cB;

  const aStall = (me.noProg >= 3 && tM > 0 && nM === 0) ? 1 : 0;
  const aRetreat = rc > 0 ? 1 : 0;
  let aBypass = 0;
  if (me.dSec > 0) {
    const left = me.dSec === 1 ? 6 : me.dSec - 1;
    const right = me.dSec === 6 ? 1 : me.dSec + 1;
    if ((mSec.has(left) || mSec.has(right)) && !mSec.has(me.dSec)) aBypass = 1;
  }
  const aS = aStall + aRetreat - aBypass;

  const cAdj = cB;
  const cPers = sHist.filter(s => s === dSec && s > 0).length >= 2 ? 1 : 0;
  const cProg = (nM > 0 && dSec > 0) ? 1 : 0;
  const coh = cAdj + cPers + cProg;

  // T
  let T = Math.max(fs, is);
  if (me.sensor === "ALERT") T = Math.max(T, 5);
  if ((me.state === S.IMPACT || me.state === S.WARNING) && tM > 0 && T < me.T) T = Math.max(T, me.T - 1);
  if (me.state === S.CONTAINED) T = Math.max(T, Math.max(0, me.T - 0.5));
  if (me.state === S.RECOVERING) T = Math.max(0, Math.min(T, me.T - 1));
  if (fs === 0 && is === 0 && me.T > 0 && T === 0) T = Math.max(0, me.T - 1);
  T = Math.round(T * 10) / 10;

  // Slope
  const th = [...(me.tHist || []), T].slice(-4);
  let sl = 0;
  if (th.length >= 2) sl = Math.round(((th[th.length - 1] - th[0]) / (th.length - 1)) * 10) / 10;

  // Dominant
  let dom = "none";
  if (T > 0) {
    if (aS >= 1 && me.noProg >= 3) dom = "arrest";
    else if (is >= fs) dom = "impact";
    else dom = "front";
  }

  // State
  const hasProg = nM > 0 || me.sensor === "ALERT";
  let noProg = me.noProg;
  if (hasProg) noProg = 0;
  else if (tM > 0 || me.state !== S.NORMAL) noProg++;
  else noProg = 0;

  const gated = coh >= 2;
  let state = S.NORMAL;

  if (me.sensor === "RECOVERING" || (me.state === S.RECOVERING && T > 0)) state = S.RECOVERING;
  else if (me.state === S.CONTAINED && rc > 0 && nM === 0) state = S.RECOVERING;
  // CONTAINED path 1: WARNING/IMPACT nodes — front was here and stopped
  else if (noProg >= 3 && tM > 0 && nM === 0 && [S.IMPACT, S.WARNING, S.CONTAINED].includes(me.state)) state = S.CONTAINED;
  // CONTAINED path 2: WATCH nodes — slope confirms the front stalled here
  // If a WATCH node has nearby missing, no new damage, AND slope ≤ 0 (not getting worse),
  // the front reached this area but stopped advancing. That IS containment.
  else if (noProg >= 3 && tM > 0 && nM === 0 && sl <= 0 && me.state === S.WATCH) state = S.CONTAINED;
  else if (T >= 10) state = S.IMPACT;
  else if (me.state === S.IMPACT && tM > 0 && T >= 6) state = S.IMPACT;
  else if (T >= 6 && gated) state = S.WARNING;
  else if (T >= 3 && sl >= 2 && gated) state = S.WARNING;
  else if (T >= 3 && gated) state = S.WATCH;
  else if (T >= 2 && sl >= 1.5 && gated) state = S.WATCH;
  else if (T >= 6 && !gated) state = S.WATCH;
  else if (T > 0 && tM === 0 && rc > 0 && [S.IMPACT, S.WARNING, S.CONTAINED, S.WATCH].includes(me.state)) state = S.RECOVERING;
  else if (T > 0 && me.state !== S.NORMAL && rc > 0) state = S.RECOVERING;
  else if (me.state === S.RECOVERING && T > 0) state = S.RECOVERING;
  if (T === 0) state = S.NORMAL;

  return {
    ...me, T, prevT: me.T, state, noProg, nbrSt: nSt, missK: mK,
    cycles: me.cycles + 1, known, tHist: th, slope: sl,
    frontScore: Math.round(fs * 10) / 10, impactScore: Math.round(is * 10) / 10,
    arrestScore: aS, coherence: coh, dom, cAdj, cPers, cProg, pd, dSec, sHist
  };
}

// Demo scripts
function layersFrom(p) {
  const sn = NODES.find(n => n.port === p);
  const by = {};
  NODES.forEach(n => {
    const d = Math.floor(Math.sqrt((n.x - sn.x) ** 2 + (n.y - sn.y) ** 2) / 1.8);
    if (!by[d]) by[d] = [];
    by[d].push(n.port);
  });
  return Object.keys(by).sort((a, b) => a - b).map(k => by[k]);
}

function cornerScript() {
  const L = layersFrom(BASE);
  const mx = Math.min(L.length, 5);
  const st = [{ a: [], k: [], r: [], lbl: "Baseline — all stable, T = 0" }];
  st.push({ a: L[0] || [], k: [], r: [], lbl: "Disturbance at corner" });
  for (let i = 1; i < mx; i++) {
    st.push({ a: L[i] || [], k: L[i-1] || [], r: [], lbl: "Shell " + i + " alerted, shell " + (i-1) + " offline" });
  }
  st.push({ a: [], k: [], r: [], lbl: "No new losses — containment building" });
  st.push({ a: [], k: [], r: [], lbl: "Front arrested — CONTAINED" });
  for (let i = 0; i < mx; i++) {
    st.push({ a: [], k: [], r: L[i] || [], lbl: "Recovery — shell " + i });
  }
  st.push({ a: [], k: [], r: [], clr: true, lbl: "Baseline restored" });
  return st;
}

function tornadoScript() {
  const cp = BASE + Math.floor(GRID / 2) * GRID + Math.floor(GRID / 2);
  const L = layersFrom(cp);
  return [
    { a: [], k: [], r: [], lbl: "Baseline — T = 0" },
    { a: L[0] || [], k: [], r: [], lbl: "Sudden disturbance at center" },
    { a: L[1] || [], k: L[0] || [], r: [], lbl: "Center destroyed, ring 1 hit" },
    { a: [], k: L[1] || [], r: [], lbl: "Ring 1 destroyed" },
    { a: [], k: [], r: [], lbl: "Destruction stops — containment building" },
    { a: [], k: [], r: [], lbl: "Front arrested — CONTAINED" },
    { a: [], k: [], r: L[0] || [], lbl: "Center recovering" },
    { a: [], k: [], r: L[1] || [], lbl: "Ring 1 recovering" },
    { a: [], k: [], r: [], clr: true, lbl: "Baseline restored" },
  ];
}

// SVG scaling
const SC = 30;
function px(v) { return v * SC; }

const VB = (() => {
  let x0 = 1e9, x1 = -1e9, y0 = 1e9, y1 = -1e9;
  NODES.forEach(n => { x0 = Math.min(x0, n.x); x1 = Math.max(x1, n.x); y0 = Math.min(y0, n.y); y1 = Math.max(y1, n.y); });
  const p = 1.5;
  return (px(x0 - p)) + " " + (px(y0 - p)) + " " + (px(x1 - x0 + p * 2)) + " " + (px(y1 - y0 + p * 2));
})();

// Sparkline
function Spark({ hist }) {
  if (!hist || hist.length < 2) return null;
  const mx = Math.max(...hist, 1);
  const pts = hist.map((v, i) => (i / (hist.length - 1) * 50) + "," + (14 - v / mx * 12)).join(" ");
  return (
    <svg width={50} height={14} style={{ verticalAlign: "middle", marginLeft: 4 }}>
      <polyline points={pts} fill="none" stroke="#CD3333" strokeWidth="1.5" />
    </svg>
  );
}

export default function EGESSDemo() {
  const [nodes, setNodes] = useState(() => {
    const o = {};
    NODES.forEach(n => { o[n.port] = mkNode(); });
    return o;
  });
  const [cycle, setCycle] = useState(0);
  const [running, setRunning] = useState(false);
  const [demo, setDemo] = useState(null);
  const [dStep, setDStep] = useState(0);
  const [label, setLabel] = useState("Select a demo to begin");
  const [speed, setSpeed] = useState(3500);
  const [zoom, setZoom] = useState(1.0);
  const [draggingMap, setDraggingMap] = useState(null);
  const [sel, setSel] = useState(null);
  const [inspOpen, setInspOpen] = useState(true);
  const [refOpen, setRefOpen] = useState(false);

  const subRef = useRef(0);
  const dRef = useRef({ demo, dStep });
  const leftMapRef = useRef(null);
  const rightMapRef = useRef(null);
  const dragRef = useRef({
    active: false,
    target: null,
    startX: 0,
    startY: 0,
    scrollLeft: 0,
    scrollTop: 0,
    moved: false,
    suppressClick: false
  });
  dRef.current = { demo, dStep };

  const scripts = useMemo(() => ({ corner: cornerScript(), tornado: tornadoScript() }), []);

  const startMapDrag = useCallback((target, e) => {
    const el = target === "left" ? leftMapRef.current : rightMapRef.current;
    if (!el) return;
    dragRef.current = {
      active: true,
      target,
      startX: e.clientX,
      startY: e.clientY,
      scrollLeft: el.scrollLeft,
      scrollTop: el.scrollTop,
      moved: false,
      suppressClick: false
    };
    setDraggingMap(target);
  }, []);

  const moveMapDrag = useCallback((target, e) => {
    const st = dragRef.current;
    if (!st.active || st.target !== target) return;
    const el = target === "left" ? leftMapRef.current : rightMapRef.current;
    if (!el) return;
    const dx = e.clientX - st.startX;
    const dy = e.clientY - st.startY;
    if (Math.abs(dx) > 3 || Math.abs(dy) > 3) st.moved = true;
    el.scrollLeft = st.scrollLeft - dx;
    el.scrollTop = st.scrollTop - dy;
  }, []);

  const endMapDrag = useCallback((target) => {
    const st = dragRef.current;
    if (!st.active || st.target !== target) return;
    st.active = false;
    if (st.moved) st.suppressClick = true;
    setDraggingMap(null);
  }, []);

  const handleNodeSelect = useCallback((port) => {
    if (dragRef.current.suppressClick) {
      dragRef.current.suppressClick = false;
      return;
    }
    setSel(port);
  }, []);

  const resetAll = useCallback(() => {
    setRunning(false); setDemo(null); setDStep(0); setCycle(0); subRef.current = 0;
    setDraggingMap(null);
    dragRef.current = { active: false, target: null, startX: 0, startY: 0, scrollLeft: 0, scrollTop: 0, moved: false, suppressClick: false };
    setLabel("Select a demo to begin"); setSel(null);
    const o = {};
    NODES.forEach(n => { o[n.port] = mkNode(); });
    setNodes(o);
  }, []);

  const applyStep = useCallback((step, cur) => {
    const nx = {};
    Object.keys(cur).forEach(k => { nx[k] = { ...cur[k] }; });
    // CRITICAL: Reset all online nodes' sensors to NORMAL first.
    // Without this, nodes set to ALERT in a previous step stay ALERT forever,
    // which prevents noProg from accumulating and CONTAINED from ever triggering.
    Object.keys(nx).forEach(k => {
      if (nx[k].online && nx[k].sensor === "ALERT") {
        nx[k] = { ...nx[k], sensor: "NORMAL" };
      }
    });
    // Now apply this step's specific injections
    (step.k || []).forEach(p => { if (nx[p]) nx[p] = { ...nx[p], online: false, sensor: "NORMAL" }; });
    (step.a || []).forEach(p => { if (nx[p] && nx[p].online) nx[p] = { ...nx[p], sensor: "ALERT" }; });
    (step.r || []).forEach(p => { if (nx[p]) nx[p] = { ...nx[p], online: true, sensor: "RECOVERING" }; });
    if (step.clr) NODES.forEach(n => { nx[n.port] = mkNode(); });
    return nx;
  }, []);

  const tick = useCallback(() => {
    subRef.current += 1;
    const adv = subRef.current % 2 === 1;
    setNodes(prev => {
      const cur = {};
      Object.keys(prev).forEach(k => { cur[k] = { ...prev[k] }; });
      const dd = dRef.current;
      if (adv && dd.demo && scripts[dd.demo] && dd.dStep < scripts[dd.demo].length) {
        const step = scripts[dd.demo][dd.dStep];
        const applied = applyStep(step, cur);
        setLabel(step.lbl);
        setDStep(dd.dStep + 1);
        const upd = {};
        Object.keys(applied).forEach(k => { upd[k] = pullNode(+k, applied); });
        return upd;
      }
      const upd = {};
      Object.keys(cur).forEach(k => { upd[k] = pullNode(+k, cur); });
      return upd;
    });
    setCycle(c => c + 1);
  }, [applyStep, scripts]);

  useEffect(() => {
    if (!running) return;
    const iv = setInterval(tick, speed);
    return () => clearInterval(iv);
  }, [running, speed, tick]);

  const startDemo = useCallback(w => {
    resetAll();
    setTimeout(() => { setDemo(w); setDStep(0); setRunning(true); }, 100);
  }, [resetAll]);

  const nd = sel !== null ? nodes[sel] : null;

  const missing = useMemo(() => {
    return Object.entries(nodes).filter(([, n]) => !n.online).map(([p]) => +p);
  }, [nodes]);

  const counts = useMemo(() => {
    const c = { NORMAL: 0, WATCH: 0, WARNING: 0, IMPACT: 0, CONTAINED: 0, RECOVERING: 0, OFFLINE: 0 };
    Object.values(nodes).forEach(n => { if (!n.online) c.OFFLINE++; else c[n.state]++; });
    return c;
  }, [nodes]);

  const edges = useMemo(() => {
    const e = [];
    const seen = new Set();
    NODES.forEach(n => {
      const d = nodes[n.port];
      if (!d.online) return;
      (d.known || []).forEach(np => {
        const k = Math.min(n.port, np) + "-" + Math.max(n.port, np);
        if (seen.has(k)) return;
        seen.add(k);
        if (!nodes[np] || !nodes[np].online) return;
        e.push({ a: n.port, b: np, mut: (nodes[np].known || []).includes(n.port) });
      });
    });
    return e;
  }, [nodes]);

  // ─── Live Metrics ───
  const metrics = useMemo(() => {
    const vals = Object.values(nodes);
    const offlineNodes = vals.filter(n => !n.online);
    const watchPlus = vals.filter(n => n.online && n.state !== S.NORMAL);
    const frontNodes = vals.filter(n => n.online && n.dom === "front");
    const impactNodes = vals.filter(n => n.online && n.dom === "impact");
    const arrestNodes = vals.filter(n => n.online && n.dom === "arrest");

    // Detection latency: among nodes adjacent to offline nodes, avg cycles with T > 0
    let detectionSum = 0, detectionCount = 0;
    offlineNodes.forEach(offN => {
      const offPort = Object.entries(nodes).find(([, v]) => v === offN);
      if (!offPort) return;
      const port = +offPort[0];
      (NBRS[port] || []).forEach(np => {
        const nb = nodes[np];
        if (nb && nb.online && nb.T > 0) {
          // This neighbor detected something — how many cycles has it known?
          const histLen = (nb.tHist || []).filter(t => t > 0).length;
          if (histLen > 0) { detectionSum += histLen; detectionCount++; }
        }
      });
    });
    const detectionLatency = detectionCount > 0 ? Math.round((detectionSum / detectionCount) * 10) / 10 : 0;

    // False positive: nodes with T > 0 that have zero missing neighbors and no missing neighbors' neighbors
    let falsePos = 0;
    vals.forEach(n => {
      if (!n.online || n.T === 0) return;
      const port = Object.entries(nodes).find(([, v]) => v === n);
      if (!port) return;
      const p = +port[0];
      const hasNearbyDamage = (NBRS[p] || []).some(np => !nodes[np].online) ||
        (NBRS[p] || []).some(np => nodes[np].online && nodes[np].T >= 3);
      if (!hasNearbyDamage) falsePos++;
    });
    const fpRate = watchPlus.length > 0 ? Math.round(falsePos / Math.max(1, watchPlus.length) * 100) : 0;

    // Spatial accuracy: % of elevated nodes that correctly neighbor offline nodes
    let correctRole = 0, totalElevated = 0;
    vals.forEach(n => {
      if (!n.online || n.dom === "none") return;
      totalElevated++;
      const port = Object.entries(nodes).find(([, v]) => v === n);
      if (!port) return;
      const p = +port[0];
      const nbrsOff = (NBRS[p] || []).filter(np => !nodes[np].online).length;
      if (n.dom === "impact" && nbrsOff >= 2) correctRole++;
      else if (n.dom === "front" && nbrsOff >= 1 && nbrsOff <= 2) correctRole++;
      else if (n.dom === "arrest" && nbrsOff >= 1) correctRole++;
    });
    const spatialAcc = totalElevated > 0 ? Math.round(correctRole / totalElevated * 100) : 100;

    // Recovery: cycles since last offline node, if all back
    const allOnline = offlineNodes.length === 0;
    const anyElevated = watchPlus.length > 0;

    // Hazard detection summary
    let hazardStatus = "CLEAR";
    let hazardDesc = "No hazard activity detected";
    let hazardColor = "#2E7D32";
    const nOff = offlineNodes.length;
    const nFront = frontNodes.length;
    const nImpact = impactNodes.length;
    const nArrest = arrestNodes.length;

    // Count by PROTOCOL STATE (more reliable than dominant score for status)
    const nRecState = vals.filter(n => n.online && n.state === S.RECOVERING).length;
    const nContState = vals.filter(n => n.online && n.state === S.CONTAINED).length;
    const nImpState = vals.filter(n => n.online && n.state === S.IMPACT).length;
    const nWarnState = vals.filter(n => n.online && (n.state === S.WARNING || n.state === S.WATCH)).length;

    if (nRecState > 0 && nImpState === 0 && nOff <= nRecState) {
      // Recovery is dominant — more recovering nodes than remaining offline
      hazardStatus = "RECOVERING";
      hazardDesc = "Recovery underway — " + nRecState + " nodes recovering" + (nOff > 0 ? ", " + nOff + " still offline" : ", all back online");
      hazardColor = "#3CB371";
    } else if (nContState > 0 && nImpState === 0) {
      hazardStatus = "CONTAINED";
      hazardDesc = "Front arrested — " + nContState + " at containment boundary, " + nOff + " still offline";
      hazardColor = "#8B668B";
    } else if (nOff > 0 && nFront > 0 && nImpact === 0 && nImpState === 0) {
      hazardStatus = "APPROACHING";
      hazardDesc = "Front detected — " + nOff + " nodes offline, " + nFront + " sensing approach";
      hazardColor = "#DAA520";
    } else if (nImpState > 0 && nFront > 0) {
      hazardStatus = "ACTIVE SPREAD";
      hazardDesc = "Hazard spreading — " + nOff + " offline, " + nImpState + " in damage zone, " + nFront + " on leading edge";
      hazardColor = "#CD3333";
    } else if (nImpState > 0 && nFront === 0) {
      hazardStatus = "LOCAL IMPACT";
      hazardDesc = "Localized damage — " + nOff + " offline, " + nImpState + " impacted, not spreading";
      hazardColor = "#CD3333";
    } else if (nOff === 0 && anyElevated) {
      hazardStatus = "RECOVERING";
      hazardDesc = "All nodes back online, " + watchPlus.length + " still elevated — scores decaying";
      hazardColor = "#3CB371";
    } else if (nOff > 0 && nWarnState === 0 && nImpState === 0) {
      hazardStatus = "OFFLINE";
      hazardDesc = nOff + " nodes offline — signal still propagating";
      hazardColor = "#888";
    }

    return {
      nOff, nFront, nImpact, nArrest,
      detectionLatency, fpRate, spatialAcc,
      allOnline, anyElevated,
      hazardStatus, hazardDesc, hazardColor
    };
  }, [nodes]);

  const btnBase = { padding: "3px 10px", fontSize: "10px", fontFamily: "monospace", border: "1px solid #bbb", borderRadius: "3px", cursor: "pointer" };
  const zoomLevels = [0.75, 1.0, 1.25, 1.5];

  return (
    <div style={{ minHeight: "100vh", display: "flex", flexDirection: "column", background: "#FDFDFD", color: "#1a1a1a", fontFamily: "Georgia, serif", fontSize: "11px", overflowY: "auto", overflowX: "hidden" }}>

      {/* Title */}
      <div style={{ padding: "8px 16px 6px", borderBottom: "2px solid #222", display: "flex", justifyContent: "space-between", alignItems: "baseline", flexShrink: 0, background: "#fff" }}>
        <div>
          <span style={{ fontSize: "17px", fontWeight: "bold" }}>EGESS</span>
          <span style={{ fontSize: "11px", color: "#666", marginLeft: 8, fontFamily: "monospace" }}>Experimental Gear for Evaluation of Swarm Systems</span>
        </div>
        <span style={{ fontSize: "9px", color: "#aaa", fontFamily: "monospace" }}>2026 N. Ivanov / ACSUS Lab</span>
      </div>

      {/* Controls */}
      <div style={{ padding: "5px 16px", background: "#f8f8f8", borderBottom: "1px solid #ddd", display: "flex", gap: 5, alignItems: "center", flexShrink: 0, flexWrap: "wrap" }}>
        <button style={{ ...btnBase, background: "#4682B4", color: "#fff" }} onClick={() => startDemo("corner")}>Corner Spread</button>
        <button style={{ ...btnBase, background: "#CD3333", color: "#fff" }} onClick={() => startDemo("tornado")}>Tornado</button>
        <button style={{ ...btnBase, background: "#f0f0f0", color: "#333" }} onClick={() => setRunning(!running)}>{running ? "⏸ Pause" : "▶ Play"}</button>
        <button style={{ ...btnBase, background: "#f0f0f0", color: "#333" }} onClick={tick}>Step</button>
        <button style={{ ...btnBase, background: "#f0f0f0", color: "#333" }} onClick={resetAll}>Reset</button>
        {sel !== null && <>
          <span style={{ color: "#ccc" }}>|</span>
          <button style={{ ...btnBase, background: "#f0f0f0" }} onClick={() => setNodes(p => ({ ...p, [sel]: { ...p[sel], online: false, sensor: "NORMAL" } }))}>Kill</button>
          <button style={{ ...btnBase, background: "#f0f0f0" }} onClick={() => setNodes(p => ({ ...p, [sel]: { ...p[sel], online: true, sensor: "ALERT" } }))}>Alert</button>
          <button style={{ ...btnBase, background: "#f0f0f0" }} onClick={() => setNodes(p => ({ ...p, [sel]: { ...p[sel], online: true, sensor: "RECOVERING" } }))}>Recover</button>
          <button style={{ ...btnBase, background: "#f0f0f0" }} onClick={() => setNodes(p => ({ ...p, [sel]: mkNode() }))}>Normal</button>
        </>}
        <span style={{ color: "#ccc" }}>|</span>
        <span style={{ fontSize: "9px", color: "#888", fontFamily: "monospace" }}>Zoom:</span>
        {zoomLevels.map(level => (
          <button
            key={level}
            style={{
              ...btnBase,
              padding: "3px 7px",
              background: zoom === level ? "#222" : "#f0f0f0",
              color: zoom === level ? "#fff" : "#333",
              borderColor: zoom === level ? "#222" : "#bbb"
            }}
            onClick={() => setZoom(level)}
          >
            {level.toFixed(2)}x
          </button>
        ))}
        <span style={{ fontSize: "9px", color: "#999", fontFamily: "monospace" }}>drag maps to pan</span>
        <div style={{ flex: 1 }} />
        <span style={{ fontSize: "9px", color: "#888", fontFamily: "monospace" }}>Speed:</span>
        <input type="range" min={1000} max={6000} step={500} value={speed} onChange={e => setSpeed(+e.target.value)} style={{ width: 60 }} />
        <span style={{ fontSize: "9px", color: "#999", fontFamily: "monospace" }}>{(speed / 1000).toFixed(1)}s</span>
      </div>

      {/* Phase */}
      <div style={{ padding: "4px 16px", background: "#fff", borderBottom: "1px solid #eee", display: "flex", justifyContent: "space-between", alignItems: "center", flexShrink: 0, fontFamily: "monospace", fontSize: "10px" }}>
        <span><b>Cycle {cycle}</b> — {label}</span>
        <div style={{ display: "flex", gap: 10 }}>
          {Object.entries(counts).filter(([, v]) => v > 0).map(([k, v]) => (
            <span key={k}>
              <span style={{ display: "inline-block", width: 8, height: 8, background: FILL[k], border: "1px solid #555", borderRadius: 1, marginRight: 3, verticalAlign: "middle" }} />
              {k}:{v}
            </span>
          ))}
        </div>
      </div>

      {/* Hazard Detection + Metrics */}
      <div style={{ padding: "4px 16px", background: "#f9f9f5", borderBottom: "1px solid #ddd", display: "flex", justifyContent: "space-between", alignItems: "center", flexShrink: 0, fontFamily: "monospace", fontSize: "10px", gap: 12 }}>
        {/* Hazard status */}
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <div style={{ width: 8, height: 8, borderRadius: "50%", background: metrics.hazardColor, boxShadow: metrics.hazardStatus !== "CLEAR" ? "0 0 6px " + metrics.hazardColor : "none" }} />
          <span style={{ fontWeight: "bold", color: metrics.hazardColor }}>{metrics.hazardStatus}</span>
          <span style={{ color: "#888", fontSize: "9px" }}>{metrics.hazardDesc}</span>
        </div>

        {/* Live metrics */}
        <div style={{ display: "flex", gap: 14, fontSize: "9px", color: "#555" }}>
          <div style={{ textAlign: "center" }}>
            <div style={{ fontSize: "12px", fontWeight: "bold", color: metrics.detectionLatency <= 2 ? "#2E7D32" : metrics.detectionLatency <= 4 ? "#DAA520" : "#CD3333" }}>
              {metrics.detectionLatency || "—"}
            </div>
            <div style={{ color: "#999", fontSize: "7px" }}>detect cyc</div>
          </div>
          <div style={{ textAlign: "center" }}>
            <div style={{ fontSize: "12px", fontWeight: "bold", color: metrics.fpRate <= 5 ? "#2E7D32" : metrics.fpRate <= 20 ? "#DAA520" : "#CD3333" }}>
              {metrics.fpRate}%
            </div>
            <div style={{ color: "#999", fontSize: "7px" }}>false pos</div>
          </div>
          <div style={{ textAlign: "center" }}>
            <div style={{ fontSize: "12px", fontWeight: "bold", color: metrics.spatialAcc >= 80 ? "#2E7D32" : metrics.spatialAcc >= 50 ? "#DAA520" : "#CD3333" }}>
              {metrics.spatialAcc}%
            </div>
            <div style={{ color: "#999", fontSize: "7px" }}>spatial acc</div>
          </div>
          <div style={{ textAlign: "center" }}>
            <div style={{ fontSize: "12px", fontWeight: "bold", color: "#555" }}>
              {metrics.nFront}
            </div>
            <div style={{ color: "#999", fontSize: "7px" }}>front</div>
          </div>
          <div style={{ textAlign: "center" }}>
            <div style={{ fontSize: "12px", fontWeight: "bold", color: "#555" }}>
              {metrics.nImpact}
            </div>
            <div style={{ color: "#999", fontSize: "7px" }}>impact</div>
          </div>
          <div style={{ textAlign: "center" }}>
            <div style={{ fontSize: "12px", fontWeight: "bold", color: "#555" }}>
              {metrics.nArrest}
            </div>
            <div style={{ color: "#999", fontSize: "7px" }}>arrest</div>
          </div>
        </div>
      </div>

      {/* Maps */}
      <div style={{ flex: 1, display: "flex", overflow: "hidden", minHeight: 420 }}>
        {/* LEFT: Gossip */}
        <div style={{ flex: 1, display: "flex", flexDirection: "column", borderRight: "1px solid #bbb" }}>
          <div style={{ padding: "3px 10px", fontSize: "10px", color: "#555", background: "#fafafa", borderBottom: "1px solid #e0e0e0", flexShrink: 0 }}>
            <b>Figure A</b> — Gossip View
          </div>
          <div
            ref={leftMapRef}
            style={{
              flex: 1,
              background: "#fff",
              display: "flex",
              justifyContent: "center",
              alignItems: "center",
              padding: 4,
              overflow: "auto",
              cursor: draggingMap === "left" ? "grabbing" : "grab",
              userSelect: "none"
            }}
            onMouseDown={e => startMapDrag("left", e)}
            onMouseMove={e => moveMapDrag("left", e)}
            onMouseUp={() => endMapDrag("left")}
            onMouseLeave={() => endMapDrag("left")}
          >
            <div style={{ width: `${zoom * 100}%`, height: `${zoom * 100}%`, flexShrink: 0 }}>
            <svg viewBox={VB} style={{ width: "100%", height: "100%" }}>
              {edges.map(e => {
                const na = NODES.find(n => n.port === e.a);
                const nb = NODES.find(n => n.port === e.b);
                if (!na || !nb) return null;
                const hl = sel === e.a || sel === e.b;
                return (
                  <line key={e.a + "-" + e.b}
                    x1={px(na.x)} y1={px(na.y)} x2={px(nb.x)} y2={px(nb.y)}
                    stroke={hl ? "#444" : "#9BB8D3"}
                    strokeWidth={e.mut ? (hl ? 2 : 1) : (hl ? 1.2 : 0.35)}
                    opacity={hl ? 0.8 : (e.mut ? 0.4 : 0.18)} />
                );
              })}
              {NODES.map(n => {
                const d = nodes[n.port];
                const off = !d.online;
                const sk = off ? "OFFLINE" : d.state;
                const isSel = sel === n.port;
                const cx = px(n.x), cy = px(n.y), sz = SC * 0.9;
                return (
                  <g key={n.port} onClick={() => handleNodeSelect(n.port)} style={{ cursor: "pointer" }}>
                    <polygon points={hexPts(cx, cy, sz)} fill={FILL[sk]}
                      stroke={isSel ? "#000" : "#333"} strokeWidth={isSel ? 2.5 : 0.6}
                      opacity={off ? 0.3 : 1} />
                    {off && <>
                      <line x1={cx - sz * .35} y1={cy - sz * .35} x2={cx + sz * .35} y2={cy + sz * .35} stroke="#444" strokeWidth={1.5} />
                      <line x1={cx + sz * .35} y1={cy - sz * .35} x2={cx - sz * .35} y2={cy + sz * .35} stroke="#444" strokeWidth={1.5} />
                    </>}
                    <text x={cx} y={cy + 1} textAnchor="middle" dominantBaseline="middle"
                      fill={off ? "#666" : (sk === "WATCH" || sk === "WARNING") ? "#111" : "#fff"}
                      fontSize="7.5" fontFamily="monospace" fontWeight={isSel ? "bold" : "normal"}>
                      {n.port}
                    </text>
                  </g>
                );
              })}
            </svg>
            </div>
          </div>
        </div>

        {/* RIGHT: Threshold Map */}
        <div style={{ flex: 1, display: "flex", flexDirection: "column" }}>
          <div style={{ padding: "3px 10px", fontSize: "10px", color: "#555", background: "#fafafa", borderBottom: "1px solid #e0e0e0", flexShrink: 0 }}>
            <b>Figure B</b> — Threshold Map
          </div>
          <div
            ref={rightMapRef}
            style={{
              flex: 1,
              background: "#fff",
              display: "flex",
              justifyContent: "center",
              alignItems: "center",
              padding: 4,
              overflow: "auto",
              cursor: draggingMap === "right" ? "grabbing" : "grab",
              userSelect: "none"
            }}
            onMouseDown={e => startMapDrag("right", e)}
            onMouseMove={e => moveMapDrag("right", e)}
            onMouseUp={() => endMapDrag("right")}
            onMouseLeave={() => endMapDrag("right")}
          >
            <div style={{ width: `${zoom * 100}%`, height: `${zoom * 100}%`, flexShrink: 0 }}>
            <svg viewBox={VB} style={{ width: "100%", height: "100%" }}>
              {NODES.map(n => {
                const d = nodes[n.port];
                const off = !d.online;
                const sk = off ? "OFFLINE" : d.state;
                const isSel = sel === n.port;
                const cx = px(n.x), cy = px(n.y), sz = SC * 0.9;
                const tc = off ? "#777" : (sk === "WATCH" || sk === "WARNING") ? "#111" : "#fff";
                const oc = d.T >= 10 ? "#8B0000" : d.T >= 6 ? "#B8600B" : d.T >= 3 ? "#8B8000" : "#444";
                const ow = d.T >= 10 ? 2.5 : d.T >= 6 ? 2 : d.T >= 3 ? 1.5 : 0.6;
                return (
                  <g key={n.port} onClick={() => handleNodeSelect(n.port)} style={{ cursor: "pointer" }}>
                    <polygon points={hexPts(cx, cy, sz)} fill={FILL[sk]}
                      stroke={isSel ? "#000" : oc} strokeWidth={isSel ? 3 : ow}
                      opacity={off ? 0.3 : 1} />
                    {off && <>
                      <line x1={cx - sz * .35} y1={cy - sz * .35} x2={cx + sz * .35} y2={cy + sz * .35} stroke="#444" strokeWidth={1.5} />
                      <line x1={cx + sz * .35} y1={cy - sz * .35} x2={cx - sz * .35} y2={cy + sz * .35} stroke="#444" strokeWidth={1.5} />
                    </>}
                    <text x={cx} y={cy - sz * .33} textAnchor="middle" dominantBaseline="middle"
                      fill={tc} fontSize="5.5" fontFamily="monospace" opacity={.6}>{n.port}</text>
                    {!off && (
                      <text x={cx} y={cy + sz * .08} textAnchor="middle" dominantBaseline="middle"
                        fill={tc} fontSize="12" fontFamily="monospace" fontWeight="bold">
                        {d.T === 0 ? "0" : d.T % 1 === 0 ? d.T.toFixed(0) : d.T.toFixed(1)}
                      </text>
                    )}
                  </g>
                );
              })}
            </svg>
            </div>
          </div>
          <div style={{ padding: "2px 10px", fontSize: "8px", color: "#999", borderTop: "1px solid #eee", flexShrink: 0, fontFamily: "monospace", background: "#fafafa" }}>
            Missing: [{missing.join(", ") || "none"}]
          </div>
        </div>
      </div>

      {/* Legend */}
      <div style={{ display: "flex", gap: 14, padding: "4px 16px", background: "#fafafa", borderTop: "1px solid #ddd", borderBottom: "1px solid #ddd", justifyContent: "center", flexShrink: 0 }}>
        {[["NORMAL", "Normal"], ["WATCH", "Watch ≥3"], ["WARNING", "Warning ≥6"], ["IMPACT", "Impact ≥10"], ["CONTAINED", "Contained"], ["RECOVERING", "Recovering"], ["OFFLINE", "Offline"]].map(([k, l]) => (
          <div key={k} style={{ display: "flex", alignItems: "center", gap: 4 }}>
            <div style={{ width: 10, height: 10, background: FILL[k], border: "1px solid #555", borderRadius: 1 }} />
            <span style={{ fontSize: "9px", color: "#555", fontFamily: "monospace" }}>{l}</span>
          </div>
        ))}
      </div>

      {/* Scoring Reference */}
      <div style={{ flexShrink: 0, borderTop: "1px solid #ccc", background: "#fff", overflow: "hidden" }}>
        <div onClick={() => setRefOpen(!refOpen)} style={{ padding: "3px 16px", cursor: "pointer", display: "flex", justifyContent: "space-between", alignItems: "center", background: "#f7f7f0", borderBottom: refOpen ? "1px solid #ddd" : "none" }}>
          <span style={{ fontSize: "11px", fontWeight: "bold" }}>{refOpen ? "▼" : "▶"} Scoring Reference — How T is computed</span>
          <span style={{ fontSize: "9px", color: "#999", fontFamily: "monospace" }}>T = max(FrontScore, ImpactScore)</span>
        </div>
        {refOpen && (
          <div style={{ padding: "8px 16px", display: "flex", gap: 20, fontSize: "10px", fontFamily: "monospace", overflowX: "auto" }}>
            {/* Column 1: Pull signals */}
            <div style={{ minWidth: 200 }}>
              <div style={{ fontWeight: "bold", fontSize: "11px", marginBottom: 4, color: "#333" }}>Pull Signals (per neighbor)</div>
              <div style={{ ...refRow, color: "#8B0000" }}><span style={refBadge}>+3</span> Missing neighbor (in ImpactScore)</div>
              <div style={refHint}>Neighbor didn't respond to pull — offline.</div>
              <div style={{ ...refRow, color: "#8B0000" }}><span style={refBadge}>+2</span> New missing (in FrontScore)</div>
              <div style={refHint}>Just went offline THIS cycle. Strongest front signal.</div>
              <div style={{ ...refRow, color: "#8B4513" }}><span style={{ ...refBadge, background: "#FFF3E0" }}>+0.5</span> Persistent missing (in FrontScore)</div>
              <div style={refHint}>Was already missing last cycle. Old news, counts less.</div>
              <div style={{ ...refRow, color: "#8B6914" }}><span style={{ ...refBadge, background: "#FFF8E1" }}>+1</span> Disagreement (capped at 2)</div>
              <div style={refHint}>Neighbor has T≥3 and T {">"} myT+1. They see danger I don't yet.</div>
              <div style={{ ...refRow, color: "#2E7D32" }}><span style={{ ...refBadge, background: "#E8F5E9" }}>→</span> Recovered neighbor</div>
              <div style={refHint}>Was missing, now back. Triggers CONTAINED → RECOVERING.</div>
            </div>

            {/* Column 2: Sub-scores */}
            <div style={{ minWidth: 240 }}>
              <div style={{ fontWeight: "bold", fontSize: "11px", marginBottom: 6, color: "#333" }}>Three Questions Every Node Asks</div>
              
              <div style={{ ...refRow, color: "#8B6914" }}><span style={{ ...refBadge, background: "#FFF8E1" }}>1</span> <b>FrontScore</b> — "Is danger coming toward me?"</div>
              <div style={refHint}>Looks at NEW disappearances. If neighbors just started going offline and others are warning me, the front is approaching. Higher = closer.</div>
              <div style={{ fontSize: "9px", color: "#888", padding: "2px 0 4px 34px" }}>= 2 × (just went offline) + 1 × (neighbors warning me) + 0.5 × (still offline from before)</div>

              <div style={{ ...refRow, color: "#CD3333", marginTop: 6 }}><span style={{ ...refBadge, background: "#FFEBEE" }}>2</span> <b>ImpactScore</b> — "How bad is it around me right now?"</div>
              <div style={refHint}>Counts ALL offline neighbors, not just new ones. If 3 of your 6 neighbors are gone, that's severe regardless of when they went down.</div>
              <div style={{ fontSize: "9px", color: "#888", padding: "2px 0 4px 34px" }}>= 3 × (total offline neighbors) + 2 × (if they're clustered together)</div>

              <div style={{ ...refRow, color: "#8B668B", marginTop: 6 }}><span style={{ ...refBadge, background: "#F3E5F5" }}>3</span> <b>ArrestScore</b> — "Has it stopped spreading?"</div>
              <div style={refHint}>Checks if no new damage has happened for 3+ cycles, and if neighbors are starting to come back. Positive = the front stalled.</div>
              <div style={{ fontSize: "9px", color: "#888", padding: "2px 0 4px 34px" }}>= (stalled?) + (recovering?) − (spreading sideways?)</div>

              <div style={{ marginTop: 8, padding: "4px 8px", background: "#f9f9f0", border: "1px solid #e8e8d8", borderRadius: 3 }}>
                <div style={{ fontWeight: "bold" }}>T = whichever is higher: FrontScore or ImpactScore</div>
                <div style={{ fontSize: "8px", color: "#888", marginTop: 2 }}>One number that says "how bad is it?" — the worst of approaching danger or current damage.</div>
              </div>

              <div style={{ display: "flex", gap: 12, marginTop: 6 }}>
                <div><span style={{ ...refBadge, background: "#E3F2FD" }}>+2</span> <span style={{ fontSize: "9px" }}>momentum</span></div>
                <div style={{ fontSize: "8px", color: "#888", paddingTop: 2 }}>If T was already up and signal continues, add 2. Keeps score from flickering.</div>
              </div>
              <div style={{ display: "flex", gap: 12, marginTop: 2 }}>
                <div><span style={{ ...refBadge, background: "#E8F5E9" }}>−1</span> <span style={{ fontSize: "9px" }}>decay</span></div>
                <div style={{ fontSize: "8px", color: "#888", paddingTop: 2 }}>When nothing is wrong, T drops by 1 each cycle until back to 0.</div>
              </div>
            </div>

            {/* Column 3: Thresholds + Slope + Coherence */}
            <div style={{ minWidth: 220 }}>
              <div style={{ fontWeight: "bold", fontSize: "11px", marginBottom: 6, color: "#333" }}>What T Means (State Thresholds)</div>
              <div style={refRow}><span style={{ ...refBadge, background: FILL.NORMAL, color: "#fff" }}>0</span> NORMAL — everything is fine</div>
              <div style={refRow}><span style={{ ...refBadge, background: FILL.WATCH, color: "#000" }}>≥3</span> WATCH — pay attention, could be something</div>
              <div style={refRow}><span style={{ ...refBadge, background: FILL.WARNING, color: "#000" }}>≥6</span> WARNING — real danger approaching</div>
              <div style={refRow}><span style={{ ...refBadge, background: FILL.IMPACT, color: "#fff" }}>≥10</span> IMPACT — you're in the damage zone</div>
              <div style={refHint}>After impact: purple = front stopped here (confirmed by flat/declining slope), green = recovering, blue = normal.</div>

              <div style={{ fontWeight: "bold", fontSize: "11px", marginTop: 8, marginBottom: 4, color: "#333" }}>Slope — "Is it getting worse or better?"</div>
              <div style={refHint}>T is a snapshot. Slope is the trend. Same T, different slope = completely different situation.</div>
              <div style={refRow}><span style={{ ...refBadge, background: "#FFEBEE" }}>▲▲</span> Rising fast — front approaching quickly. Can skip WATCH → WARNING.</div>
              <div style={refRow}><span style={{ ...refBadge, background: "#FFF3E0" }}>▲</span> Rising — things are getting worse each cycle.</div>
              <div style={refRow}><span style={{ ...refBadge, background: "#FAFAFA" }}>—</span> Flat/declining + stalled → CONTAINED. Front stopped here (yellow → purple).</div>
              <div style={refRow}><span style={{ ...refBadge, background: "#E8F5E9" }}>▼</span> Falling — situation improving, recovery underway.</div>

              <div style={{ fontWeight: "bold", fontSize: "11px", marginTop: 8, marginBottom: 4, color: "#333" }}>Coherence — "Is this real or random?"</div>
              <div style={refHint}>Before a node escalates, 2 of these 3 must be true. This is how we tell a real spreading hazard from two random crashes.</div>
              <div style={refRow}>[?] <b>Adjacent</b> — are the missing nodes next to each other?</div>
              <div style={refRow}>[?] <b>Persistent</b> — same direction for multiple cycles?</div>
              <div style={refRow}>[?] <b>Progressing</b> — new damage moving outward?</div>
              <div style={refHint}>If only 1 of 3 is true → probably coincidence → don't escalate.</div>
            </div>
          </div>
        )}
      </div>

      {/* Inspector */}
      <div style={{ flexShrink: 0, borderTop: "2px solid #333", background: "#fafafa", overflow: "hidden", maxHeight: inspOpen ? 300 : 24 }}>
        <div onClick={() => setInspOpen(!inspOpen)} style={{ padding: "3px 16px", cursor: "pointer", display: "flex", justifyContent: "space-between", alignItems: "center", background: "#f0f0f0", borderBottom: inspOpen ? "1px solid #ddd" : "none" }}>
          <span style={{ fontSize: "11px", fontWeight: "bold" }}>
            {inspOpen ? "▼" : "▶"} Node Inspector {sel !== null ? " — Node " + sel : ""}
          </span>
          <span style={{ fontSize: "9px", color: "#999", fontFamily: "monospace" }}>
            {nd ? (nd.online ? nd.state : "OFFLINE") + " | T=" + nd.T + " | slope=" + (nd.slope > 0 ? "+" : "") + nd.slope : "click any node"}
          </span>
        </div>

        {inspOpen && (
          <div style={{ display: "flex", overflow: "hidden", maxHeight: 270 }}>
            {/* LEFT: Pull Math */}
            <div style={{ flex: 1, padding: "6px 12px", overflowY: "auto", fontSize: "10px", fontFamily: "monospace", borderRight: "1px solid #ddd" }}>
              {nd ? (
                <div style={{ color: "#333", lineHeight: 1.6 }}>
                  <div style={{ fontWeight: "bold", fontSize: "11px", color: FILL[nd.online ? nd.state : "OFFLINE"], marginBottom: 4 }}>
                    Pull Results — Cycle {nd.cycles}
                  </div>
                  <div>Neighbors: <b>{(NBRS[sel] || []).length}</b> | Online: <b>{(NBRS[sel] || []).length - nd.pd.tm}</b></div>
                  <div style={hintStyle}>This node asks its 6 neighbors "are you there?" every cycle.</div>

                  <div style={cardStyle}>
                    <div style={{ color: "#8B0000" }}>New missing: [{nd.pd.nm.join(", ") || "—"}] = <b>{nd.pd.nm.length}</b></div>
                    <div style={hintStyle}>Just went offline this cycle. Strongest danger signal.</div>
                    <div style={{ color: "#8B4513", marginTop: 3 }}>Persistent missing: [{nd.pd.pm.join(", ") || "—"}] = <b>{nd.pd.pm.length}</b></div>
                    <div style={hintStyle}>Already gone last cycle and still gone.</div>
                    <div style={{ color: "#2E7D32", marginTop: 3 }}>Recovered: [{nd.pd.rc.join(", ") || "—"}] = <b>{nd.pd.rc.length}</b></div>
                    <div style={hintStyle}>Were missing, now back. Triggers recovery.</div>
                    <div style={{ color: "#8B6914", marginTop: 3 }}>Disagree: [{nd.pd.dg.join(", ") || "—"}] = <b>{nd.pd.dg.length}</b></div>
                    <div style={hintStyle}>Neighbors with T≥3 and T &gt; myT+1. Capped at 2 to prevent cascade — awareness spreads but can't escalate alone.</div>
                  </div>

                  <div style={cardStyle}>
                    <div><b>FrontScore</b> — "Is the hazard heading toward me?"</div>
                    <div style={hintStyle}>High when NEW neighbors disappear. Detects the leading edge.</div>
                    <div>= 2×{nd.pd.nm.length} + 1×min({nd.pd.dg.length}, 2) + 0.5×{nd.pd.pm.length} + mom({nd.T > 0 && (nd.pd.nm.length > 0 || nd.pd.dg.length > 0) ? 1 : 0}) = <b style={{ color: "#8B6914" }}>{nd.frontScore}</b></div>
                    <div style={hintStyle}>Disagreement capped at 2 max — {nd.pd.dg.length > 2 ? nd.pd.dg.length + " neighbors disagree but only 2 count" : "prevents cascade across the grid"}.</div>
                  </div>

                  <div style={cardStyle}>
                    <div><b>ImpactScore</b> — "How much damage around me?"</div>
                    <div style={hintStyle}>Counts ALL missing neighbors.</div>
                    <div>= 3×{nd.pd.tm} + 2×cluster({nd.cAdj}) = <b style={{ color: "#CD3333" }}>{nd.impactScore}</b></div>
                  </div>

                  <div style={{ ...cardStyle, background: "#fefef0" }}>
                    <div><b>T = max({nd.frontScore}, {nd.impactScore}) = {Math.max(nd.frontScore, nd.impactScore)}</b>
                      {nd.T !== Math.max(nd.frontScore, nd.impactScore) && <span style={{ color: "#888" }}> → {nd.T}</span>}
                    </div>
                    <div style={hintStyle}>{nd.frontScore === 0 && nd.impactScore === 0 ? (nd.T > 0 ? "No signal — decaying by 1." : "All clear.") : "T = whichever sub-score is higher."}</div>
                  </div>
                </div>
              ) : (
                <div style={{ color: "#bbb", padding: 8, lineHeight: 1.8 }}>
                  <div><b>How it works:</b></div>
                  <div>Every cycle, each node asks 6 neighbors "are you there?"</div>
                  <div>No reply = <b>missing</b>. Reply + high T = <b>disagreement</b>.</div>
                  <div><b>FrontScore</b> = approaching front. <b>ImpactScore</b> = local damage.</div>
                  <div><b>T</b> = max(Front, Impact) = severity number.</div>
                </div>
              )}
            </div>

            {/* RIGHT: Scores */}
            <div style={{ flex: 1, padding: "6px 12px", overflowY: "auto", fontSize: "10px", fontFamily: "monospace" }}>
              {nd ? (
                <div style={{ color: "#333", lineHeight: 1.6 }}>
                  <div style={{ fontWeight: "bold", fontSize: "11px", color: FILL[nd.online ? nd.state : "OFFLINE"], marginBottom: 4 }}>
                    Scores and State
                  </div>

                  <div style={cardStyle}>
                    <div>State: <b style={{ color: FILL[nd.online ? nd.state : "OFFLINE"], fontSize: "12px" }}>{nd.online ? nd.state : "OFFLINE"}</b>
                      {" | T: "}<b style={{ fontSize: "14px" }}>{nd.T}</b>
                      <Spark hist={nd.tHist} />
                      <span style={{ marginLeft: 6, fontSize: "10px", color: nd.T > nd.prevT ? "#8B0000" : nd.T < nd.prevT ? "#2E7D32" : "#888" }}>
                        (Δ{nd.T > nd.prevT ? "+" : ""}{(nd.T - nd.prevT).toFixed(1)})
                      </span>
                    </div>
                    <div style={hintStyle}>
                      {nd.state === S.NORMAL ? "All clear." : nd.state === S.WATCH ? "Possible approaching hazard." : nd.state === S.WARNING ? "Confirmed front approaching." : nd.state === S.IMPACT ? "In the damage zone." : nd.state === S.CONTAINED ? "Front stopped here. No new damage + slope flat or declining confirms containment." : nd.state === S.RECOVERING ? "Getting better." : "Offline."}
                    </div>
                  </div>

                  <div style={cardStyle}>
                    <div>Slope: <b style={{ fontSize: "12px", color: nd.slope >= 2 ? "#8B0000" : nd.slope >= 1 ? "#CD6600" : nd.slope <= -1 ? "#2E7D32" : "#555" }}>
                      {nd.slope > 0 ? "+" : ""}{nd.slope}/cyc {nd.slope >= 2 ? "▲▲" : nd.slope >= 1 ? "▲" : nd.slope <= -1 ? "▼" : ""}
                    </b></div>
                    <div style={{ fontSize: "9px", color: "#888" }}>T history: [{(nd.tHist || []).join(" → ")}]</div>
                    <div style={hintStyle}>
                      {nd.slope >= 2 ? "Rising fast — can push WATCH → WARNING early." : nd.slope >= 1 ? "Rising — getting worse." : nd.slope === 0 ? "Flat — stable." : "Falling — improving."}
                    </div>
                  </div>

                  <div style={cardStyle}>
                    <div>Role: <b>{nd.dom === "front" ? "LEADING EDGE" : nd.dom === "impact" ? "DAMAGE ZONE" : nd.dom === "arrest" ? "CONTAINMENT BOUNDARY" : "UNAFFECTED"}</b></div>
                    <div style={hintStyle}>
                      {nd.dom === "front" ? "Hazard heading this way." : nd.dom === "impact" ? "Surrounded by damage." : nd.dom === "arrest" ? "Front stopped here." : "No elevated scores."}
                    </div>
                    <div style={{ fontSize: "9px", marginTop: 2 }}>Front:<b>{nd.frontScore}</b> Impact:<b>{nd.impactScore}</b> Arrest:<b>{nd.arrestScore}</b></div>
                  </div>

                  <div style={cardStyle}>
                    <div><b>Coherence: {nd.coherence}/3 {nd.coherence >= 2 ? "✓ PASS" : "✗ FAIL"}</b></div>
                    <div style={hintStyle}>False alarm filter. 2 of 3 must pass to escalate.</div>
                    <div style={{ fontSize: "9px", lineHeight: 1.7, marginTop: 3 }}>
                      <div>[{nd.cAdj ? "✓" : "✗"}] Adjacency — missing neighbors next to each other?</div>
                      <div>[{nd.cPers ? "✓" : "✗"}] Persistence — same direction 2/3 cycles?</div>
                      <div>[{nd.cProg ? "✓" : "✗"}] Progression — new disappearances in that direction?</div>
                    </div>
                  </div>
                </div>
              ) : (
                <div style={{ color: "#bbb", padding: 8, lineHeight: 1.8 }}>
                  <div><b>Slope</b> = is T going up or down? (trend)</div>
                  <div><b>Role</b> = which sub-score is highest</div>
                  <div><b>Coherence</b> = false alarm filter (2/3 must pass)</div>
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
