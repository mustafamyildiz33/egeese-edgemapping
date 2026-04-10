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
const ALERT_BITS = { 0: "00", 1: "01", 2: "10", 3: "11" };
const ALERT_LABEL = { 0: "NORMAL", 1: "WATCH", 2: "WARNING", 3: "IMPACT" };
const DIR_LABELS = { 1: "E", 2: "NE", 3: "NW", 4: "W", 5: "SW", 6: "SE" };
const PAPER_PHASES = [
  {
    key: "phase1",
    title: "Phase 1 - Fair Baseline",
    tint: "#1F6C8C",
    challenge: "steady_state_baseline",
    focus: "speed, throughput, overhead, scalability, watched-node load",
    behavior: "Clean traffic only. Same seeds and same node sizes across 49, 64, and 81 nodes.",
    spec60: "paper_eval/phase1/phase1_baseline_60s.json",
    spec120: "paper_eval/phase1/phase1_baseline_120s.json"
  },
  {
    key: "phase2",
    title: "Phase 2 - Hazard Sensing",
    tint: "#B45309",
    challenge: "tornado_sweep",
    focus: "local hazard, far sensing, first detection, scaling under a moving threat",
    behavior: "Seeded tornado batches move across the grid, then recover before reset.",
    spec60: "paper_eval/phase2/phase2_hazard_60s.json",
    spec120: "paper_eval/phase2/phase2_hazard_120s.json"
  },
  {
    key: "phase3",
    title: "Phase 3 - Adversarial Stress",
    tint: "#9E2A2B",
    challenge: "ghost_outage_noise",
    focus: "false unavailability, lie_sensor, flap, resilience, false positives",
    behavior: "Temporary crash, forced alert lie, and flapping noise on nearby nodes.",
    spec60: "paper_eval/phase3/phase3_stress_60s.json",
    spec120: "paper_eval/phase3/phase3_stress_120s.json"
  }
];
const PAPER_OUTPUTS = [
  "runs/<timestamp>/paper_events.jsonl",
  "runs/<timestamp>/paper_summary.tsv",
  "runs/<timestamp>/paper_watch_nodes.tsv",
  "paper_reports/<suite_id>_<timestamp>/all_runs.tsv",
  "paper_reports/<suite_id>_<timestamp>/summary_by_nodes.tsv",
  "campaign_reports/<campaign_id>_<timestamp>/campaign_runs.tsv"
];
const PAPER_SMOKE_COMMAND = "python3 paper_eval_runner.py --spec paper_eval/phase2/phase2_hazard_60s.json --max-runs 1 --node-counts 49 --duration-sec 10";
const PAPER_CAMPAIGN_60_COMMAND = "python3 paper_eval_campaign.py --spec paper_eval/campaign/all_together_60s.json --max-batches 1 --node-counts 49";
const PAPER_CAMPAIGN_120_COMMAND = "python3 paper_eval_campaign.py --spec paper_eval/campaign/all_together_120s.json --max-batches 1 --node-counts 49";
const PAPER_CAMPAIGN_FULL_COMMAND = "python3 paper_eval_campaign.py --spec paper_eval/campaign/all_together_60s.json --node-counts 49";
const PUSH_READY_ITEMS = [
  "Commit the runner, the phase specs, the implementation guide, and the React demo update together.",
  "Do not commit generated paper_reports; they are local validation output only.",
  "Run the 60s specs first, then rerun with 120s only if the team wants the longer set in the paper.",
  "Use the same seeds for EGESS and the check-in protocol when the comparison adapter is added."
];
const PAPER_HOME_MODES = [
  {
    key: "run",
    title: "Run Evaluation",
    tint: "#1F6C8C",
    description: "Start a new benchmark batch or a single scenario.",
    details: "You will see exact commands, expected timing, and which HTML/TSV outputs will be created."
  },
  {
    key: "report",
    title: "View Reports",
    tint: "#9E2A2B",
    description: "Open finished dashboards instead of starting anything new.",
    details: "You will see campaign summaries, protocol comparisons, node spotlight pages, pull history, and raw exports."
  }
];
const PAPER_REPORT_SURFACES = [
  {
    key: "campaign",
    title: "Campaign Dashboard",
    tint: "#1F6C8C",
    description: "This is the best top-level report after an all-together run.",
    shows: "Batch overview, one row per scenario per batch, and links into every scenario dashboard.",
    command: "CAMPAIGN_DIR=$(ls -1dt campaign_reports/* | head -n 1)\nopen \"$CAMPAIGN_DIR/index.html\""
  },
  {
    key: "suite",
    title: "Scenario Suite Dashboard",
    tint: "#B45309",
    description: "Use this when you want to compare one scenario across saved runs.",
    shows: "Protocol comparison tabs, averages, suite charts, watched-node tables, and grouped node-count summaries.",
    command: "REPORT_DIR=$(ls -1dt paper_reports/* | head -n 1)\nopen \"$REPORT_DIR/index.html\""
  },
  {
    key: "run",
    title: "Single Run Deep Dive",
    tint: "#2E7D32",
    description: "Use this when you want to inspect one finished run in detail.",
    shows: "Node Spotlight, pull history, watch-node history, all-node snapshot, and raw per-run files.",
    command: "RUN_DIR=$(ls -1dt runs/* | head -n 1)\nopen \"$RUN_DIR/paper_summary.html\""
  }
];

const hintStyle = { fontSize: "8px", color: "#999", fontStyle: "italic", marginBottom: 1, lineHeight: "1.35" };
const cardStyle = { background: "#fff", padding: "5px 8px", border: "1px solid #e0e0e0", borderRadius: "3px", marginTop: "5px" };
const refRow = { padding: "2px 0", lineHeight: "1.5", display: "flex", alignItems: "center", gap: 6 };
const refBadge = { display: "inline-block", padding: "1px 6px", fontSize: "10px", fontWeight: "bold", background: "#FFF3E0", border: "1px solid #ddd", borderRadius: 3, minWidth: 28, textAlign: "center" };
const refHint = { fontSize: "8px", color: "#999", fontStyle: "italic", marginBottom: 2, paddingLeft: 34 };

function alertCodeFromState(state, T) {
  if (state === S.IMPACT) return 3;
  if (state === S.WARNING) return 2;
  if ([S.WATCH, S.CONTAINED, S.RECOVERING].includes(state) || T > 0) return 1;
  return 0;
}

function alertBits(code) {
  return ALERT_BITS[code] || "00";
}

function alertLabel(code) {
  return ALERT_LABEL[code] || "NORMAL";
}

function alertDeltaBits(currentCode, prevCode) {
  const diff = currentCode - prevCode;
  if (currentCode <= 0 && diff <= 0) return 0;
  if (diff >= 2) return 3;
  if (diff === 1) return 2;
  if (currentCode > 0) return 1;
  return 0;
}

function mkNode() {
  return {
    online: true, state: S.NORMAL, T: 0, prevT: 0, sensor: "NORMAL",
    cycles: 0, noProg: 0, nbrSt: {}, missK: {}, known: [],
    tHist: [], slope: 0,
    frontScore: 0, impactScore: 0, arrestScore: 0, coherence: 0,
    alert: { code: 0, bits: "00", level: "NORMAL", deltaBits: 0, cycle: 0, message: "L1 00 NORMAL" },
    at: {
      vec: [0, 0, 0, 0, 0, 0],
      tGrad: [0, 0, 0, 0, 0, 0],
      delta: [0, 0, 0, 0, 0, 0],
      dir: 0,
      dirLabel: "",
      width: 0,
      dist: 0,
      speed: 0,
      eta: 99,
      phase: "clear",
      strength: 0
    },
    wire: {
      alertTx: [],
      alertRx: [],
      confirmTx: null,
      confirmRx: []
    },
    atVecHist: [],
    atDistHist: [],
    dom: "none", cAdj: 0, cPers: 0, cProg: 0,
    pd: { nm: [], pm: [], rc: [], dg: [], dgRaw: 0, dgFiltered: 0, tm: 0 },
    dSec: 0, sHist: [],
    dgStreak: {}
  };
}

function pullNode(port, nodes, filterEnabled) {
  const me = nodes[port];
  if (!me.online) return { ...me, cycles: me.cycles + 1 };

  const nbrs = NBRS[port] || [];
  const nSt = {};
  const mK = {};
  let nM = 0, pM = 0, dg = 0, rc = 0;
  let dgRaw = 0;
  const known = [];
  const pnm = [], ppm = [], prc = [], pdg = [];
  const dgS = { ...(me.dgStreak || {}) };

  nbrs.forEach(np => {
    const nb = nodes[np];
    const prev = me.nbrSt[np] || "alive";
    const prevM = me.missK[np] || 0;

    if (!nb.online) {
      mK[np] = prevM + 1;
      nSt[np] = "missing";
      if (prev === "missing") { pM++; ppm.push(np); }
      else { nM++; pnm.push(np); }
      dgS[np] = 0;
    } else {
      mK[np] = 0;
      known.push(np);
      if (prev === "missing") { nSt[np] = "recovered"; rc++; prc.push(np); }
      else { nSt[np] = "alive"; }

      const disagree = nb.T >= 3 && nb.T > me.T + 1;
      if (disagree) {
        dgRaw++;
        dgS[np] = (dgS[np] || 0) + 1;
        if (dgS[np] > 5) dgS[np] = 2;

        if (filterEnabled) {
          if (dgS[np] >= 2) {
            dg++;
            pdg.push(np);
          }
        } else {
          dg++;
          pdg.push(np);
        }
      } else {
        dgS[np] = 0;
      }
    }
  });

  const dgUsed = filterEnabled ? Math.min(dg, 2) : dg;

  const tM = nM + pM;
  const pd = {
    nm: pnm,
    pm: ppm,
    rc: prc,
    dg: pdg,
    dgRaw,
    dgFiltered: dg,
    tm: tM
  };

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
  const mom = (me.T > 0 && (nM > 0 || dgUsed > 0)) ? 1 : 0;
  const fs = 2 * nM + dgUsed + 0.5 * pM + mom;
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
  // Strong raw scores can open WATCH early, but WARNING still needs confirmed front coherence.
  else if (T >= 6 && !gated) state = S.WATCH;
  else if (T > 0 && tM === 0 && rc > 0 && [S.IMPACT, S.WARNING, S.CONTAINED, S.WATCH].includes(me.state)) state = S.RECOVERING;
  else if (T > 0 && me.state !== S.NORMAL && rc > 0) state = S.RECOVERING;
  if (T === 0) state = S.NORMAL;

  const atVec = [0, 0, 0, 0, 0, 0];
  const atTGrad = [0, 0, 0, 0, 0, 0];
  const atDelta = [0, 0, 0, 0, 0, 0];

  nbrs.forEach(np => {
    const sec = sectorOf(port, np);
    if (sec < 1 || sec > 6) return;
    const idx = sec - 1;
    const nb = nodes[np];

    if (!nb.online) {
      atVec[idx] = 1;
      atTGrad[idx] = 99;
      atDelta[idx] = 3;
      return;
    }

    atTGrad[idx] = Math.max(atTGrad[idx], nb.T);
    if (nb.T === 0) atDelta[idx] = 0;
    else if (nb.slope <= 0) atDelta[idx] = 1;
    else if (nb.slope < 2) atDelta[idx] = 2;
    else atDelta[idx] = 3;
  });

  let sinSum = 0;
  let cosSum = 0;
  let totalWeight = 0;
  for (let i = 0; i < 6; i++) {
    const angle = i * Math.PI / 3;
    const w = atVec[i] * 5 + (atTGrad[i] < 99 ? atTGrad[i] : 0) + atDelta[i] * 0.5;
    sinSum += w * Math.sin(angle);
    cosSum += w * Math.cos(angle);
    totalWeight += w;
  }

  let atDir = 0;
  let atDirLabel = "";
  if (totalWeight > 0) {
    const angleDeg = ((Math.atan2(sinSum, cosSum) * 180 / Math.PI) + 360) % 360;
    atDir = Math.round(angleDeg / 60) % 6 + 1;
    if (atDir > 6) atDir = 1;
    atDirLabel = DIR_LABELS[atDir] || "";
  }

  const atWidth = atVec.reduce((sum, value) => sum + value, 0)
    + atTGrad.filter(value => value > 0 && value < 99).length * 0.5
    + atDelta.filter(value => value >= 2).length * 0.3;

  const maxNbrT = Math.max(...atTGrad.filter(value => value < 99), 0);
  const atDist = maxNbrT > 0 ? Math.round((12 / maxNbrT) * 10) / 10 : 99;

  const dHist = [...(me.atDistHist || []), atDist].slice(-4);
  let atSpeed = 0;
  if (dHist.length >= 2 && dHist[0] < 90 && dHist[dHist.length - 1] < 90) {
    atSpeed = Math.round(((dHist[0] - dHist[dHist.length - 1]) / (dHist.length - 1)) * 10) / 10;
  }

  const atEta = atSpeed > 0 && atDist < 90 ? Math.round((atDist / atSpeed) * 10) / 10 : 99;

  const vHist = [...(me.atVecHist || []), [...atVec]].slice(-4);
  let atPhase = "clear";
  if (totalWeight > 0) {
    if (vHist.length >= 2) {
      const prev = vHist[vHist.length - 2];
      const curr = vHist[vHist.length - 1];
      let growing = 0;
      let shrinking = 0;
      for (let i = 0; i < 6; i++) {
        if (curr[i] > prev[i]) growing++;
        else if (curr[i] < prev[i]) shrinking++;
      }
      const risingDeltas = atDelta.filter(value => value >= 2).length;
      const stableDeltas = atDelta.filter(value => value === 1).length;

      if (growing > 0 || risingDeltas >= 2) atPhase = "approaching";
      else if (shrinking > 0 && growing === 0) atPhase = "recovering";
      else if (stableDeltas >= 2 && risingDeltas === 0 && growing === 0) atPhase = "contained";
      else atPhase = "monitoring";
    } else {
      atPhase = "monitoring";
    }
  }

  let atStrength = 0;
  atStrength += atVec.reduce((sum, value) => sum + value, 0) * 2;
  atStrength += atTGrad.filter(value => value > 0 && value < 99).reduce((sum, value) => sum + value * 0.3, 0);
  atStrength += atDelta.filter(value => value >= 2).length * 0.5;
  atStrength = Math.round(atStrength * 10) / 10;

  const at = {
    vec: atVec,
    tGrad: atTGrad,
    delta: atDelta,
    dir: atDir,
    dirLabel: atDirLabel,
    width: Math.round(atWidth * 10) / 10,
    dist: atDist,
    speed: atSpeed,
    eta: atEta,
    phase: atPhase,
    strength: atStrength
  };

  const prevAlertCode = me.alert?.code || 0;
  const alertCode = alertCodeFromState(state, T);
  const alert = {
    code: alertCode,
    bits: alertBits(alertCode),
    level: alertLabel(alertCode),
    deltaBits: alertDeltaBits(alertCode, prevAlertCode),
    cycle: me.cycles + 1,
    message: `L1 ${alertBits(alertCode)} ${alertLabel(alertCode)} means something is happening`
  };

  const alertRx = [];
  const confirmRx = [];
  nbrs.forEach(np => {
    const nb = nodes[np];
    if (!nb || !nb.online) return;
    const sec = sectorOf(port, np);
    const nbAlertCode = nb.alert?.code ?? alertCodeFromState(nb.state, nb.T);
    const nbAlertBits = nb.alert?.bits ?? alertBits(nbAlertCode);
    const nbAlertLevel = nb.alert?.level ?? alertLabel(nbAlertCode);
    if (nbAlertCode > 0) {
      alertRx.push({
        port: np,
        direction: DIR_LABELS[sec] || "",
        bits: nbAlertBits,
        level: nbAlertLevel,
        deltaBits: nb.alert?.deltaBits ?? alertDeltaBits(nbAlertCode, 0)
      });
    }
    if (nb.at && nb.at.phase !== "clear" && nb.at.strength > 0) {
      confirmRx.push({
        port: np,
        direction: DIR_LABELS[sec] || "",
        phase: nb.at.phase.toUpperCase(),
        dirLabel: nb.at.dirLabel || "",
        dist: nb.at.dist,
        speed: nb.at.speed,
        eta: nb.at.eta
      });
    }
  });

  const wire = {
    alertTx: alert.code > 0 ? (nbrs || []).map(np => ({ port: np, bits: alert.bits, level: alert.level })) : [],
    alertRx,
    confirmTx: at.phase !== "clear" && at.strength > 0
      ? {
          phase: at.phase.toUpperCase(),
          dirLabel: at.dirLabel || "",
          dist: at.dist,
          speed: at.speed,
          eta: at.eta,
          targets: [...(nbrs || [])]
        }
      : null,
    confirmRx
  };

  return {
    ...me, T, prevT: me.T, state, noProg, nbrSt: nSt, missK: mK,
    cycles: me.cycles + 1, known, tHist: th, slope: sl,
    frontScore: Math.round(fs * 10) / 10, impactScore: Math.round(is * 10) / 10,
    arrestScore: aS, coherence: coh, dom, cAdj, cPers, cProg, pd, dSec, sHist,
    dgStreak: dgS, alert, at, wire, atVecHist: vHist, atDistHist: dHist
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
  st.push({ a: [], k: [], r: [], lbl: "No new losses — stall detection begins" });
  st.push({ a: [], k: [], r: [], lbl: "Stall persists — no new damage" });
  st.push({ a: [], k: [], r: [], lbl: "Stall confirmed — slope flat or declining" });
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
    { a: [], k: [], r: [], lbl: "Destruction stops — stall detection" },
    { a: [], k: [], r: [], lbl: "Stall persists — no new damage" },
    { a: [], k: [], r: [], lbl: "Slope flat — containment confirmed" },
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
  const [filterEnabled, setFilterEnabled] = useState(true);
  const [compareResults, setCompareResults] = useState({ off: null, on: null });
  const [comparisonRun, setComparisonRun] = useState(null);
  const [zoom, setZoom] = useState(1.0);
  const [draggingMap, setDraggingMap] = useState(null);
  const [sel, setSel] = useState(null);
  const [inspOpen, setInspOpen] = useState(true);
  const [refOpen, setRefOpen] = useState(false);
  const [paperOpen, setPaperOpen] = useState(true);
  const [paperView, setPaperView] = useState("run");
  const [calcDeg, setCalcDeg] = useState(120);
  const [calcX, setCalcX] = useState(-2.5);
  const [calcY, setCalcY] = useState(2.6);

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
  const peakRef = useRef(null);
  dRef.current = { demo, dStep };
  const filterRef = useRef(filterEnabled);
  filterRef.current = filterEnabled;

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
    setComparisonRun(null);
    dragRef.current = { active: false, target: null, startX: 0, startY: 0, scrollLeft: 0, scrollTop: 0, moved: false, suppressClick: false };
    peakRef.current = null;
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
    const filt = filterRef.current;
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
        Object.keys(applied).forEach(k => { upd[k] = pullNode(+k, applied, filt); });
        return upd;
      }
      const upd = {};
      Object.keys(cur).forEach(k => { upd[k] = pullNode(+k, cur, filt); });
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
    peakRef.current = null;
    setTimeout(() => { setDemo(w); setDStep(0); setRunning(true); }, 100);
  }, [resetAll]);

  const startComparison = useCallback((enabled, key) => {
    resetAll();
    setFilterEnabled(enabled);
    setCompareResults(prev => ({ ...prev, [key]: null }));
    setComparisonRun(key);
    peakRef.current = {
      key,
      filterEnabled: enabled,
      peakFpRate: 0,
      peakFalsePosCount: 0,
      peakPositiveCount: 0,
      detectionLatency: null,
      peakSpatialAcc: 0,
      fpNodes: [],
      fpLabel: "none"
    };
    setTimeout(() => {
      setDemo("corner");
      setDStep(0);
      setRunning(true);
      setLabel(enabled ? "Comparison run — filter ON" : "Comparison run — filter OFF");
    }, 100);
  }, [resetAll]);

  const nd = sel !== null ? nodes[sel] : null;

  const trigAngleRad = useMemo(() => Number(calcDeg) * Math.PI / 180, [calcDeg]);
  const trigValues = useMemo(() => {
    const sin = Math.sin(trigAngleRad);
    const cos = Math.cos(trigAngleRad);
    const tan = Math.abs(cos) < 1e-9 ? null : Math.tan(trigAngleRad);
    const atan = ((Math.atan2(Number(calcY), Number(calcX)) * 180 / Math.PI) + 360) % 360;
    const sector = Math.floor(((atan + 30) % 360) / 60) + 1;
    return { sin, cos, tan, atan, sector, sectorLabel: DIR_LABELS[sector] || "" };
  }, [calcX, calcY, trigAngleRad]);

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
    const alertNodes = vals.filter(n => n.online && [S.WATCH, S.WARNING, S.IMPACT].includes(n.state));
    const layer1Nodes = vals.filter(n => n.online && (n.alert?.code || 0) > 0);
    const layer2Nodes = vals.filter(n => n.online && n.at && n.at.phase !== "clear" && n.at.strength > 0);
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

    // False positive: protocol alert nodes (WATCH/WARNING/IMPACT) with no nearby damage or corroborating non-normal neighbors
    let falsePos = 0;
    const falsePositiveNodes = [];
    alertNodes.forEach(n => {
      const port = Object.entries(nodes).find(([, v]) => v === n);
      if (!port) return;
      const p = +port[0];
      const hasNearbyDamage = (NBRS[p] || []).some(np => !nodes[np].online) ||
        (NBRS[p] || []).some(np => nodes[np].online && nodes[np].state !== S.NORMAL);
      if (!hasNearbyDamage) {
        falsePos++;
        falsePositiveNodes.push(p);
      }
    });
    const fpRate = alertNodes.length > 0 ? Math.round(falsePos / alertNodes.length * 100) : 0;

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
      falsePosCount: falsePos,
      falsePositiveNodes,
      elevatedCount: alertNodes.length,
      allOnline, anyElevated,
      hazardStatus, hazardDesc, hazardColor
      , layer1Count: layer1Nodes.length
      , layer2Count: layer2Nodes.length
    };
  }, [nodes]);

  useEffect(() => {
    if (!comparisonRun || !peakRef.current) return;

    const peak = peakRef.current;
    if (metrics.fpRate >= peak.peakFpRate) {
      peak.peakFpRate = metrics.fpRate;
      peak.peakFalsePosCount = metrics.falsePosCount;
      peak.peakPositiveCount = metrics.elevatedCount;
      peak.fpNodes = [...metrics.falsePositiveNodes];
      peak.fpLabel = metrics.falsePositiveNodes.length > 1 ? "scattered" : metrics.falsePositiveNodes.length === 1 ? "isolated" : "none";
    }
    if (peak.detectionLatency === null && metrics.detectionLatency > 0) {
      peak.detectionLatency = metrics.detectionLatency;
    }
    peak.peakSpatialAcc = Math.max(peak.peakSpatialAcc, metrics.spatialAcc);

    const demoDone = demo === "corner" && dStep >= scripts.corner.length;
    if (demoDone && metrics.hazardStatus === "CLEAR" && metrics.nOff === 0 && !metrics.anyElevated) {
      setCompareResults(prev => ({
        ...prev,
        [comparisonRun]: {
          method: peak.filterEnabled ? "With filter" : "No filter",
          filterEnabled: peak.filterEnabled,
          fpRate: peak.peakFpRate,
          falsePosCount: peak.peakFalsePosCount,
          positiveCount: peak.peakPositiveCount,
          detectionLatency: peak.detectionLatency ?? 0,
          spatialAcc: peak.peakSpatialAcc,
          elevated: peak.peakPositiveCount,
          fpNodes: peak.fpNodes,
          fpLabel: peak.fpLabel
        }
      }));
      setComparisonRun(null);
      peakRef.current = null;
      setRunning(false);
      setDemo(null);
      setLabel((peak.filterEnabled ? "Filtered" : "No-filter") + " comparison run saved");
    }
  }, [comparisonRun, metrics, demo, dStep, scripts.corner.length]);

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
        <button style={{ ...btnBase, background: filterEnabled ? "#2E7D32" : "#9E2A2B", color: "#fff" }} onClick={() => setFilterEnabled(v => !v)}>
          Filter: {filterEnabled ? "ON" : "OFF"}
        </button>
        <button style={{ ...btnBase, background: "#9E2A2B", color: "#fff" }} onClick={() => startComparison(false, "off")}>Test: No Filter</button>
        <button style={{ ...btnBase, background: "#2E7D32", color: "#fff" }} onClick={() => startComparison(true, "on")}>Test: With Filter</button>
        <span style={{ fontSize: "9px", color: "#777", fontFamily: "monospace" }}>
          Filter rule: disagreements need a 2-cycle streak and are capped at 2 before they affect FrontScore.
        </span>
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
          <div style={{ textAlign: "center" }}>
            <div style={{ fontSize: "12px", fontWeight: "bold", color: "#8B6914" }}>
              {metrics.layer1Count}
            </div>
            <div style={{ color: "#999", fontSize: "7px" }}>L1 alert</div>
          </div>
          <div style={{ textAlign: "center" }}>
            <div style={{ fontSize: "12px", fontWeight: "bold", color: "#CD6600" }}>
              {metrics.layer2Count}
            </div>
            <div style={{ color: "#999", fontSize: "7px" }}>L2 confirm</div>
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
            <b>Figure B</b> — Threshold Map + Absence Tomography
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
                    {!off && d.at && d.at.strength > 0 && d.T === 0 && (
                      <polygon
                        points={hexPts(cx, cy, sz * 1.15)}
                        fill="none"
                        stroke={d.at.phase === "approaching" ? "#CD3333" : d.at.phase === "contained" ? "#8B668B" : d.at.phase === "recovering" ? "#3CB371" : "#DAA520"}
                        strokeWidth={d.at.strength >= 3 ? 2.5 : d.at.strength >= 1.5 ? 2 : 1.2}
                        strokeDasharray={d.at.phase === "contained" ? "2,4" : "4,3"}
                        opacity={Math.min(0.9, 0.3 + d.at.strength * 0.1)}
                      />
                    )}
                    {!off && d.at && d.at.strength > 0 && d.T === 0 && d.at.dir > 0 && d.at.phase === "approaching" && (() => {
                      const angle = (d.at.dir - 1) * 60 * Math.PI / 180;
                      const ax = cx + Math.cos(angle) * sz * 1.35;
                      const ay = cy + Math.sin(angle) * sz * 1.35;
                      const tx = cx + Math.cos(angle) * sz * 0.65;
                      const ty = cy + Math.sin(angle) * sz * 0.65;
                      return (
                        <line
                          x1={ax}
                          y1={ay}
                          x2={tx}
                          y2={ty}
                          stroke={d.at.strength >= 3 ? "#CD3333" : "#CD6600"}
                          strokeWidth={d.at.strength >= 2 ? 2.5 : 1.8}
                          opacity={0.7}
                        />
                      );
                    })()}
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
                    {!off && d.at && d.at.strength > 0 && d.T === 0 && (
                      <text
                        x={cx}
                        y={cy + sz * .45}
                        textAnchor="middle"
                        dominantBaseline="middle"
                        fill={d.at.phase === "approaching" ? "#CD3333" : d.at.phase === "contained" ? "#8B668B" : d.at.phase === "recovering" ? "#3CB371" : "#CD6600"}
                        fontSize="5"
                        fontFamily="monospace"
                        fontWeight="bold"
                        opacity={0.85}
                      >
                        {d.at.dirLabel || ""}{d.at.eta < 90 ? " ~" + d.at.eta + "c" : ""}{d.at.phase === "contained" ? " ■" : d.at.phase === "recovering" ? " ↓" : ""}
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
        <div style={{ display: "flex", alignItems: "center", gap: 4 }}>
          <div style={{ width: 10, height: 10, background: "transparent", border: "2px dashed #CD6600", borderRadius: 1 }} />
          <span style={{ fontSize: "9px", color: "#CD6600", fontFamily: "monospace" }}>Tomography</span>
        </div>
      </div>

      {(compareResults.off || compareResults.on) && (
        <div style={{ padding: "8px 16px", background: "#fff", borderBottom: "1px solid #ddd", fontFamily: "monospace", fontSize: "10px" }}>
          <div style={{ fontWeight: "bold", fontSize: "11px", marginBottom: 6 }}>Filter Comparison</div>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ background: "#f6f6f6" }}>
                {["Method", "False Pos%", "Detect Delay", "Spatial Acc", "WATCH+ Nodes", "False Positive Nodes"].map(h => (
                  <th key={h} style={{ textAlign: "left", padding: "6px 8px", border: "1px solid #ddd" }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {[compareResults.off, compareResults.on].filter(Boolean).map(row => (
                <tr key={row.method}>
                  <td style={{ padding: "6px 8px", border: "1px solid #ddd" }}>
                    <span style={{ color: row.filterEnabled ? "#2E7D32" : "#9E2A2B", marginRight: 6 }}>■</span>{row.method}
                  </td>
                  <td style={{ padding: "6px 8px", border: "1px solid #ddd", color: row.fpRate <= 5 ? "#2E7D32" : "#9E2A2B" }}>{row.fpRate}%</td>
                  <td style={{ padding: "6px 8px", border: "1px solid #ddd" }}>{row.detectionLatency.toFixed(1)} cyc</td>
                  <td style={{ padding: "6px 8px", border: "1px solid #ddd", color: row.spatialAcc >= 80 ? "#2E7D32" : "#B8860B" }}>{row.spatialAcc}%</td>
                  <td style={{ padding: "6px 8px", border: "1px solid #ddd" }}>{row.elevated}</td>
                  <td style={{ padding: "6px 8px", border: "1px solid #ddd" }}>
                    {row.fpNodes.length ? row.fpNodes.join(", ") + " (" + row.fpLabel + ")" : "none"}
                  </td>
                </tr>
              ))}
              {compareResults.off && compareResults.on && (
                <tr style={{ background: "#fafaf0" }}>
                  <td style={{ padding: "6px 8px", border: "1px solid #ddd", fontWeight: "bold" }}>Filter Delta</td>
                  <td style={{ padding: "6px 8px", border: "1px solid #ddd", color: (compareResults.on.fpRate - compareResults.off.fpRate) <= 0 ? "#2E7D32" : "#9E2A2B" }}>
                    {(compareResults.on.fpRate - compareResults.off.fpRate) >= 0 ? "+" : ""}{compareResults.on.fpRate - compareResults.off.fpRate}%
                  </td>
                  <td style={{ padding: "6px 8px", border: "1px solid #ddd", color: (compareResults.on.detectionLatency - compareResults.off.detectionLatency) <= 0 ? "#2E7D32" : "#B8860B" }}>
                    {(compareResults.on.detectionLatency - compareResults.off.detectionLatency) >= 0 ? "+" : ""}{(compareResults.on.detectionLatency - compareResults.off.detectionLatency).toFixed(1)} cyc
                  </td>
                  <td style={{ padding: "6px 8px", border: "1px solid #ddd", color: (compareResults.on.spatialAcc - compareResults.off.spatialAcc) >= 0 ? "#2E7D32" : "#9E2A2B" }}>
                    {(compareResults.on.spatialAcc - compareResults.off.spatialAcc) >= 0 ? "+" : ""}{compareResults.on.spatialAcc - compareResults.off.spatialAcc}%
                  </td>
                  <td style={{ padding: "6px 8px", border: "1px solid #ddd" }}>
                    {(compareResults.on.positiveCount - compareResults.off.positiveCount) >= 0 ? "+" : ""}{compareResults.on.positiveCount - compareResults.off.positiveCount}
                  </td>
                  <td style={{ padding: "6px 8px", border: "1px solid #ddd" }}>
                    {compareResults.off.falsePosCount >= compareResults.on.falsePosCount
                      ? `${compareResults.off.falsePosCount - compareResults.on.falsePosCount} false triggers removed`
                      : `${compareResults.on.falsePosCount - compareResults.off.falsePosCount} more false triggers`}
                  </td>
                </tr>
              )}
            </tbody>
          </table>
          <div style={{ fontSize: "9px", color: "#777", marginTop: 6 }}>
            False Pos% is computed from the worst snapshot as isolated WATCH/WARNING/IMPACT nodes divided by total WATCH/WARNING/IMPACT nodes. With the filter on, disagreements must persist for 2 cycles and are capped at 2 before they can amplify FrontScore.
          </div>
        </div>
      )}

      <div style={{ flexShrink: 0, borderTop: "1px solid #ccc", background: "#fff", overflow: "hidden" }}>
        <div onClick={() => setPaperOpen(!paperOpen)} style={{ padding: "4px 16px", cursor: "pointer", display: "flex", justifyContent: "space-between", alignItems: "center", background: "#F5F8FA", borderBottom: paperOpen ? "1px solid #d8e1e8" : "none" }}>
          <span style={{ fontSize: "11px", fontWeight: "bold" }}>{paperOpen ? "▼" : "▶"} Paper Evaluation Home</span>
          <span style={{ fontSize: "9px", color: "#7b8794", fontFamily: "monospace" }}>choose run or report</span>
        </div>
        {paperOpen && (
          <div style={{ padding: "10px 16px 12px", display: "flex", flexDirection: "column", gap: 10, background: "linear-gradient(180deg, #fff 0%, #fcfcf8 100%)" }}>
            <div style={{ display: "flex", flexWrap: "wrap", gap: 10 }}>
              {PAPER_HOME_MODES.map(mode => {
                const active = paperView === mode.key;
                return (
                  <button
                    key={mode.key}
                    type="button"
                    onClick={() => setPaperView(mode.key)}
                    style={{
                      flex: "1 1 260px",
                      textAlign: "left",
                      border: active ? `1px solid ${mode.tint}` : "1px solid #d8e1e8",
                      borderLeft: `4px solid ${mode.tint}`,
                      borderRadius: 4,
                      padding: "10px 12px",
                      background: active ? "#F8FBFF" : "#fff",
                      cursor: "pointer",
                      boxShadow: active ? "0 0 0 1px rgba(31,108,140,0.08)" : "none"
                    }}
                  >
                    <div style={{ display: "flex", justifyContent: "space-between", gap: 8, alignItems: "baseline" }}>
                      <div style={{ fontWeight: "bold", fontSize: "11px", color: mode.tint }}>{mode.title}</div>
                      <span style={{ fontSize: "8px", color: active ? mode.tint : "#777", fontFamily: "monospace" }}>{active ? "selected" : "available"}</span>
                    </div>
                    <div style={{ fontSize: "9px", color: "#444", marginTop: 5, lineHeight: 1.7 }}>{mode.description}</div>
                    <div style={{ fontSize: "8px", color: "#6b7280", marginTop: 6, lineHeight: 1.6 }}>{mode.details}</div>
                  </button>
                );
              })}
            </div>

            {paperView === "run" ? (
              <>
                <div style={{ display: "flex", flexWrap: "wrap", gap: 10 }}>
                  <div style={{ ...cardStyle, marginTop: 0, flex: "1 1 340px", borderLeft: "4px solid #1F6C8C", background: "#F7FBFD" }}>
                    <div style={{ fontWeight: "bold", fontSize: "11px", marginBottom: 4 }}>Implementation Guide</div>
                    <div style={{ fontSize: "9px", lineHeight: 1.7, color: "#4b5563" }}>
                      The shareable team doc lives at <code>PAPER_EVAL_IMPLEMENTATION.md</code>. The standard runner is <code>paper_eval_runner.py</code>, and the all-scenarios batch runner is <code>paper_eval_campaign.py</code>.
                    </div>
                    <div style={{ fontSize: "9px", marginTop: 6, color: "#0f4c5c" }}>
                      Exact active windows: <b>60s</b> and <b>120s</b>. The all-together flow treats <b>1 batch</b> as <b>Baseline + Tornado + Stress</b>.
                    </div>
                  </div>
                  <div style={{ ...cardStyle, marginTop: 0, flex: "1 1 320px", borderLeft: "4px solid #B45309", background: "#FFF9F1" }}>
                    <div style={{ fontWeight: "bold", fontSize: "11px", marginBottom: 4 }}>What Run Evaluation Means</div>
                    <div style={{ fontSize: "9px", lineHeight: 1.7, color: "#5b4636" }}>
                      Choose this when you want to start a fresh run. The terminal command will create new HTML dashboards and TSV files.
                    </div>
                    <div style={{ fontSize: "8px", color: "#8b6b4a", marginTop: 6, lineHeight: 1.6 }}>
                      You will see: command blocks, scenario timing, what each run creates, and which dashboard to open after it finishes.
                    </div>
                  </div>
                  <div style={{ ...cardStyle, marginTop: 0, flex: "1 1 320px", borderLeft: "4px solid #2E7D32", background: "#F8FCF8" }}>
                    <div style={{ fontWeight: "bold", fontSize: "11px", marginBottom: 4 }}>Same-Computer Smoke Test</div>
                    <div style={{ fontSize: "9px", lineHeight: 1.7, color: "#355E3B" }}>
                      One laptop is enough for implementation, evaluation, and validation. Use this short run before the full <code>60s</code> or <code>120s</code> suites.
                    </div>
                    <div style={{ marginTop: 7, padding: "5px 6px", background: "#eef8ee", borderRadius: 3 }}>
                      <code style={{ display: "block", whiteSpace: "pre-wrap", overflowWrap: "anywhere", fontSize: "9px", lineHeight: 1.5 }}>
                        {PAPER_SMOKE_COMMAND}
                      </code>
                    </div>
                  </div>
                </div>

                <div style={{ ...cardStyle, marginTop: 0, borderLeft: "4px solid #1F6C8C", background: "#F7FBFD" }}>
                  <div style={{ display: "flex", justifyContent: "space-between", gap: 8, alignItems: "baseline" }}>
                    <div style={{ fontWeight: "bold", fontSize: "11px", color: "#1F6C8C" }}>All Scenarios Together</div>
                    <span style={{ fontSize: "8px", color: "#1F6C8C", fontFamily: "monospace" }}>recommended</span>
                  </div>
                  <div style={{ fontSize: "9px", color: "#444", marginTop: 5, lineHeight: 1.7 }}>
                    <div><b>What it does:</b> Runs <b>Baseline</b>, <b>Tornado</b>, and <b>Ghost Outage + Noise</b> as one batch.</div>
                    <div><b>How to count it:</b> <b>1 batch</b> means one full pass of all three scenarios. If you want 30 total all-together repetitions, run <b>30 batches</b>.</div>
                    <div><b>What you will see:</b> A campaign dashboard with one row per scenario per batch, plus links into each scenario dashboard.</div>
                  </div>
                  <div style={{ display: "flex", flexWrap: "wrap", gap: 10, marginTop: 8 }}>
                    <div style={{ flex: "1 1 280px", padding: "5px 6px", background: "#fff", borderRadius: 3, border: "1px solid #d8e1e8" }}>
                      <div style={{ fontSize: "8px", color: "#666", marginBottom: 2 }}>60s batch command</div>
                      <code style={{ display: "block", whiteSpace: "pre-wrap", overflowWrap: "anywhere", fontSize: "9px", lineHeight: 1.5 }}>
                        {PAPER_CAMPAIGN_60_COMMAND}
                      </code>
                    </div>
                    <div style={{ flex: "1 1 280px", padding: "5px 6px", background: "#fff", borderRadius: 3, border: "1px solid #d8e1e8" }}>
                      <div style={{ fontSize: "8px", color: "#666", marginBottom: 2 }}>120s batch command</div>
                      <code style={{ display: "block", whiteSpace: "pre-wrap", overflowWrap: "anywhere", fontSize: "9px", lineHeight: 1.5 }}>
                        {PAPER_CAMPAIGN_120_COMMAND}
                      </code>
                    </div>
                    <div style={{ flex: "1 1 280px", padding: "5px 6px", background: "#fff", borderRadius: 3, border: "1px solid #d8e1e8" }}>
                      <div style={{ fontSize: "8px", color: "#666", marginBottom: 2 }}>Full 30-batch command</div>
                      <code style={{ display: "block", whiteSpace: "pre-wrap", overflowWrap: "anywhere", fontSize: "9px", lineHeight: 1.5 }}>
                        {PAPER_CAMPAIGN_FULL_COMMAND}
                      </code>
                    </div>
                  </div>
                </div>

                <div style={{ display: "flex", flexWrap: "wrap", gap: 10 }}>
                  {PAPER_PHASES.map(phase => (
                    <div key={phase.key} style={{ ...cardStyle, marginTop: 0, flex: "1 1 290px", borderLeft: `4px solid ${phase.tint}`, background: "#fff" }}>
                      <div style={{ display: "flex", justifyContent: "space-between", gap: 8, alignItems: "baseline" }}>
                        <div style={{ fontWeight: "bold", fontSize: "11px", color: phase.tint }}>{phase.title}</div>
                        <span style={{ fontSize: "8px", color: "#777", fontFamily: "monospace" }}>{phase.challenge}</span>
                      </div>
                      <div style={{ fontSize: "9px", color: "#444", marginTop: 5, lineHeight: 1.7 }}>
                        <div><b>Focus:</b> {phase.focus}</div>
                        <div><b>Scenario:</b> {phase.behavior}</div>
                        <div><b>What you will see:</b> A single scenario suite dashboard plus deep-dive run pages for that scenario only.</div>
                      </div>
                      <div style={{ marginTop: 7, padding: "5px 6px", background: "#f8f8f8", borderRadius: 3 }}>
                        <div style={{ fontSize: "8px", color: "#666", marginBottom: 2 }}>60s command</div>
                        <code style={{ display: "block", whiteSpace: "pre-wrap", overflowWrap: "anywhere", fontSize: "9px", lineHeight: 1.5 }}>
                          python3 paper_eval_runner.py --spec {phase.spec60}
                        </code>
                      </div>
                      <div style={{ marginTop: 6, padding: "5px 6px", background: "#f8f8f8", borderRadius: 3 }}>
                        <div style={{ fontSize: "8px", color: "#666", marginBottom: 2 }}>120s command</div>
                        <code style={{ display: "block", whiteSpace: "pre-wrap", overflowWrap: "anywhere", fontSize: "9px", lineHeight: 1.5 }}>
                          python3 paper_eval_runner.py --spec {phase.spec120}
                        </code>
                      </div>
                    </div>
                  ))}
                </div>

                <div style={{ ...cardStyle, marginTop: 0, borderLeft: "4px solid #2E7D32", background: "#F8FCF8" }}>
                  <div style={{ fontWeight: "bold", fontSize: "11px", marginBottom: 4 }}>Push Checklist</div>
                  <div style={{ display: "flex", flexWrap: "wrap", gap: 10 }}>
                    {PUSH_READY_ITEMS.map(item => (
                      <div key={item} style={{ flex: "1 1 240px", fontSize: "9px", lineHeight: 1.7, color: "#355E3B" }}>
                        - {item}
                      </div>
                    ))}
                  </div>
                </div>
              </>
            ) : (
              <>
                <div style={{ display: "flex", flexWrap: "wrap", gap: 10 }}>
                  <div style={{ ...cardStyle, marginTop: 0, flex: "1 1 320px", borderLeft: "4px solid #9E2A2B", background: "#FFF7F7" }}>
                    <div style={{ fontWeight: "bold", fontSize: "11px", marginBottom: 4 }}>What View Reports Means</div>
                    <div style={{ fontSize: "9px", lineHeight: 1.7, color: "#5f3737" }}>
                      Choose this when you do <b>not</b> want to start anything new. You are only opening finished HTML dashboards and raw files.
                    </div>
                    <div style={{ fontSize: "8px", color: "#8c5a5a", marginTop: 6, lineHeight: 1.6 }}>
                      You will see: protocol comparison tables, scenario filters, suite averages, node spotlight pages, pull history, and exports for Excel.
                    </div>
                  </div>
                  <div style={{ ...cardStyle, marginTop: 0, flex: "1 1 320px", borderLeft: "4px solid #B45309", background: "#FFF9F1" }}>
                    <div style={{ fontWeight: "bold", fontSize: "11px", marginBottom: 4 }}>What The Reports Show</div>
                    <div style={{ fontSize: "9px", lineHeight: 1.7, color: "#5b4636" }}>
                      Campaign dashboards summarize batches, scenario suite dashboards compare runs of one scenario, and run dashboards let you inspect one node or one watched pair in detail.
                    </div>
                    <div style={{ fontSize: "8px", color: "#8b6b4a", marginTop: 6, lineHeight: 1.6 }}>
                      Use campaign first, then scenario suite, then single run if you need a deeper explanation for one node or one event path.
                    </div>
                  </div>
                  <div style={{ ...cardStyle, marginTop: 0, flex: "1 1 340px", borderLeft: "4px solid #1F6C8C", background: "#F7FBFD" }}>
                    <div style={{ fontWeight: "bold", fontSize: "11px", marginBottom: 4 }}>Saved Outputs</div>
                    <div style={{ fontSize: "9px", lineHeight: 1.7, color: "#4b5563" }}>
                      {PAPER_OUTPUTS.map(path => (
                        <div key={path}><code>{path}</code></div>
                      ))}
                    </div>
                    <div style={{ fontSize: "8px", color: "#0f4c5c", marginTop: 6 }}>
                      TSV outputs paste cleanly into Excel for color-coded scorecards and grouped figures.
                    </div>
                  </div>
                </div>

                <div style={{ display: "flex", flexWrap: "wrap", gap: 10 }}>
                  {PAPER_REPORT_SURFACES.map(surface => (
                    <div key={surface.key} style={{ ...cardStyle, marginTop: 0, flex: "1 1 290px", borderLeft: `4px solid ${surface.tint}`, background: "#fff" }}>
                      <div style={{ display: "flex", justifyContent: "space-between", gap: 8, alignItems: "baseline" }}>
                        <div style={{ fontWeight: "bold", fontSize: "11px", color: surface.tint }}>{surface.title}</div>
                        <span style={{ fontSize: "8px", color: "#777", fontFamily: "monospace" }}>open latest</span>
                      </div>
                      <div style={{ fontSize: "9px", color: "#444", marginTop: 5, lineHeight: 1.7 }}>
                        <div><b>When to use it:</b> {surface.description}</div>
                        <div><b>What you will see:</b> {surface.shows}</div>
                      </div>
                      <div style={{ marginTop: 7, padding: "5px 6px", background: "#f8f8f8", borderRadius: 3 }}>
                        <div style={{ fontSize: "8px", color: "#666", marginBottom: 2 }}>Open command</div>
                        <code style={{ display: "block", whiteSpace: "pre-wrap", overflowWrap: "anywhere", fontSize: "9px", lineHeight: 1.5 }}>
                          {surface.command}
                        </code>
                      </div>
                    </div>
                  ))}
                </div>

                <div style={{ ...cardStyle, marginTop: 0, borderLeft: "4px solid #1F6C8C", background: "#F7FBFD" }}>
                  <div style={{ fontWeight: "bold", fontSize: "11px", marginBottom: 4 }}>Reading Order</div>
                  <div style={{ display: "flex", flexWrap: "wrap", gap: 10 }}>
                    <div style={{ flex: "1 1 220px", fontSize: "9px", lineHeight: 1.7, color: "#274c5e" }}>
                      <b>1.</b> Start with the campaign dashboard if you ran the all-together batch.
                    </div>
                    <div style={{ flex: "1 1 220px", fontSize: "9px", lineHeight: 1.7, color: "#274c5e" }}>
                      <b>2.</b> Open the scenario suite dashboard to use comparison tabs like <code>All</code>, <code>Baseline</code>, and <code>Tornado</code>.
                    </div>
                    <div style={{ flex: "1 1 220px", fontSize: "9px", lineHeight: 1.7, color: "#274c5e" }}>
                      <b>3.</b> Open a single run page when you want <code>Node Spotlight</code>, pull history, or the all-node raw table.
                    </div>
                  </div>
                </div>
              </>
            )}
          </div>
        )}
      </div>

      {/* Scoring Reference */}
      <div style={{ flexShrink: 0, borderTop: "1px solid #ccc", background: "#fff", overflow: "hidden" }}>
        <div onClick={() => setRefOpen(!refOpen)} style={{ padding: "3px 16px", cursor: "pointer", display: "flex", justifyContent: "space-between", alignItems: "center", background: "#f7f7f0", borderBottom: refOpen ? "1px solid #ddd" : "none" }}>
          <span style={{ fontSize: "11px", fontWeight: "bold" }}>{refOpen ? "▼" : "▶"} Scoring Reference — How T is computed</span>
          <span style={{ fontSize: "9px", color: "#999", fontFamily: "monospace" }}>T = max(FrontScore, ImpactScore)</span>
        </div>
        {refOpen && (
          <div style={{ padding: "8px 16px", display: "flex", gap: 20, fontSize: "10px", fontFamily: "monospace", overflowX: "auto" }}>
            {/* Column 0: Two-layer protocol + calculator */}
            <div style={{ minWidth: 260 }}>
              <div style={{ fontWeight: "bold", fontSize: "11px", marginBottom: 6, color: "#333" }}>Two-Layer Protocol</div>
              <div style={{ ...refRow, color: "#8B6914" }}><span style={{ ...refBadge, background: "#FFF8E1" }}>L1</span> <b>2-bit alert</b> — "something is happening"</div>
              <div style={refHint}>Every cycle, each node sends a compact 2-bit status to its six neighbors.</div>
              <div style={{ fontSize: "9px", lineHeight: 1.7, paddingLeft: 34, color: "#555" }}>
                <div><b>00</b> = normal</div>
                <div><b>01</b> = watch</div>
                <div><b>10</b> = warning</div>
                <div><b>11</b> = impact</div>
              </div>

              <div style={{ ...refRow, color: "#CD6600", marginTop: 8 }}><span style={{ ...refBadge, background: "#FFF3E0" }}>L2</span> <b>confirmation</b> — "it is strongest from this direction"</div>
              <div style={refHint}>Only sent when local absence tomography sees a directional signal. Carries phase, direction, distance, speed, and ETA.</div>
              <div style={{ fontSize: "9px", lineHeight: 1.7, paddingLeft: 34, color: "#555" }}>
                <div><b>approaching</b> = getting closer</div>
                <div><b>impact</b> = local failures now</div>
                <div><b>contained</b> = not growing</div>
                <div><b>recovering</b> = pulling away / coming back</div>
              </div>

              <div style={{ marginTop: 8, padding: "6px 8px", background: "#f9f9f0", border: "1px solid #e8e8d8", borderRadius: 3 }}>
                <div style={{ fontWeight: "bold", marginBottom: 4 }}>Degree-Mode Trig Calculator</div>
                <div style={{ display: "grid", gridTemplateColumns: "58px 1fr", gap: 6, alignItems: "center", fontSize: "9px" }}>
                  <label>deg</label>
                  <input type="number" value={calcDeg} onChange={e => setCalcDeg(e.target.value)} style={{ width: "100%", padding: "3px 4px", fontFamily: "monospace", fontSize: "9px" }} />
                  <label>sin</label>
                  <div>{trigValues.sin.toFixed(3)}</div>
                  <label>cos</label>
                  <div>{trigValues.cos.toFixed(3)}</div>
                  <label>tan</label>
                  <div>{trigValues.tan === null ? "undefined" : trigValues.tan.toFixed(3)}</div>
                </div>
                <div style={{ fontSize: "8px", color: "#777", marginTop: 6 }}>Use this for the arrow pieces in the direction vote.</div>
                <div style={{ borderTop: "1px solid #e3e3d3", marginTop: 6, paddingTop: 6, display: "grid", gridTemplateColumns: "58px 1fr", gap: 6, alignItems: "center", fontSize: "9px" }}>
                  <label>atan Y</label>
                  <input type="number" value={calcY} onChange={e => setCalcY(e.target.value)} style={{ width: "100%", padding: "3px 4px", fontFamily: "monospace", fontSize: "9px" }} />
                  <label>atan X</label>
                  <input type="number" value={calcX} onChange={e => setCalcX(e.target.value)} style={{ width: "100%", padding: "3px 4px", fontFamily: "monospace", fontSize: "9px" }} />
                  <label>atan2</label>
                  <div>{trigValues.atan.toFixed(1)}° → {trigValues.sectorLabel}</div>
                </div>
              </div>
            </div>

            {/* Column 1: Pull signals */}
            <div style={{ minWidth: 200 }}>
              <div style={{ fontWeight: "bold", fontSize: "11px", marginBottom: 4, color: "#333" }}>Pull Signals (per neighbor)</div>
              <div style={{ ...refRow, color: "#8B0000" }}><span style={refBadge}>+3</span> Missing neighbor (in ImpactScore)</div>
              <div style={refHint}>Neighbor didn't respond to pull — offline.</div>
              <div style={{ ...refRow, color: "#8B0000" }}><span style={refBadge}>+2</span> New missing (in FrontScore)</div>
              <div style={refHint}>Just went offline THIS cycle. Strongest front signal.</div>
              <div style={{ ...refRow, color: "#8B4513" }}><span style={{ ...refBadge, background: "#FFF3E0" }}>+0.5</span> Persistent missing (in FrontScore)</div>
              <div style={refHint}>Was already missing last cycle. Old news, counts less.</div>
              <div style={{ ...refRow, color: "#8B6914" }}><span style={{ ...refBadge, background: "#FFF8E1" }}>+1</span> Disagreement</div>
              <div style={refHint}>Neighbor sees higher danger than me. With the filter on, it must persist for 2 cycles before it counts.</div>
              <div style={{ ...refRow, color: "#2E7D32" }}><span style={{ ...refBadge, background: "#E8F5E9" }}>→</span> Recovered neighbor</div>
              <div style={refHint}>Was missing, now back. Triggers CONTAINED → RECOVERING.</div>
            </div>

            {/* Column 2: Sub-scores */}
            <div style={{ minWidth: 240 }}>
              <div style={{ fontWeight: "bold", fontSize: "11px", marginBottom: 6, color: "#333" }}>Three Questions Every Node Asks</div>
              
              <div style={{ ...refRow, color: "#8B6914" }}><span style={{ ...refBadge, background: "#FFF8E1" }}>1</span> <b>FrontScore</b> — "Is danger coming toward me?"</div>
              <div style={refHint}>Looks at NEW disappearances. If neighbors just started going offline and others are warning me, the front is approaching. Higher = closer.</div>
              <div style={{ fontSize: "9px", color: "#888", padding: "2px 0 4px 34px" }}>= 2 × (just went offline) + filtered disagreement + 0.5 × (still offline from before)</div>

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
                <div><span style={{ ...refBadge, background: "#E3F2FD" }}>+1</span> <span style={{ fontSize: "9px" }}>momentum</span></div>
                <div style={{ fontSize: "8px", color: "#888", paddingTop: 2 }}>If T was already up and signal continues, add 1. Keeps score from flickering.</div>
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
              <div style={refRow}><span style={{ ...refBadge, background: FILL.WATCH, color: "#000" }}>≥3</span> WATCH — early caution; can also open on a strong raw score</div>
              <div style={refRow}><span style={{ ...refBadge, background: FILL.WARNING, color: "#000" }}>≥6</span> WARNING — confirmed front approaching</div>
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
                  <div style={{ ...cardStyle, borderLeft: "3px solid #CD6600" }}>
                    <div><b>Two-Layer Messages</b></div>
                    <div style={{ fontSize: "9px", marginTop: 3 }}>
                      Layer 1 TX: <b>{nd.alert.bits}</b> {nd.alert.level} {nd.wire.alertTx.length > 0 ? `-> [${nd.wire.alertTx.map(item => item.port).join(", ")}]` : "-> quiet"}
                    </div>
                    <div style={hintStyle}>2-bit alert means "something is happening."</div>
                    <div style={{ fontSize: "9px", marginTop: 3 }}>
                      Layer 1 RX: {nd.wire.alertRx.length > 0
                        ? nd.wire.alertRx.map(item => `${item.direction}:${item.bits}/${item.level}`).join(" | ")
                        : "none"}
                    </div>
                    <div style={{ fontSize: "9px", marginTop: 3 }}>
                      Layer 2 TX: {nd.wire.confirmTx
                        ? `${nd.wire.confirmTx.phase} ${nd.wire.confirmTx.dirLabel || "?"} d=${nd.wire.confirmTx.dist} v=${nd.wire.confirmTx.speed} eta=${nd.wire.confirmTx.eta}`
                        : "none"}
                    </div>
                    <div style={hintStyle}>Confirmation message means "yes, and it is strongest from this direction."</div>
                    <div style={{ fontSize: "9px", marginTop: 3 }}>
                      Layer 2 RX: {nd.wire.confirmRx.length > 0
                        ? nd.wire.confirmRx.map(item => `${item.direction}:${item.phase}/${item.dirLabel || "?"}`).join(" | ")
                        : "none"}
                    </div>
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
                    <div style={{ color: "#8B6914", marginTop: 3 }}>
                      Disagree: [{nd.pd.dg.join(", ") || "—"}] = <b>{nd.pd.dg.length}</b>
                      {nd.pd.dgRaw > nd.pd.dg.length && <span style={{ color: "#999", fontSize: "9px" }}> (raw: {nd.pd.dgRaw}, filtered to {nd.pd.dg.length})</span>}
                    </div>
                    <div style={hintStyle}>
                      {filterEnabled
                        ? "Filter ON: disagreement needs 2 consecutive cycles and is capped at 2 before it affects FrontScore."
                        : "Filter OFF: every disagreement counts immediately, uncapped."}
                    </div>
                  </div>

                  <div style={cardStyle}>
                    <div><b>FrontScore</b> — "Is the hazard heading toward me?"</div>
                    <div style={hintStyle}>High when NEW neighbors disappear. Detects the leading edge.</div>
                    <div>
                      {filterEnabled
                        ? <> = 2×{nd.pd.nm.length} + dg({Math.min(nd.pd.dgFiltered, 2)}) + 0.5×{nd.pd.pm.length} + mom({nd.T > 0 && (nd.pd.nm.length > 0 || nd.pd.dg.length > 0) ? 1 : 0}) = <b style={{ color: "#8B6914" }}>{nd.frontScore}</b></>
                        : <> = 2×{nd.pd.nm.length} + 1×{nd.pd.dgRaw} + 0.5×{nd.pd.pm.length} + mom({nd.T > 0 && (nd.pd.nm.length > 0 || nd.pd.dg.length > 0) ? 1 : 0}) = <b style={{ color: "#8B6914" }}>{nd.frontScore}</b></>}
                    </div>
                    <div style={hintStyle}>
                      {filterEnabled
                        ? "Filtered mode: disagreement needs a 2-cycle streak and is capped at 2. This prevents delayed nodes from creating noise cascades."
                        : "Raw mode: disagreements count immediately, so this reacts faster but is noisier."}
                    </div>
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
                      {nd.state === S.NORMAL ? "All clear." : nd.state === S.WATCH ? (nd.coherence >= 2 ? "Possible approaching hazard." : "Early caution from a strong score; front coherence is not confirmed yet.") : nd.state === S.WARNING ? "Confirmed front approaching." : nd.state === S.IMPACT ? "In the damage zone." : nd.state === S.CONTAINED ? "Front stopped here. No new damage + slope flat or declining confirms containment." : nd.state === S.RECOVERING ? "Getting better." : "Offline."}
                    </div>
                    <div style={{ fontSize: "9px", marginTop: 3 }}>
                      Layer 1 alert: <b>{nd.alert.bits}</b> {nd.alert.level} | delta bits: <b>{nd.alert.deltaBits}</b>
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
                    <div><b>Front Coherence: {nd.coherence}/3 {nd.coherence >= 2 ? "✓ PASS" : "✗ FAIL"}</b></div>
                    <div style={hintStyle}>Front confirmation check. 2 of 3 is needed for WARNING; strong raw scores can still raise early WATCH.</div>
                    <div style={{ fontSize: "9px", lineHeight: 1.7, marginTop: 3 }}>
                      <div>[{nd.cAdj ? "✓" : "✗"}] Adjacency — missing neighbors next to each other?</div>
                      <div>[{nd.cPers ? "✓" : "✗"}] Persistence — same direction 2/3 cycles?</div>
                      <div>[{nd.cProg ? "✓" : "✗"}] Progression — new disappearances in that direction?</div>
                    </div>
                  </div>

                  <div style={{ ...cardStyle, borderLeft: nd.at && nd.at.strength > 0 ? "3px solid #CD6600" : "1px solid #e0e0e0" }}>
                    <div><b>Absence Tomography: {nd.at && nd.at.strength > 0
                      ? <span style={{ color: nd.at.phase === "approaching" ? "#CD3333" : nd.at.phase === "contained" ? "#8B668B" : nd.at.phase === "recovering" ? "#3CB371" : "#CD6600", fontSize: "12px" }}>{nd.at.phase.toUpperCase()}</span>
                      : <span style={{ color: "#888" }}>CLEAR</span>}</b>
                    </div>
                    {nd.at && nd.at.strength > 0 && (
                      <div style={{ marginTop: 3, padding: "4px 6px", background: nd.at.phase === "approaching" ? "#FFF0F0" : nd.at.phase === "contained" ? "#F8F0FF" : "#F0FFF0", borderRadius: 3, fontSize: "10px", lineHeight: 1.6 }}>
                        <div>Direction: <b style={{ color: "#CD6600", fontSize: "12px" }}>{nd.at.dirLabel || "—"}</b>
                          {nd.at.eta < 90 && <span> — ETA <b style={{ color: "#CD3333" }}>~{nd.at.eta} cycles</b></span>}
                          {nd.at.speed > 0 && <span> — Speed: <b>{nd.at.speed} hops/cyc</b></span>}
                        </div>
                        <div>Dist: ~<b>{nd.at.dist < 90 ? nd.at.dist : "?"} hops</b> — Width: <b>{nd.at.width}</b> sectors — Strength: <b>{nd.at.strength}</b></div>
                      </div>
                    )}
                    <div style={hintStyle}>
                      {nd.at && nd.at.strength > 0
                        ? "Computed from local observations only. Direction uses the neighbor T-gradient; ETA comes from estimated distance and closing speed."
                        : "No directional threat signals from any sector."}
                    </div>
                    {nd.at && nd.at.strength > 0 && (
                      <div style={{ fontSize: "9px", marginTop: 3, lineHeight: 1.7, fontFamily: "monospace" }}>
                        <div>Absence vec: [{nd.at.vec.join(",")}] — {nd.at.vec.filter(value => value > 0).length === 0 ? "no missing" : nd.at.vec.filter(value => value > 0).length + " sectors absent"}</div>
                        <div>T-gradient:  [{nd.at.tGrad.map(value => value >= 99 ? "X" : value).join(",")}] — max={Math.max(...nd.at.tGrad.filter(value => value < 99), 0)}</div>
                        <div>Delta bits:  [{nd.at.delta.join(",")}] — {["", "", "rising", "spiking"][Math.max(...nd.at.delta)] || "stable"}</div>
                        <div>Direction vote: atan2(sumY, sumX) = {nd.at.dirLabel || "?"} ({nd.at.dir > 0 ? ((nd.at.dir - 1) * 60) : 0}° sector)</div>
                        <div style={{ fontSize: "8px", color: "#999", marginTop: 2 }}>
                          Sectors: E(1) NE(2) NW(3) W(4) SW(5) SE(6) | X=offline | Delta: 0=ok 1=stable 2=rising 3=spike
                        </div>
                      </div>
                    )}
                  </div>
                </div>
              ) : (
                <div style={{ color: "#bbb", padding: 8, lineHeight: 1.8 }}>
                  <div><b>Slope</b> = is T going up or down? (trend)</div>
                  <div><b>Role</b> = which sub-score is highest</div>
                  <div><b>Front Coherence</b> = confirms directional front shape; WARNING needs 2/3</div>
                  <div><b>Absence Tomography</b> = threat direction, distance, speed, ETA from local data</div>
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
