const FINGER_COLORS = {
  "L-pinky": "#f472b6",
  "L-ring": "#fb923c",
  "L-middle": "#facc15",
  "L-index": "#4ade80",
  "R-index": "#4ade80",
  "R-middle": "#facc15",
  "R-ring": "#fb923c",
  "R-pinky": "#f472b6",
  "L-thumb": "#fbbf24",
  "R-thumb": "#fbbf24",
};

const LEFT_ROWS = [
  ["`", "1", "2", "3", "4", "5"],
  ["tab", "q", "w", "e", "r", "t"],
  ["caps", "a", "s", "d", "f", "g"],
  ["shift", "z", "x", "c", "v", "b"],
];

const RIGHT_ROWS = [
  ["6", "7", "8", "9", "0", "-"],
  ["y", "u", "i", "o", "p", "["],
  ["h", "j", "k", "l", ";", "'"],
  ["n", "m", ",", ".", "/", "shift"],
];

const FINGER_MAP = {
  "`": "L-pinky", "1": "L-pinky", "2": "L-ring", "3": "L-middle", "4": "L-index", "5": "L-index",
  tab: "R-thumb", q: "L-pinky", w: "L-ring", e: "L-middle", r: "L-index", t: "L-index",
  caps: "L-pinky", a: "L-pinky", s: "L-ring", d: "L-middle", f: "L-index", g: "L-index",
  shift: "L-pinky", z: "L-pinky", x: "L-ring", c: "L-middle", v: "L-index", b: "L-index",
  "6": "R-index", "7": "R-index", "8": "R-middle", "9": "R-ring", "0": "R-pinky", "-": "R-pinky",
  y: "R-index", u: "R-index", i: "R-middle", o: "R-ring", p: "R-pinky", "[": "L-pinky",
  h: "R-index", j: "R-index", k: "R-middle", l: "R-ring", ";": "R-pinky", "'": "R-pinky",
  n: "R-index", m: "R-index", ",": "R-middle", ".": "R-ring", "/": "R-pinky",
  "\\": "R-pinky", "]": "R-pinky", "{": "L-pinky", "}": "R-pinky", "|": "R-pinky",
  "=": "R-pinky", "+": "R-pinky", _: "R-pinky", "*": "R-ring", "&": "L-ring",
  ":": "R-ring", '"': "R-pinky", "<": "R-pinky", ">": "R-pinky", "?": "R-pinky",
  " ": "R-thumb",
  enter: "R-thumb",
  backspace: "L-thumb",
  delete: "L-thumb",
  meta: "L-thumb",
  cmd: "L-thumb",
};

const RIGHT_PINKY_STRETCH_KEYS = new Set(["enter", "shift", "'", '"', "]", "\\", "return"]);

function normalizeKey(key) {
  if (!key) return "";
  const k = key.length === 1 ? key.toLowerCase() : key.toLowerCase();
  if (k === " ") return " ";
  if (k === "backspace") return "backspace";
  if (k === "enter" || k === "return") return "enter";
  if (k === "shift") return "shift";
  if (k === "tab") return "tab";
  if (k === "meta" || k === "cmd" || k === "command") return "meta";
  return k;
}

function fingerForChar(char, shiftHeld) {
  const c = normalizeKey(char);
  if (c === " ") return "R-thumb";
  if (shiftHeld && /[a-z]/.test(char)) return "R-thumb";
  return FINGER_MAP[c] || "R-index";
}

function isThumbKey(char) {
  const c = normalizeKey(char);
  return c === " " || c === "enter" || c === "backspace" || c === "delete" || c === "meta" || c === "tab";
}

function isRightPinkyStretch(char) {
  return RIGHT_PINKY_STRETCH_KEYS.has(normalizeKey(char));
}

function thumbCluster(osProfile) {
  const mac = osProfile === "mac";
  return {
    left: [
      { id: "backspace", label: "⌫", thumb: true },
      { id: " ", label: "Space", thumb: true, wide: true },
      { id: "delete", label: "Del", thumb: true },
      { id: "meta", label: mac ? "⌘" : "Win", thumb: true },
    ],
    right: [
      { id: "enter", label: "↵", thumb: true },
      { id: "tab", label: "Tab", thumb: true },
      { id: "shift", label: "⇧", thumb: true },
      { id: "layer", label: "Fn", thumb: true },
    ],
  };
}

function renderKeyboard(container, options = {}) {
  const {
    osProfile = "mac",
    highlightKey = null,
    pressedKey = null,
    compact = false,
  } = options;

  const thumbs = thumbCluster(osProfile);
  const wrap = document.createElement("div");
  wrap.className = "kb-wrap";

  function buildHalf(rows, side) {
    const half = document.createElement("div");
    half.className = "kb-half";
    const well = document.createElement("div");
    well.className = "kb-well";

    rows.forEach((row, idx) => {
      const rowEl = document.createElement("div");
      rowEl.className = "kb-row";
      if (idx === 1) rowEl.classList.add("offset-1");
      if (idx >= 2) rowEl.classList.add("offset-2");

      row.forEach((key) => {
        const keyEl = document.createElement("div");
        const id = normalizeKey(key);
        keyEl.className = "kb-key";
        if (key === "shift" || key === "tab" || key === "caps") keyEl.classList.add("wide");
        if (key === "backspace" || key === "enter") keyEl.classList.add("wider");
        keyEl.dataset.key = id;
        keyEl.textContent = key.length === 1 ? key : key.slice(0, 3);

        if (!compact && FINGER_MAP[id]) {
          const finger = document.createElement("span");
          finger.className = "finger";
          finger.textContent = FINGER_MAP[id].replace("L-", "L·").replace("R-", "R·");
          keyEl.appendChild(finger);
        }

        applyKeyState(keyEl, id, highlightKey, pressedKey);
        rowEl.appendChild(keyEl);
      });
      well.appendChild(rowEl);
    });

    const cluster = document.createElement("div");
    cluster.className = "kb-thumb-cluster";
    const clusterKeys = side === "left" ? thumbs.left : thumbs.right;
    clusterKeys.forEach((spec) => {
      const keyEl = document.createElement("div");
      keyEl.className = "kb-key thumb";
      if (spec.wide) keyEl.classList.add("space");
      keyEl.dataset.key = spec.id;
      keyEl.textContent = spec.label;
      applyKeyState(keyEl, spec.id, highlightKey, pressedKey);
      cluster.appendChild(keyEl);
    });

    half.appendChild(well);
    half.appendChild(cluster);
    return half;
  }

  wrap.appendChild(buildHalf(LEFT_ROWS, "left"));
  wrap.appendChild(buildHalf(RIGHT_ROWS, "right"));
  container.replaceChildren(wrap);
  return wrap;
}

function applyKeyState(el, id, highlightKey, pressedKey) {
  el.classList.remove("target", "pressed", "correct-flash", "wrong-flash");
  const h = highlightKey ? normalizeKey(highlightKey) : null;
  const p = pressedKey ? normalizeKey(pressedKey) : null;
  if (h && h === id) el.classList.add("target");
  if (p && p === id) el.classList.add("pressed");
}

function flashKey(container, key, ok) {
  const id = normalizeKey(key);
  const el = container.querySelector(`[data-key="${CSS.escape(id)}"]`);
  if (!el) return;
  el.classList.add(ok ? "correct-flash" : "wrong-flash");
  window.setTimeout(() => el.classList.remove("correct-flash", "wrong-flash"), 120);
}

if (typeof window !== "undefined") {
  window.KinesisKeyboard = {
    FINGER_COLORS,
    renderKeyboard,
    flashKey,
    fingerForChar,
    isThumbKey,
    isRightPinkyStretch,
    normalizeKey,
    thumbCluster,
  };
}
