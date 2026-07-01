(function () {
  const { REGIONS, LESSONS, DAILY_DRILL_IDS, PROGRAMMING_DAILY_DRILL_IDS, PROGRAMMING_REGION_IDS } = window.KINESIS_LESSONS;
  const {
    renderKeyboard,
    flashKey,
    fingerForChar,
    isThumbKey,
    isRightPinkyStretch,
    normalizeKey,
  } = window.KinesisKeyboard;

  const STORAGE_KEY = "kinesis-world-progress-v1";

  const state = {
    currentLessonId: null,
    index: 0,
    errors: 0,
    correct: 0,
    thumbHits: 0,
    startedAt: null,
    shiftHeld: false,
    wrongAt: new Set(),
    completed: loadProgress(),
    settings: loadSettings(),
    dailyIndex: dayIndex(),
    timerId: null,
    dailyMode: false,
    dailyEndsAt: null,
    dailyQueue: [],
  };

  const els = {
    welcome: document.getElementById("welcome-screen"),
    quest: document.getElementById("quest-screen"),
    complete: document.getElementById("complete-screen"),
    map: document.getElementById("quest-map"),
    preview: document.getElementById("keyboard-preview"),
    liveKb: document.getElementById("keyboard-live"),
    prompt: document.getElementById("prompt"),
    input: document.getElementById("hidden-input"),
    typingArea: document.getElementById("typing-area"),
    regionLabel: document.getElementById("region-label"),
    questTitle: document.getElementById("quest-title"),
    questDesc: document.getElementById("quest-desc"),
    questFocus: document.getElementById("quest-focus"),
    questGoal: document.getElementById("quest-goal"),
    liveWpm: document.getElementById("live-wpm"),
    liveAccuracy: document.getElementById("live-accuracy"),
    thumbCount: document.getElementById("thumb-count"),
    liveTime: document.getElementById("live-time"),
    ergoAlert: document.getElementById("ergo-alert"),
    nextBtn: document.getElementById("next-btn"),
    totalMinutes: document.getElementById("total-minutes"),
    questsDone: document.getElementById("quests-done"),
    questsTotal: document.getElementById("quests-total"),
    bestWpm: document.getElementById("best-wpm"),
    dailySummary: document.getElementById("daily-summary"),
    completeTitle: document.getElementById("complete-title"),
    completeBody: document.getElementById("complete-body"),
    completeStats: document.getElementById("complete-stats"),
  };

  function loadProgress() {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      if (!raw) return defaultProgress();
      return { ...defaultProgress(), ...JSON.parse(raw) };
    } catch {
      return defaultProgress();
    }
  }

  function defaultProgress() {
    return {
      completedLessons: [],
      bestWpm: 0,
      totalSeconds: 0,
      lessonStats: {},
    };
  }

  function loadSettings() {
    try {
      const raw = localStorage.getItem(STORAGE_KEY + "-settings");
      if (!raw) return { osProfile: "mac", ergoGuard: true, sound: false };
      return JSON.parse(raw);
    } catch {
      return { osProfile: "mac", ergoGuard: true, sound: false };
    }
  }

  function saveProgress() {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(state.completed));
  }

  function saveSettings() {
    localStorage.setItem(STORAGE_KEY + "-settings", JSON.stringify(state.settings));
  }

  function dayIndex() {
    const start = new Date("2026-01-01T00:00:00");
    const now = new Date();
    return Math.floor((now - start) / 86400000);
  }

  function lessonById(id) {
    return LESSONS.find((l) => l.id === id);
  }

  function regionById(id) {
    return REGIONS.find((r) => r.id === id);
  }

  function lessonUnlocked(lesson) {
    if (lesson.requires?.length) {
      return lesson.requires.every((id) => state.completed.completedLessons.includes(id));
    }

    const regionLessons = LESSONS.filter((l) => l.region === lesson.region);
    const idxInRegion = regionLessons.findIndex((l) => l.id === lesson.id);

    if (idxInRegion > 0) {
      return state.completed.completedLessons.includes(regionLessons[idxInRegion - 1].id);
    }

    const regionIdx = REGIONS.findIndex((r) => r.id === lesson.region);
    if (regionIdx === 0) return true;

    const prevRegion = REGIONS[regionIdx - 1];
    const prevLessons = LESSONS.filter((l) => l.region === prevRegion.id);
    const lastPrev = prevLessons[prevLessons.length - 1];
    return state.completed.completedLessons.includes(lastPrev.id);
  }

  function firstProgrammingLesson() {
    return LESSONS.find((l) => l.region === "code-glyphs");
  }

  function programmingTrackUnlocked() {
    const first = firstProgrammingLesson();
    return first ? lessonUnlocked(first) : false;
  }

  function displayText(lesson) {
    let text = lesson.text;
    if (lesson.enterMarkers) {
      text = text.replace(/⏎/g, "\n");
    }
    if (lesson.tabMarkers) {
      text = text.replace(/→/g, "\t");
    }
    return text;
  }

  function displayChar(ch) {
    if (ch === "\n") return "↵";
    if (ch === "\t") return "→";
    return ch;
  }

  function expectedChars(lesson) {
    return displayText(lesson).split("");
  }

  function showScreen(name) {
    els.welcome.classList.toggle("active", name === "welcome");
    els.quest.classList.toggle("active", name === "quest");
    els.complete.classList.toggle("active", name === "complete");
  }

  function updateHeaderStats() {
    els.totalMinutes.textContent = Math.round(state.completed.totalSeconds / 60);
    els.questsDone.textContent = state.completed.completedLessons.length;
    els.questsTotal.textContent = LESSONS.length;
    els.bestWpm.textContent = state.completed.bestWpm || "—";
  }

  function buildMap() {
    els.map.replaceChildren();
    REGIONS.forEach((region) => {
      const block = document.createElement("div");
      block.className = "region-block";
      if (PROGRAMMING_REGION_IDS.has(region.id)) {
        block.dataset.track = "dev";
      }
      const regionLessons = LESSONS.filter((l) => l.region === region.id);
      const done = regionLessons.filter((l) =>
        state.completed.completedLessons.includes(l.id)
      ).length;

      const head = document.createElement("div");
      head.className = "region-head";
      head.innerHTML = `<span>${region.name}</span><span class="progress">${done}/${regionLessons.length}</span>`;

      const list = document.createElement("ul");
      list.className = "quest-list";

      regionLessons.forEach((lesson) => {
        const li = document.createElement("li");
        li.className = "quest-item";
        const btn = document.createElement("button");
        const isDone = state.completed.completedLessons.includes(lesson.id);
        const unlocked = lessonUnlocked(lesson);
        btn.disabled = !unlocked;
        btn.dataset.lessonId = lesson.id;
        if (state.currentLessonId === lesson.id) btn.classList.add("active");
        btn.innerHTML = `<span class="status">${isDone ? "✓" : unlocked ? "○" : "🔒"}</span><span>${lesson.title}</span>`;
        btn.addEventListener("click", () => startLesson(lesson.id));
        li.appendChild(btn);
        list.appendChild(li);
      });

      block.appendChild(head);
      block.appendChild(list);
      els.map.appendChild(block);
    });
  }

  function updateDailySummary() {
    const id = DAILY_DRILL_IDS[state.dailyIndex % DAILY_DRILL_IDS.length];
    const lesson = lessonById(id);
    els.dailySummary.textContent = lesson
      ? `Day ${state.dailyIndex + 1}: ${lesson.title} — ${lesson.focus}. ~10 minutes.`
      : "Short mixed drill.";
  }

  function startLesson(lessonId) {
    const lesson = lessonById(lessonId);
    if (!lesson) return;

    state.currentLessonId = lessonId;
    state.index = 0;
    state.errors = 0;
    state.correct = 0;
    state.thumbHits = 0;
    state.startedAt = Date.now();
    state.shiftHeld = false;
    state.wrongAt = new Set();
    state.dailyMode = false;
    state.dailyEndsAt = null;
    els.nextBtn.classList.add("hidden");
    els.ergoAlert.classList.add("hidden");

    const region = regionById(lesson.region);
    els.regionLabel.textContent = region ? region.name : "";
    els.questTitle.textContent = lesson.title;
    els.questDesc.textContent = lesson.desc;
    els.questFocus.textContent = lesson.focus;
    els.questGoal.textContent = `Goal: ${lesson.goals.accuracy}% acc · ${lesson.goals.wpm} WPM`;
    els.prompt.classList.toggle("code-mode", Boolean(lesson.code));

    renderPrompt();
    renderKeyboard(els.liveKb, {
      osProfile: state.settings.osProfile,
      highlightKey: nextChar(),
    });

    showScreen("quest");
    buildMap();
    focusInput();
    startTimer();
  }

  function startDailyDrill(programming) {
    state.dailyMode = true;
    state.dailyEndsAt = Date.now() + 10 * 60 * 1000;
    state.dailyQueue = [...(programming ? PROGRAMMING_DAILY_DRILL_IDS : DAILY_DRILL_IDS)];
    const first = state.dailyQueue.shift();
    startLesson(first);
    els.questTitle.textContent += programming ? " (Dev drill)" : " (Daily drill)";
  }

  function nextChar() {
    const lesson = lessonById(state.currentLessonId);
    if (!lesson) return null;
    const chars = expectedChars(lesson);
    return chars[state.index] ?? null;
  }

  function renderPrompt() {
    const lesson = lessonById(state.currentLessonId);
    if (!lesson) return;
    const chars = expectedChars(lesson);
    els.prompt.replaceChildren();

    chars.forEach((ch, i) => {
      const span = document.createElement("span");
      span.className = "char";
      if (ch === "\t") span.classList.add("tab-char");
      span.textContent = displayChar(ch);
      if (i < state.index) span.classList.add("correct");
      if (i === state.index) span.classList.add("current");
      if (i < state.index && state.wrongAt.has(i)) {
        span.classList.remove("correct");
        span.classList.add("wrong");
      }
      els.prompt.appendChild(span);
    });
  }

  function focusInput() {
    els.input.value = "";
    els.input.focus();
    els.typingArea.focus();
  }

  function startTimer() {
    if (state.timerId) window.clearInterval(state.timerId);
    state.timerId = window.setInterval(updateLiveStats, 250);
  }

  function elapsedMinutes() {
    return (Date.now() - state.startedAt) / 60000;
  }

  function computeWpm() {
    const mins = elapsedMinutes();
    if (mins <= 0) return 0;
    const words = state.correct / 5;
    return Math.round(words / mins);
  }

  function computeAccuracy() {
    const total = state.correct + state.errors;
    if (total === 0) return 100;
    return Math.round((state.correct / total) * 100);
  }

  function updateLiveStats() {
    const wpm = computeWpm();
    const acc = computeAccuracy();
    els.liveWpm.textContent = String(wpm);
    els.liveAccuracy.textContent = `${acc}%`;
    els.thumbCount.textContent = String(state.thumbHits);

    const secs = Math.floor((Date.now() - state.startedAt) / 1000);
    const m = Math.floor(secs / 60);
    const s = String(secs % 60).padStart(2, "0");
    els.liveTime.textContent = `${m}:${s}`;

    if (state.dailyMode && state.dailyEndsAt && Date.now() >= state.dailyEndsAt) {
      finishLesson(true);
    }
  }

  function playClick(ok) {
    if (!state.settings.sound) return;
    const ctx = new (window.AudioContext || window.webkitAudioContext)();
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    osc.frequency.value = ok ? 520 : 180;
    gain.gain.value = 0.04;
    osc.connect(gain);
    gain.connect(ctx.destination);
    osc.start();
    osc.stop(ctx.currentTime + 0.04);
  }

  function showErgo(message) {
    if (!state.settings.ergoGuard) return;
    els.ergoAlert.textContent = message;
    els.ergoAlert.classList.remove("hidden");
    window.setTimeout(() => els.ergoAlert.classList.add("hidden"), 2200);
  }

  function handleKeydown(ev) {
    if (!state.currentLessonId || !els.quest.classList.contains("active")) return;

    const lesson = lessonById(state.currentLessonId);
    if (!lesson) return;

    if (ev.key === "Shift") {
      state.shiftHeld = true;
      return;
    }

    if (ev.key === "Escape") {
      ev.preventDefault();
      return;
    }

    const expected = nextChar();
    if (expected === null) {
      ev.preventDefault();
      return;
    }

  let typed = ev.key;
  if (expected === "\n" && (typed === "Enter" || typed === "\n")) {
    typed = "\n";
  }
  if (expected === "\t" && typed === "Tab") {
    typed = "\t";
  }

  if (typed.length !== 1 && typed !== "\n" && typed !== "\t") {
    if (typed === "Backspace" && lesson.practiceBackspace) {
      ev.preventDefault();
      if (state.index > 0) {
        state.index -= 1;
        state.thumbHits += 1;
        renderPrompt();
        renderKeyboard(els.liveKb, {
          osProfile: state.settings.osProfile,
          highlightKey: nextChar(),
          pressedKey: "backspace",
        });
      }
      return;
    }
    return;
  }

  ev.preventDefault();

  const match = typed === expected;
  if (match) {
    state.correct += 1;
    if (isThumbKey(typed) || typed === "\n") state.thumbHits += 1;

    const needsShift = /[A-Z]/.test(expected);
    if (needsShift) state.thumbHits += 1;

    const flashId = typed === "\n" ? "enter" : typed === "\t" ? "tab" : typed;
    flashKey(els.liveKb, flashId, true);
    playClick(true);

    if (state.settings.ergoGuard) {
      if (isRightPinkyStretch(typed) && typed !== ";") {
        showErgo("Right pinky stretch detected — use thumb keys (Enter, Shift) on the Advantage 360.");
      }
      if (lesson.avoidRightPinky && lesson.avoidRightPinky.some((k) => normalizeKey(k) === normalizeKey(typed))) {
        showErgo("Prefer thumb Enter / neutral right hand — avoid far pinky reaches.");
      }
    }

    state.index += 1;
    state.wrongAt.delete(state.index - 1);

    renderPrompt();
    renderKeyboard(els.liveKb, {
      osProfile: state.settings.osProfile,
      highlightKey: nextChar(),
      pressedKey: flashId,
    });

    if (nextChar() === null) {
      finishLesson(false);
    }
  } else {
    state.errors += 1;
    state.wrongAt.add(state.index);
    const flashId = typed === "\n" ? "enter" : typed === "\t" ? "tab" : typed;
    flashKey(els.liveKb, flashId, false);
    playClick(false);
    renderPrompt();
  }
  }

  function handleKeyup(ev) {
    if (ev.key === "Shift") state.shiftHeld = false;
  }

  function finishLesson(timeUp) {
    if (state.timerId) {
      window.clearInterval(state.timerId);
      state.timerId = null;
    }

    const lesson = lessonById(state.currentLessonId);
    const wpm = computeWpm();
    const acc = computeAccuracy();
    const duration = Math.floor((Date.now() - state.startedAt) / 1000);
    state.completed.totalSeconds += duration;

    const passed =
      !timeUp &&
      acc >= lesson.goals.accuracy &&
      wpm >= lesson.goals.wpm;

    if (passed && !state.completed.completedLessons.includes(lesson.id)) {
      state.completed.completedLessons.push(lesson.id);
    }

    if (wpm > state.completed.bestWpm) {
      state.completed.bestWpm = wpm;
    }

    state.completed.lessonStats[lesson.id] = {
      wpm,
      accuracy: acc,
      thumbHits: state.thumbHits,
      at: new Date().toISOString(),
      passed,
    };
    saveProgress();
    updateHeaderStats();

    if (state.dailyMode && !timeUp && state.dailyQueue.length > 0) {
      const nextId = state.dailyQueue.shift();
      window.setTimeout(() => startLesson(nextId), 800);
      return;
    }

    if (state.dailyMode && timeUp) {
      showCompleteScreen("Daily drill complete", "Ten minutes done. Consistency beats cramming.", {
        wpm,
        acc,
        thumbHits: state.thumbHits,
        duration,
      });
      state.dailyMode = false;
      buildMap();
      return;
    }

    if (passed) {
      els.nextBtn.classList.remove("hidden");
      showCompleteScreen(
        "Quest cleared!",
        `${lesson.title} — your thumbs and home row are syncing.`,
        { wpm, acc, thumbHits: state.thumbHits, duration }
      );
    } else {
      showCompleteScreen(
        timeUp ? "Time's up" : "Keep practicing",
        timeUp
          ? "Daily window ended. Progress still saved."
          : `Reach ${lesson.goals.accuracy}% accuracy and ${lesson.goals.wpm} WPM to unlock the next quest. Retry beats skip.`,
        { wpm, acc, thumbHits: state.thumbHits, duration }
      );
    }
    buildMap();
    updateProgrammingButton();
  }

  function updateProgrammingButton() {
    const progBtn = document.getElementById("start-programming");
    if (!progBtn) return;
    progBtn.disabled = !programmingTrackUnlocked();
    progBtn.title = programmingTrackUnlocked()
      ? "Jump to Code Glyphs track"
      : "Complete Shift Highlands quest 2 first";
  }

  function showCompleteScreen(title, body, stats) {
    els.completeTitle.textContent = title;
    els.completeBody.textContent = body;
    els.completeStats.innerHTML = `
      <div><span>WPM</span><strong>${stats.wpm}</strong></div>
      <div><span>Accuracy</span><strong>${stats.acc}%</strong></div>
      <div><span>Thumb keys</span><strong>${stats.thumbHits}</strong></div>
      <div><span>Time</span><strong>${Math.floor(stats.duration / 60)}:${String(stats.duration % 60).padStart(2, "0")}</strong></div>
    `;
    showScreen("complete");
  }

  function nextLesson() {
    const idx = LESSONS.findIndex((l) => l.id === state.currentLessonId);
    const next = LESSONS[idx + 1];
    if (next) startLesson(next.id);
    else showScreen("welcome");
  }

  function bindUI() {
    document.getElementById("start-first").addEventListener("click", () => startLesson(LESSONS[0].id));
    document.getElementById("retry-btn").addEventListener("click", () => startLesson(state.currentLessonId));
    document.getElementById("skip-btn").addEventListener("click", () => {
      const idx = LESSONS.findIndex((l) => l.id === state.currentLessonId);
      const next = LESSONS[idx + 1];
      if (next) startLesson(next.id);
    });
    document.getElementById("next-btn").addEventListener("click", nextLesson);
    document.getElementById("map-btn").addEventListener("click", () => showScreen("welcome"));
    document.getElementById("daily-btn").addEventListener("click", () => startDailyDrill(false));
    document.getElementById("dev-daily-btn").addEventListener("click", () => startDailyDrill(true));
    document.getElementById("start-programming").addEventListener("click", () => {
      const first = firstProgrammingLesson();
      if (first && lessonUnlocked(first)) startLesson(first.id);
    });

    document.getElementById("os-profile").value = state.settings.osProfile;
    document.getElementById("ergo-guard").checked = state.settings.ergoGuard;
    document.getElementById("sound-feedback").checked = state.settings.sound;

    document.getElementById("os-profile").addEventListener("change", (e) => {
      state.settings.osProfile = e.target.value;
      saveSettings();
      renderKeyboard(els.preview, { osProfile: state.settings.osProfile, compact: true });
      if (state.currentLessonId) {
        renderKeyboard(els.liveKb, {
          osProfile: state.settings.osProfile,
          highlightKey: nextChar(),
        });
      }
    });

    document.getElementById("ergo-guard").addEventListener("change", (e) => {
      state.settings.ergoGuard = e.target.checked;
      saveSettings();
    });

    document.getElementById("sound-feedback").addEventListener("change", (e) => {
      state.settings.sound = e.target.checked;
      saveSettings();
    });

    els.typingArea.addEventListener("click", focusInput);
    document.addEventListener("keydown", handleKeydown);
    document.addEventListener("keyup", handleKeyup);
  }

  function init() {
    bindUI();
    buildMap();
    updateHeaderStats();
    updateDailySummary();
    renderKeyboard(els.preview, { osProfile: state.settings.osProfile, compact: true });

    const progBtn = document.getElementById("start-programming");
    if (progBtn) {
      updateProgrammingButton();
    }
  }

  init();
})();
