# Kinesis World

A small practice world for getting comfortable on the **Kinesis Advantage 360** — especially thumb keys and right-hand ergonomics when switching from a MacBook keyboard.

## Open it

No install. From this folder:

```bash
cd kinesis-world
python3 -m http.server 8765
```

Then open [http://localhost:8765](http://localhost:8765) in your browser.

Or open `index.html` directly in a browser (some features work best via a local server).

## How to use

1. **Start with Quest 1** — home row only, low cognitive load.
2. **Thumb Shores** — Space (right thumb) and Backspace (left thumb).
3. **Enter Cave** — right thumb Enter instead of pinky reach (MacBook habit).
4. **Shift Highlands** — capitals with thumb Shift.
5. **MacBook Ghost** — unlearn Command reach and awkward right-hand angles.
6. **Speed Plains** — longer text when basics feel automatic.

**Daily drill** (sidebar): ~10 minutes rotating through thumb + home + speed quests. Short daily reps beat long frustrated sessions.

## Settings

- **Mac vs Windows** — thumb cluster labels (⌘ vs Win). Remap in Kinesis SmartSet/Clique if yours differs.
- **Right-hand ergo guard** — warns when you hit keys that often cause pinky stretch on a flat keyboard (Enter, Shift, etc.).
- Progress saves in your browser (`localStorage`).

## Ergonomics (from Kinesis guidance)

- Relax **right thumb over Space**, **left thumb over Backspace** when idle.
- For far thumb keys, **move your hand/arm slightly** — don't stretch the thumb.
- If a thumb key hurts, lighten pressure or remap; consider talking to a clinician for persistent pain.

## Files

| File | Purpose |
|------|---------|
| `index.html` | Shell UI |
| `lessons.js` | Quest regions and lesson text |
| `keyboard.js` | Advantage-style layout + finger hints |
| `app.js` | Typing engine, progress, daily drill |
| `style.css` | Visual design |

This folder is unrelated to the main research repo — a personal typing trainer you can keep or copy anywhere.
