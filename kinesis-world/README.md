# Kinesis World

A small practice world for getting comfortable on the **Kinesis Advantage 360** — especially thumb keys and right-hand ergonomics when switching from a MacBook keyboard.

Includes a **programming track** for Python, bash, Fortran, markdown, Neovim, and tmux.

## Open it

No install. From this folder:

```bash
cd kinesis-world
python3 -m http.server 8765
```

Then open [http://localhost:8765](http://localhost:8765) in your browser.

Or open `index.html` directly in a browser (some features work best via a local server).

## Tracks

### Foundation (quests 1–15)

Home row → thumbs → Enter → Shift → Mac habits → speed prose.

### Programming (quests 16–45)

Unlocks after **Shift Highlands · quest 2** (or use **Programming track** on the welcome screen).

| Region | What you practice |
|--------|-------------------|
| **Code Glyphs** | `()[]{}`, operators, `\|` pipes, `> >>` redirects, quotes |
| **Python Grove** | `def`, imports, blocks, indentation (`Tab` = right thumb) |
| **Bash Dock** | paths, `$VAR`, pipes, redirects, loops, shebang |
| **Markdown Forge** | `#` headers, links, ` ``` ` fences, tables |
| **Fortran Lab** | `program`, `implicit none`, `do` loops, modules |
| **Vim & Tmux Dojo** | `:w` `:split`, search/replace, lua config, `tmux split-window` |

**Markers in lessons**

- `⏎` in the source becomes a line break — press **Enter** (right thumb)
- `→` becomes **Tab** (right thumb on Kinesis) for indentation

## Daily drills

- **10-min foundation drill** — home row, thumbs, speed
- **10-min dev drill** — symbols, Python blocks, bash pipes, vim ex commands

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
| `app.js` | Typing engine, progress, daily drills |
| `style.css` | Visual design |

This folder is unrelated to the main research repo — a personal typing trainer you can keep or copy anywhere.
