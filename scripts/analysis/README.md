# Publication analysis scripts

Thin drivers for manuscript-critical diagnostics. Each script calls `src/utilities` and writes tables or figures to a stable path.

- **registry/** — Experiment metadata, availability, flare–reference pairing.
- **forcing/** — Forcing and setup summaries.
- **initiation/** — First-ice onset and freezing-pathway metrics.
- **growth/** — Plume ridge, PSD stats, process-dominance.
- **impacts/** — Liquid depletion, radar/precipitation-facing metrics.
- **synthesis/** — Claim register, figure inventory.

Outputs go to `data/registry/` or paths documented in each script. Every paper figure should map to a script and output file here.
