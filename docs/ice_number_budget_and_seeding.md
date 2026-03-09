# Ice Number Budget and Seeding-Flare Ice Formation in COSMO-SPECS
#### TODO: rewrite the section for computing the tendencies into a latex section for the appendix of our article @article_draft/PolarCAP/ 
**Purpose:** The main question is **which processes contribute the most to ice crystal number growth and mass growth**. This document explains how to answer it using the COSMO-SPECS tendency decomposition, and how to interpret the terms correctly—especially the role of condensation (redistribution vs growth) and seeding (immersion freezing).  
**Source:** COSMO-SPECS core (`LM/specs/core`).

**Why CONDNFROD was checked:** In the meteogram process-fraction plots (View A: stacked-area “Liquid & ice process fractions” at stations S0, S1, S2, etc.), **CONDENSATION** (dark blue) appears as a large source on the **Ice Number sources** axis (fraction 0–1), especially after seeding (t > 0). That suggests condensation is adding ice crystal number—but physically, depositional growth adds mass to existing particles and should not create new ones. The variable **CONDNFROD** is the ice-number tendency associated with the condensation/deposition step, so it was unclear whether that non-zero “condensation” contribution in the plot could mean real new ice or something else (e.g. bin reclassification). The source code was therefore inspected to determine **what CONDNFROD actually includes**. The result: CONDNFROD is purely **bin reclassification** (sum over bins = 0). So the dark-blue “condensation” share in the **Ice Number** sources panel is **redistribution of existing ice between size bins**, not nucleation. Any net increase in total ice number must come from other terms (e.g. IMMERN, DEPON); see Section 2.

**Roadmap:** Section 1 introduces the assembled tendency equations and notation. Section 2 clarifies why the condensation term in the ice-number budget is redistribution only. Sections 3-6 then interpret which processes actually create or remove ice, how flare seeding enters the system, and how to rank contributions in practice.

---

## 1. The ice number budget: one equation

Ice particle number tendency (frozen drops, `NFROD`) is assembled in `cloudxd.f90` as:

```text
DNFROD = (CONDNFROD + KOLLNFROD + KOLLNFRODI + KOLLNFROD_INS + knf + IMMERN + DEPON + HOMN + dnfmelt) × DELTAT
```

In the notation of Section 1b, this is \(\Delta N_f = \Delta t\,\sum \dot{n}_f^{\mathrm{process}}\), with \(\dot{n}_f^{\mathrm{cond}}\) redistribution-only (sum over bins = 0). To answer **which processes contribute most to ice number and mass growth** (Section 3b), we therefore need the full set of process-wise rates and a clear interpretation of which terms are true sources, sinks, or only bin transfers.

---

## 1b. Complete tendency formulas (cloudxd.f90)

All hydrometeor and aerosol tendency equations are assembled in `cloudxd.f90` (main loop over bins `JJ`, `II`). Each increment is **rate × DELTAT**. **Number** concentration rates use \(\dot{n}\) (e.g. \(\dot{n}_f^{\mathrm{imm}} = \mathrm{d}N_f/\mathrm{d}t|_{\mathrm{imm}}\)); **mass** mixing-ratio rates use \(\dot{q}\) (e.g. \(\dot{q}_w^{\mathrm{cond}} = \mathrm{d}Q_w/\mathrm{d}t|_{\mathrm{cond}}\)). Subscripts identify the affected reservoir; superscripts identify the process.

### Notation

**State variables (subscripts: phase; superscripts: constituent)**  
Rates use the same index as the state variable: \(\dot{n}_w\), \(\dot{q}_w\) (droplet), \(\dot{n}_f\), \(\dot{q}_f\) (frozen), etc. Because \(\dot{n}\) already means number rate and \(\dot{q}\) already means mass rate, the subscript only tracks the reservoir, not N or Q again.

| Symbol | Meaning | Code name |
|--------|--------|-----------|
| \(N_w\) | Droplet number concentration | NW |
| \(Q_w\) | Droplet mass mixing ratio (total) | QW |
| \(Q_s^w\) | Soluble aerosol mass in droplets | QS |
| \(Q_a^w\) | Insoluble aerosol mass in droplets | QA |
| \(N_f\) | Frozen-drop (ice) number concentration | NFROD |
| \(Q_f\) | Frozen-drop total mass mixing ratio | QFROD |
| \(Q_i^f\) | Solid ice mass in frozen drops | QSFROD |
| \(Q_a^f\) | Aerosol mass in frozen drops | QAFROD |
| \(Q_w^f\) | Liquid-shell mass on frozen drops | QWFROD |
| \(N_\mathrm{ins}\) | Interstitial insoluble number | NWINS |
| \(Q_\mathrm{ins}\) | Interstitial insoluble mass | QAINS |
| \(n_\mathrm{inp}\) | INP number (per temperature bin) | ninp |

**Process superscripts for tendency rates \(\dot{n}\) (number) and \(\dot{q}\) (mass)**

| Superscript | Process | Examples (code) |
|-------------|---------|------------------|
| <span style="color:#0072B2">\(\mathrm{cond}\)</span> | <span style="color:#0072B2">Condensation / deposition + bin shift</span> | CONDN, CONDQ, CONDNFROD, CONDQFROD |
| <span style="color:#E69F00">\(\mathrm{coll}\)</span> | <span style="color:#E69F00">Liquid–liquid collision</span> | KOLLN, KOLLQ |
| <span style="color:#CC79A7">\(\mathrm{coll},f\)</span> | <span style="color:#CC79A7">Liquid–ice collision (riming; drop side)</span> | KOLLNI, KOLLQI |
| <span style="color:#999999">\(\mathrm{coll},\mathrm{ins}\)</span> | <span style="color:#999999">Liquid–insoluble collision (contact)</span> | KOLLN_INS, KOLLQ_INS |
| <span style="color:#999999">\(\mathrm{coll},f\) (on \(N_f,Q_f\))</span> | <span style="color:#999999">Liquid–ice → ice (contact/riming)</span> | KOLLNFROD, KOLLQFROD |
| <span style="color:#999999">\(\mathrm{coll},fi\)</span> | <span style="color:#999999">Contact freezing (liquid–ice → ice)</span> | KOLLNFRODI, KOLLQFRODI |
| <span style="color:#999999">\(\mathrm{coll},\mathrm{ins}\) (on \(N_f\))</span> | <span style="color:#999999">Liquid–INS → ice</span> | KOLLNFROD_INS, … |
| <span style="color:#D55E00">\(\mathrm{coll},ff\)</span> | <span style="color:#D55E00">Ice–ice collision</span> | knf, kqf, kqwf, kqsf, kqaf |
| <span style="color:#F0E442">\(\mathrm{brea}\)</span> | <span style="color:#F0E442">Breakup</span> | BREAN, BREAQ |
| <span style="color:#009E73">\(\mathrm{imm}\)</span> | <span style="color:#009E73">Immersion freezing</span> | IMMERN, IMMERQ |
| <span style="color:#56B4E9">\(\mathrm{hom}\)</span> | <span style="color:#56B4E9">Homogeneous freezing</span> | HOMN, HOMQ |
| <span style="color:#6A3D9A">\(\mathrm{dep}\)</span> | <span style="color:#6A3D9A">Deposition nucleation</span> | DEPON, DEPOQ, DEPOS, DEPOA |
| <span style="color:#8c564b">\(\mathrm{melt}\)</span> | <span style="color:#8c564b">Melting (ice → liquid)</span> | dnfmelt, dqfmelt; dnwmelt, dqwmelt |
| <span style="color:#b5e0f0">\(\mathrm{frz}\)</span> | <span style="color:#b5e0f0">Refreezing (liquid shell on ice)</span> | dqffrier |
| <span style="color:#9E77B5">\(\mathrm{dep},\mathrm{ins}\)</span> | <span style="color:#9E77B5">Deposition on interstitial</span> | deponi, depoqia |
| <span style="color:#333333">\(\mathrm{flare}\)</span> | <span style="color:#333333">Flare source</span> | FLARE_DNW, FLARE_DQW, … |

So \(\dot{n}_f^{\mathrm{imm}}\) = IMMERN and \(\dot{q}_w^{\mathrm{cond}}\) = CONDQ. In general, per-bin increments are \(\Delta N = \Delta t\,\sum \dot{n}^{\mathrm{process}}\) and \(\Delta Q = \Delta t\,\sum \dot{q}^{\mathrm{process}}\).

**Process colors (same as notebook `05-process-budget` View A)**  
The colors below match `utilities.style_profiles.PROC_COLORS` (Okabe–Ito palette), so the formulas and stacked-area plot can be read together:

<span style="color:#0072B2">■</span> cond · <span style="color:#E69F00">■</span> coll · <span style="color:#CC79A7">■</span> coll,f (riming) · <span style="color:#999999">■</span> coll,fi / coll,ins (ice) · <span style="color:#D55E00">■</span> coll,ff · <span style="color:#F0E442">■</span> brea · <span style="color:#009E73">■</span> imm · <span style="color:#56B4E9">■</span> hom · <span style="color:#6A3D9A">■</span> dep · <span style="color:#8c564b">■</span> melt · <span style="color:#b5e0f0">■</span> frz · <span style="color:#9E77B5">■</span> dep,ins · <span style="color:#333333">■</span> flare

---

### Liquid droplet number \(N_w\) and mass \(Q_w\)

$$
\Delta N_w = \Delta t\,\bigl(
  \dot{n}_w^{\mathrm{cond}} + \dot{n}_w^{\mathrm{coll}} + \dot{n}_w^{\mathrm{coll},f} + \dot{n}_w^{\mathrm{coll},\mathrm{ins}}
  + \dot{n}_w^{\mathrm{brea}} - \dot{n}_w^{\mathrm{imm}} - \dot{n}_w^{\mathrm{hom}}
  + \dot{n}_w^{\mathrm{melt}} + \dot{n}_w^{\mathrm{flare}}
\bigr)
$$

$$
\Delta Q_w = \Delta t\,\bigl(
  \dot{q}_w^{\mathrm{cond}} + \dot{q}_w^{\mathrm{coll}} + \dot{q}_w^{\mathrm{coll},f} + \dot{q}_w^{\mathrm{coll},\mathrm{ins}}
  + \dot{q}_w^{\mathrm{brea}} - \dot{q}_w^{\mathrm{imm}} - \dot{q}_w^{\mathrm{hom}}
  + \dot{q}_w^{\mathrm{melt}} + \dot{q}_w^{\mathrm{flare}}
\bigr)
$$

If \(\texttt{idepo} \ge 101\) (Diehl–Mitra 2015 deposition): subtract \(\Delta t\cdot \dot{n}_w^{\mathrm{dep}}\) from \(\Delta N_w\) and \(\Delta t\cdot \dot{q}_w^{\mathrm{dep}}\) from \(\Delta Q_w\).

### Liquid soluble / insoluble aerosol in droplets (\(Q_s^w\), \(Q_a^w\))

$$
\Delta Q_s^w = \Delta t\,\bigl(
  \dot{q}_{s^w}^{\mathrm{cond}} + \dot{q}_{s^w}^{\mathrm{coll}} + \dot{q}_{s^w}^{\mathrm{coll},f} + \dot{q}_{s^w}^{\mathrm{coll},\mathrm{ins}}
  + \dot{q}_{s^w}^{\mathrm{brea}} - \dot{q}_{s^w}^{\mathrm{imm}} - \dot{q}_{s^w}^{\mathrm{hom}}
  + \dot{q}_{s^w}^{\mathrm{melt}} + \dot{q}_{s^w}^{\mathrm{flare}}
\bigr)
$$

$$
\Delta Q_a^w = \Delta t\,\bigl(
  \dot{q}_{a^w}^{\mathrm{cond}} + \dot{q}_{a^w}^{\mathrm{coll}} + \dot{q}_{a^w}^{\mathrm{coll},f} + \dot{q}_{a^w}^{\mathrm{coll},\mathrm{ins}}
  + \dot{q}_{a^w}^{\mathrm{brea}} - \dot{q}_{a^w}^{\mathrm{imm}} - \dot{q}_{a^w}^{\mathrm{hom}}
  + \dot{q}_{a^w}^{\mathrm{melt}} + \dot{q}_{a^w}^{\mathrm{flare}}
\bigr)
$$

If \(\texttt{idepo} \ge 101\): subtract \(\Delta t\cdot \dot{q}_{s^w}^{\mathrm{dep}}\) from \(\Delta Q_s^w\) and \(\Delta t\cdot \dot{q}_{a^w}^{\mathrm{dep}}\) from \(\Delta Q_a^w\).

### Ice (frozen drop) number \(N_f\) and masses \(Q_f\), \(Q_i^f\), \(Q_a^f\), \(Q_w^f\)

$$
\Delta N_f = \Delta t\,\bigl(
  \dot{n}_f^{\mathrm{cond}} + \dot{n}_f^{\mathrm{coll},f} + \dot{n}_f^{\mathrm{coll},fi} + \dot{n}_f^{\mathrm{coll},\mathrm{ins}}
  + \dot{n}_f^{\mathrm{coll},ff} + \dot{n}_f^{\mathrm{imm}} + \dot{n}_f^{\mathrm{dep}} + \dot{n}_f^{\mathrm{hom}} + \dot{n}_f^{\mathrm{melt}}
\bigr)
$$

*Color-coded (same as View A):*  
Δ*N*<sub>f</sub> = Δ*t* · ( <span style="color:#0072B2">*ṅ*<sub>f</sub><sup>cond</sup></span> + <span style="color:#999999">*ṅ*<sub>f</sub><sup>coll,f</sup></span> + <span style="color:#999999">*ṅ*<sub>f</sub><sup>coll,fi</sup></span> + <span style="color:#999999">*ṅ*<sub>f</sub><sup>coll,ins</sup></span> + <span style="color:#D55E00">*ṅ*<sub>f</sub><sup>coll,ff</sup></span> + <span style="color:#009E73">*ṅ*<sub>f</sub><sup>imm</sup></span> + <span style="color:#6A3D9A">*ṅ*<sub>f</sub><sup>dep</sup></span> + <span style="color:#56B4E9">*ṅ*<sub>f</sub><sup>hom</sup></span> + <span style="color:#8c564b">*ṅ*<sub>f</sub><sup>melt</sup></span> )

$$
\Delta Q_f = \Delta t\,\bigl(
  \dot{q}_f^{\mathrm{cond}} + \dot{q}_f^{\mathrm{coll},f} + \dot{q}_f^{\mathrm{coll},fi} + \dot{q}_f^{\mathrm{coll},\mathrm{ins}}
  + \dot{q}_f^{\mathrm{coll},ff} + \dot{q}_f^{\mathrm{imm}} + \dot{q}_f^{\mathrm{dep}} + \dot{q}_f^{\mathrm{hom}}
  + \dot{q}_f^{\mathrm{melt}} + \dot{q}_f^{\mathrm{frz}}
\bigr)
$$

*Color-coded:*  
Δ*Q*<sub>f</sub> = Δ*t* · ( <span style="color:#0072B2">*q̇*<sub>f</sub><sup>cond</sup></span> + <span style="color:#999999">*q̇*<sub>f</sub><sup>coll,f</sup></span> + <span style="color:#999999">*q̇*<sub>f</sub><sup>coll,fi</sup></span> + <span style="color:#999999">*q̇*<sub>f</sub><sup>coll,ins</sup></span> + <span style="color:#D55E00">*q̇*<sub>f</sub><sup>coll,ff</sup></span> + <span style="color:#009E73">*q̇*<sub>f</sub><sup>imm</sup></span> + <span style="color:#6A3D9A">*q̇*<sub>f</sub><sup>dep</sup></span> + <span style="color:#56B4E9">*q̇*<sub>f</sub><sup>hom</sup></span> + <span style="color:#8c564b">*q̇*<sub>f</sub><sup>melt</sup></span> + <span style="color:#b5e0f0">*q̇*<sub>f</sub><sup>frz</sup></span> )

$$
\Delta Q_i^f = \Delta t\,\bigl(
  \dot{q}_{i^f}^{\mathrm{cond}} + \dot{q}_{i^f}^{\mathrm{coll},f} + \dot{q}_{i^f}^{\mathrm{coll},fi} + \dot{q}_{i^f}^{\mathrm{coll},\mathrm{ins}}
  + \dot{q}_{i^f}^{\mathrm{coll},ff} + \dot{q}_{i^f}^{\mathrm{imm}} + \dot{q}_{i^f}^{\mathrm{dep}} + \dot{q}_{i^f}^{\mathrm{hom}} + \dot{q}_{i^f}^{\mathrm{melt}}
\bigr)
$$

$$
\Delta Q_a^f = \Delta t\,\bigl(
  \dot{q}_{a^f}^{\mathrm{cond}} + \dot{q}_{a^f}^{\mathrm{coll},f} + \dot{q}_{a^f}^{\mathrm{coll},fi} + \dot{q}_{a^f}^{\mathrm{coll},\mathrm{ins}}
  + \dot{q}_{a^f}^{\mathrm{coll},ff} + \dot{q}_{a^f}^{\mathrm{imm}} + \dot{q}_{a^f}^{\mathrm{dep}} + \dot{q}_{a^f}^{\mathrm{hom}} + \dot{q}_{a^f}^{\mathrm{melt}}
\bigr)
$$

$$
\Delta Q_w^f = \Delta t\,\bigl(
  \dot{q}_{w^f}^{\mathrm{cond}} + \dot{q}_{w^f}^{\mathrm{coll},f} + \dot{q}_{w^f}^{\mathrm{coll},\mathrm{ins}}
  + \dot{q}_{w^f}^{\mathrm{coll},ff} + \dot{q}_{w^f}^{\mathrm{imm}} + \dot{q}_{w^f}^{\mathrm{dep}} + \dot{q}_{w^f}^{\mathrm{hom}} + \dot{q}_{w^f}^{\mathrm{melt}}
\bigr)
$$

Note: \(\dot{n}_f^{\mathrm{cond}}\) (CONDNFROD) sums to zero over all bins (redistribution only); see Section 2.

### Interstitial insoluble aerosol (\(N_\mathrm{ins}\), \(Q_\mathrm{ins}\))

$$
\Delta N_\mathrm{ins}(i_\mathrm{ins}, i_t) = \Delta t\,\bigl(
  \dot{n}_\mathrm{ins}^{\mathrm{coll}} + \dot{n}_\mathrm{ins}^{\mathrm{dep},\mathrm{ins}} + \dot{n}_\mathrm{ins}^{\mathrm{flare}}
\bigr)
$$

$$
\Delta Q_\mathrm{ins}(i_\mathrm{ins}, i_t) = \Delta t\,\bigl(
  \dot{q}_\mathrm{ins}^{\mathrm{coll}} + \dot{q}_\mathrm{ins}^{\mathrm{dep},\mathrm{ins}} + \dot{q}_\mathrm{ins}^{\mathrm{flare}}
\bigr)
$$

Indices: \(i_\mathrm{ins} = 1,\ldots,\texttt{SIMAX}\), \(i_t = 1,\ldots,\texttt{ITMAX}\).

### INP number tendency

$$
\Delta n_\mathrm{inp}(i_T) = \Delta t \cdot \dot{n}_\mathrm{inp}^{\mathrm{imm}}(i_T)
$$

\(i_T = 1,\ldots,\texttt{NINPmax}\); \(\dot{n}_\mathrm{inp}^{\mathrm{imm}}\) is the sink of INP by immersion freezing (code: imfrni).

### Code mapping (symbol → Fortran)

| \(\dot{n}\) / \(\dot{q}\) symbol | Code name |
|--------------|-----------|
| <span style="color:#0072B2">\(\dot{n}_w^{\mathrm{cond}}\), \(\dot{q}_w^{\mathrm{cond}}\), \(\dot{q}_{s^w}^{\mathrm{cond}}\), \(\dot{q}_{a^w}^{\mathrm{cond}}\)</span> | CONDN, CONDQ, CONDS, CONDA |
| <span style="color:#0072B2">\(\dot{n}_f^{\mathrm{cond}}\), \(\dot{q}_f^{\mathrm{cond}}\), \(\dot{q}_{i^f}^{\mathrm{cond}}\), \(\dot{q}_{a^f}^{\mathrm{cond}}\), \(\dot{q}_{w^f}^{\mathrm{cond}}\)</span> | CONDNFROD, CONDQFROD, CONDSFROD, CONDAFROD, CONDQWFROD |
| <span style="color:#E69F00">\(\dot{n}^{\mathrm{coll}}\), \(\dot{q}^{\mathrm{coll}}\) (liquid)</span> | KOLLN, KOLLQ, KOLLS, KOLLA |
| <span style="color:#CC79A7">\(\dot{n}^{\mathrm{coll},f}\), \(\dot{q}^{\mathrm{coll},f}\) (liquid)</span> | KOLLNI, KOLLQI, KOLLSI, KOLLAI |
| <span style="color:#999999">\(\dot{n}^{\mathrm{coll},\mathrm{ins}}\), \(\dot{q}^{\mathrm{coll},\mathrm{ins}}\) (liquid)</span> | KOLLN_INS, KOLLQ_INS, KOLLS_INS, KOLLA_INS |
| <span style="color:#999999">\(\dot{n}_f^{\mathrm{coll},f}\), \(\dot{q}_f^{\mathrm{coll},f}\), …</span> | KOLLNFROD, KOLLQFROD, KOLLSFROD, KOLLAFROD; kollqwf |
| <span style="color:#999999">\(\dot{n}_f^{\mathrm{coll},fi}\), \(\dot{q}_f^{\mathrm{coll},fi}\), …</span> | KOLLNFRODI, KOLLQFRODI, … |
| <span style="color:#999999">\(\dot{n}_f^{\mathrm{coll},\mathrm{ins}}\), \(\dot{q}_f^{\mathrm{coll},\mathrm{ins}}\), …</span> | KOLLNFROD_INS, KOLLQFROD_INS, … |
| <span style="color:#D55E00">\(\dot{n}_f^{\mathrm{coll},ff}\), \(\dot{q}_f^{\mathrm{coll},ff}\), \(\dot{q}_{w^f}^{\mathrm{coll},ff}\), …</span> | knf, kqf, kqwf, kqsf, kqaf |
| <span style="color:#F0E442">\(\dot{n}^{\mathrm{brea}}\), \(\dot{q}^{\mathrm{brea}}\)</span> | BREAN, BREAQ, BREAS, BREAA |
| <span style="color:#009E73">\(\dot{n}^{\mathrm{imm}}\), \(\dot{q}^{\mathrm{imm}}\)</span> | IMMERN, IMMERQ, IMMERS, IMMERA |
| <span style="color:#56B4E9">\(\dot{n}^{\mathrm{hom}}\), \(\dot{q}^{\mathrm{hom}}\)</span> | HOMN, HOMQ, HOMS, HOMA |
| <span style="color:#6A3D9A">\(\dot{n}^{\mathrm{dep}}\), \(\dot{q}^{\mathrm{dep}}\)</span> | DEPON, DEPOQ, DEPOS, DEPOA |
| <span style="color:#8c564b">\(\dot{n}_f^{\mathrm{melt}}\), \(\dot{n}_w^{\mathrm{melt}}\), \(\dot{q}_f^{\mathrm{melt}}\), \(\dot{q}_{w^f}^{\mathrm{melt}}\), \(\dot{q}_w^{\mathrm{melt}}\), …</span> | dnfmelt, dqfmelt, dqfwmelt, dnwmelt, dqwmelt, … |
| <span style="color:#b5e0f0">\(\dot{q}_f^{\mathrm{frz}}\)</span> | dqffrier |
| <span style="color:#9E77B5">\(\dot{n}_\mathrm{ins}^{\mathrm{dep},\mathrm{ins}}\), \(\dot{q}_\mathrm{ins}^{\mathrm{dep},\mathrm{ins}}\)</span> | deponi, depoqia |
| <span style="color:#333333">\(\dot{n}^{\mathrm{flare}}\), \(\dot{q}^{\mathrm{flare}}\)</span> | FLARE_DNW, FLARE_DQW, FLARE_DQS, FLARE_DQA, FLARE_DNI, FLARE_DQIA |

Source: `cloudxd.f90` (tendency assembly and meteogram sums in the lower part of the file).

---

## 2. Interpretation note: condensation is not the seeding mechanism

For the seeding question, **CONDNFROD** is an important cautionary term, but it is not the main story. The main scientific question is not whether condensation redistributes ice between bins; it is **which processes create new ice after seeding, and which processes then grow that ice mass**.

The key source-code result remains:

- In `cond_mixxd.f90`, depositional growth changes **mass** tendencies (`CONDQFROD`, `CONDQWFROD`) but leaves ice number unchanged:

```fortran
CONDNFROD(JJ,II) = 0.D0
```

- In `COND_SHIFTxd`, the model then reclassifies particles between mass bins and overwrites the number tendency with a **bin-shift** term:

```fortran
CONDN(JJ,II) = (NWver(JJ,II) - NW(JJ,II,IP)) / DELTAT
```

So **CONDNFROD** is redistribution only: it can be large in spectral budgets, but its sum over all bins is zero and it does **not** nucleate new ice. For the seeding storyline, this term should therefore be treated as an **interpretation guardrail**, not as a dominant physical pathway.

---

## 3. Process taxonomy for seeding experiments

For the seeding experiments it is useful to separate five process classes:

| Class | Physical role | Main terms | Interpretation for seeding |
|------|------|------|------|
| **Reservoir preparation** | Adds or prepares INP / aerosol / droplets that later participate in freezing | `flare_burn`, `inp_parxd_flare`, `ninp` | Sets up the seeding perturbation |
| **Primary ice initiation** | Creates new ice number from aerosol/INP activation | `IMMERN`, `DEPON`, `HOMN` | This is the key class for identifying the dominant nucleation pathway |
| **Freezing conversion / transfer** | Converts existing droplets into frozen particles after collision/contact processes | `KOLLNFROD`, `KOLLNFRODI`, `KOLLNFROD_INS` | Can add ice number, but usually as a secondary pathway after reservoirs already exist |
| **Post-initiation growth** | Grows the mass of already existing ice | `CONDQFROD`, `CONDQWFROD`, `DEPOQ`, `KOLLQFROD*`, `kqf`, `dqffrier` | Dominates mass growth rather than first activation |
| **Redistribution / sinks** | Moves particles between bins or removes them | `CONDNFROD`, `dnfmelt`, `dqfmelt`, `dqfwmelt`, negative collision terms | Needed for interpretation, but not the primary seeding signal |

This separation is the most useful way to read the tendency decomposition for the flare case: first ask **who creates new ice number**, then ask **who grows that ice mass**.

---

## 4. Which terms actually create new ice number?

### 4.1. Primary nucleation / activation terms

These are the terms to inspect first when the question is: **Which nucleation process is dominant in the seeding experiment?**

| Term | Source-code meaning | Uses flare `ninp`? | Interpretation |
|------|------|------|------|
| **IMMERN** | Immersion freezing of droplets | **Yes** | Direct flare-to-ice pathway |
| **DEPON** | Deposition nucleation | No | Background or environment-driven ice initiation |
| **HOMN** | Homogeneous freezing | No | Cold-end background pathway |

**IMMERN** is the clearest seeding-specific initiation term. In `freezing.f90`, `ice_binTxd` sums the INP classes for which `TABS <= TFR(it)`, converts that available `ninp_T` into a freezing rate, and distributes the freezing over eligible droplet bins:

```fortran
if(TABS.le.tfr(it)) then
  nd_fr = nd_fr + ninp_T(it,ip)
  imfrni(it) = -ninp_T(it,ip)
endif
...
IMMERN(J,I) = rat_fr*NW(J,I,IP)
IMMERQ(J,I) = rat_fr*QW(J,I,IP)
```

That means the flare signal reaches the ice-number budget primarily through **flare INP -> `ninp` -> `IMMERN`**.

### 4.2. Deposition nucleation is real, but not flare-fed in the same way

Both deposition schemes create new ice number, but neither uses the flare `ninp` reservoir:

- **`depoxd`** (`idepo == 1`) nucleates ice from **interstitial insoluble aerosol** (`NWINS`, `QAINS`) and writes `DEPON`, `DEPOQ`, etc.
- **`depoxd_DM15`** (`idepo = 101...107`) activates a **fraction of liquid droplets** (`NW`, `QW`, `QA`, `QS`) under ice supersaturation and writes `DEPON`, `DEPOQ`, `DEPOA`, `DEPOS`.

So deposition nucleation can absolutely matter for total ice-number production, but it is **not** the direct flare-INP pathway.

### 4.3. Collision/contact freezing terms add ice number by conversion

Collision-related freezing terms can also increase `N_f`, but physically they are best treated as **conversion pathways**, not the primary flare activation pathway:

- `KOLLNFROD`, `KOLLQFROD` from `koll_contactxd_DM15.f90`
- `KOLLNFRODI`, `KOLLQFRODI`, `kollqwf` from `koll_ice_dropsxd.f90`
- `KOLLNFROD_INS`, `KOLLQFROD_INS` from `koll_insolxd.f90`

These terms convert already existing droplets into frozen particles after contact/collision with ice or insoluble particles. They can become important after ice is already present, so they are better interpreted as **secondary ice-number production or freezing conversion**, not as the first seeding trigger.

### 4.4. Practical conclusion for number formation

For the seeding experiments, the number-source hierarchy should be read in this order:

1. **IMMERN**: first candidate for the dominant flare-induced nucleation pathway
2. **DEPON**: competing background/environmental nucleation pathway
3. **HOMN**: cold-end background nucleation
4. **KOLLNFROD***: secondary conversion/freezing pathways that can amplify ice number after initiation

`CONDNFROD` should stay outside this ranking because it does not create new particles.

---

## 5. Which terms grow ice mass after initiation?

Once ice exists, the dominant question changes from **formation** to **growth**. The relevant mass pathways are different from the number-initiation pathways.

The main ice-mass growth terms are:

| Term group | Physical meaning | Main role |
|------|------|------|
| `CONDQFROD`, `CONDQWFROD` | Depositional growth plus bin shift | Growth of existing ice / wet shell mass |
| `DEPOQ` | Mass transfer associated with deposition nucleation | Mass added when new deposition-frozen particles form |
| `KOLLQFROD`, `KOLLQFRODI`, `KOLLQFROD_INS`, `kollqwf` | Collision/contact/riming-related ice or shell growth | Growth and conversion after collisions |
| `kqf`, `kqwf` | Ice-ice collisional growth / redistribution | Post-initiation growth and transfer |
| `dqffrier` | Refreezing of liquid shell | Converts shell liquid to frozen mass |

The practical distinction is:

- **ice number production**: mainly `IMMERN`, `DEPON`, `HOMN`, plus collision/contact-freezing number terms
- **ice mass growth**: mainly deposition/growth, riming/contact growth, aggregation/ice-ice collision, and refreezing

This distinction is central for the flare analysis: the process that produces **more ice crystals** need not be the same process that produces **most ice mass**.

---

## 6. Recommended analysis workflow for the seeding runs

### 6.1. Separate formation from growth

Use two different rankings:

1. **Ice-number formation ranking**  
   Rank only terms that can create new ice number:
   `IMMERN`, `DEPON`, `HOMN`, `KOLLNFROD`, `KOLLNFRODI`, `KOLLNFROD_INS`, optionally `knf` if relevant in your setup.

2. **Ice-mass growth ranking**  
   Rank the mass terms:
   `CONDQFROD`, `CONDQWFROD`, `DEPOQ`, `KOLLQFROD`, `KOLLQFRODI`, `KOLLQFROD_INS`, `kqf`, `kqwf`, `dqffrier`.

### 6.2. Isolate the flare signal

Restrict the analysis to times after seeding start and compare:

- pre-seeding vs post-seeding
- seeded vs unseeded experiment
- `IMMERN` vs `DEPON` in the same time-height window

If the flare acts primarily through `ninp`, then the cleanest signature should be an enhancement in **IMMERN** first, followed by growth terms that build up ice mass.

### 6.3. Keep redistribution separate

For interpretation:

- **`CONDNFROD`**: keep for spectral redistribution diagnostics, but exclude from total ice-number source ranking
- **melting terms** (`dnfmelt`, `dqfmelt`, `dqfwmelt`) and negative collision terms: keep as sinks, not sources

### 6.4. Output mapping

Map the model diagnostics to your post-processing variables, for example:

- `DIMMERN_sum` -> immersion-freezing number source
- `Ddeponf_sum` -> deposition-nucleation number source
- `DCONDNFROD_sum` -> redistribution-only number term
- `DCONDQFROD_sum`, `DCONDQWFROD_sum` -> condensation/deposition mass-growth terms

This way, View A / View B can be read with the right storyline: **first activation/initiation, then conversion and growth, with redistribution kept separate**.

---

## 7. Source references

- **cloudxd.f90:** DNFROD equation; call order (flare_burn → depoxd/depoxd_DM15 → ice_binTxd/imfr_inpxd/ice_asxd); CONDNFROD, IMMERN, DEPON in tendency and diagnostics.
- **cond_mixxd.f90:** CONDNFROD set to 0; call to COND_SHIFTxd with CONDNFROD.
- **cond_shiftxd.f90:** CONDN (CONDNFROD) overwritten as (NWver − NW) / DELTAT after LDM redistribution.
- **flare.f90:** flare_burn updates ninp_T/ninp via inp_parxd_flare.
- **freezing.f90:** ice_binTxd(ninp → IMMERN, IMMERQ).
- **depoxd.f90, depoxd_DM15.f90:** DEPON from interstitial aerosol or droplets (no ninp).

---

## Appendix A. Notebook 05-process-budget: investigation text for markdown cells

The notebook **`notebooks/05-process-budget.ipynb`** implements this investigation. The narrative (main question, View A/B interpretation, answers for Ice Number / Ice Mass / Liquid) is embedded in the notebook’s markdown cells. If you need to **re-apply** this text (e.g. after clearing cells or using another copy of the notebook), use the sections below. If the notebook fails to save or open due to truncated outputs, use **Kernel → Restart & Clear Output** (or **Edit → Clear All Outputs**), then save.

### A.1. First cell (after the title) – add after "Disentangles which..."

**Main question:** Which processes contribute the most to **ice crystal number growth** and **ice crystal mass growth** (and for liquid)? This notebook implements the investigation in this document: we use the process tendency terms (rates) that sum to DNFROD/DQFROD, normalise by source/sink, and rank contributions. View A (stacked-area fractions) and View B (stacked bars by height) show which process dominates; the doc explains how to interpret the **condensation** term (redistribution vs growth) and the role of **seeding** (immersion freezing).

### A.2. View A header (before the View A plots) – replace or add after "## View A – Normalised stacked area..."

**What this view shows:** Height-averaged, bin-summed process tendencies, split into **sources** (positive) and **sinks** (negative), normalised so that at each time the stacked area shows each process’s **fraction** of the total source or sink. Time is coarsened to reduce noise. The dashed line is seeding start (12:30).

**Answering “which processes contribute most?”:** The stacked fractions are exactly the process tendency terms (e.g. CONDNFROD, IMMERN, DEPON, …) normalised per timestep. So the **relative area** of each colour is that process’s contribution. For **ice number**: the dark-blue **CONDENSATION** slice is **CONDNFROD** — in the source code this is **redistribution only** (sum over bins = 0), so it does **not** add total ice number. For total ice number growth, treat it as zero and look at the other processes (e.g. immersion freezing, contact freezing, deposition nucleation). For **ice mass**, the condensation term is depositional growth + bin shift combined and does contribute to mass growth.

### A.3. After the View A Liquid & ice plots (stacked_area_liquid_ice_*)

**What the plot showed:** View A — Liquid & ice process fractions (cloud bins): four columns (Liquid Number, Ice Number, Liquid Mass, Ice Mass), each with sources (top, 0–1) and sinks (bottom, 0 to −1) over time since seeding.

**Answer (Ice Number):** The dark-blue **CONDENSATION** in the **Ice Number sources** panel is CONDNFROD. In COSMO-SPECS this is **bin reclassification only** (particles moving between size bins when they grow by deposition); **sum over all bins = 0**, so it does **not** increase total ice crystal number. The processes that actually add ice number are the other colours (e.g. immersion freezing, contact freezing, deposition nucleation). After seeding, **immersion freezing** (flare INP → ice) is the main nucleation source for extra ice number (Section 4).

**Answer (Ice Mass):** The condensation slice for ice mass is depositional growth + shift combined; it is a real contribution to ice mass growth.

**Answer (Liquid):** For liquid number/mass, condensation is the usual vapor–liquid process; interpretation is standard.

### A.4. Optional: before View B

**View B** shows the same process tendency terms averaged over **time** and plotted as stacked bars by **height**. So you see which processes dominate at which levels. The same interpretation applies: for ice number, CONDNFROD is redistribution only (exclude from total number growth); for ice mass, condensation is growth+shift.
