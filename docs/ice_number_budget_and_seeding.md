# Ice Number Budget and Seeding-Flare Ice Formation in COSMO-SPECS

**Purpose:** The main question is **which processes contribute the most to ice crystal number growth and mass growth**. This document explains how to answer it using the COSMO-SPECS tendency decomposition, and how to interpret the terms correctly—especially the role of condensation (redistribution vs growth) and seeding (immersion freezing).  
**Source:** COSMO-SPECS core (`LM/specs/core`).

**Why CONDNFROD was checked:** In the meteogram process-fraction plots (View A: stacked-area “Liquid & ice process fractions” at stations S0, S1, S2, etc.), **CONDENSATION** (dark blue) appears as a large source on the **Ice Number sources** axis (fraction 0–1), especially after seeding (t > 0). That suggests condensation is adding ice crystal number—but physically, depositional growth adds mass to existing particles and should not create new ones. The variable **CONDNFROD** is the ice-number tendency associated with the condensation/deposition step, so it was unclear whether that non-zero “condensation” contribution in the plot could mean real new ice or something else (e.g. bin reclassification). The source code was therefore inspected to determine **what CONDNFROD actually includes**. The result: CONDNFROD is purely **bin reclassification** (sum over bins = 0). So the dark-blue “condensation” share in the **Ice Number** sources panel is **redistribution of existing ice between size bins**, not nucleation. Any net increase in total ice number must come from other terms (e.g. IMMERN, DEPON); see Section 2.

---

## 1. The ice number budget: one equation

Ice particle number tendency (frozen drops, `NFROD`) is assembled in `cloudxd.f90` as:

```text
DNFROD = (CONDNFROD + KOLLNFROD + KOLLNFRODI + KOLLNFROD_INS + knf + IMMERN + DEPON + HOMN + dnfmelt) × DELTAT
```

To answer **which processes contribute most to ice number and mass growth** (Section 3b), we need to know which terms **create** new ice and which only **redistribute** existing particles or act as sinks. The following sections set that up.

---

## 2. CONDNFROD: redistribution only (no new particles)

**CONDNFROD** is the ice number tendency from the condensation/deposition and bin-shift part of the microphysics. Its name can suggest “condensation adding ice number,” but in the source it does **not** create new particles.

**In the condensation step** (`cond_mixxd.f90`): Only **mass** tendencies are set from vapor depositing onto existing ice (`CONDQFROD`, `CONDQWFROD`). Number is explicitly left unchanged:

```fortran
CONDNFROD(JJ,II) = 0.D0
```

**In the bin-shift step** (`COND_SHIFTxd`): The routine takes the updated mass per bin and, via the **Linear Discrete Method (LDM)**, reclassifies particles into the correct mass bins. It keeps total number unchanged (`NWneu = NW(JJ,II,IP)` in the source bin) and only moves number between bins. After the shift it overwrites CONDNFROD:

```fortran
CONDN(JJ,II) = (NWver(JJ,II) - NW(JJ,II,IP)) / DELTAT
```

So CONDNFROD is the **per-bin number tendency from reclassification**: some bins gain particles (positive), others lose (negative). **Sum over all bins = 0**—no creation or destruction.

**Takeaway:** For **total** ice number, CONDNFROD contributes nothing. Any net increase must come from other terms (nucleation, collisions, etc.). In spectral or per-bin budgets, CONDNFROD can be non-zero; do not interpret it as a nucleation source.

**How this affects ranking process contributions (Section 3b):** When you rank which processes contribute most to **ice number** growth, treat CONDNFROD as **zero** for the total (it’s redistribution only). For **ice mass**, the condensation tendencies CONDQFROD and CONDQWFROD are also overwritten in `COND_SHIFTxd`: what you get in the output is **growth + shift combined** per bin (depositional mass growth plus reclassification). You can still use that as the single “condensation/deposition” contribution when ranking. If you ever need to separate **growth-only** from **shift-only** for mass (e.g. to isolate the depositional growth parameterisation), you would need the model to write out the mass tendencies **before** the call to COND_SHIFTxd (e.g. CONDQFROD_GROWTH, CONDQWFROD_GROWTH), or an approximate post-processing from spectral tendencies and concentrations.

---

## 3. Processes that create or destroy ice number

| Term | Role | Creates/destroys? | Relevant for flare? |
|------|------|--------------------|----------------------|
| **IMMERN** | Immersion freezing (droplets freeze via INP) | Yes (source) | **Yes** — uses **ninp** |
| **DEPON** | Deposition nucleation (new ice from vapor) | Yes (source) | No (uses NWINS or NW,QW) |
| **HOMN** | Homogeneous freezing | Yes (source) | No |
| **knf** | Ice–ice collision (shattering, etc.) | Can be + or − | No |
| **KOLLNFROD*** | Liquid–ice collision | Yes | Indirect |
| **dnfmelt** | Ice melt | Sink (negative) | No |
| **CONDNFROD** | Bin reclassification after growth | **No** — sum = 0 | No |

So for **“where does extra ice number come from after seeding?”** the main **nucleation** terms are **IMMERN** (flare-driven) and **DEPON** (environment-driven).

---

## 3b. Which processes contribute most to ice number and mass growth?

**Main question:** Which processes contribute the most to ice crystal number growth and mass growth? That is exactly what the tendency decomposition is for: the terms in the DNFROD and DQFROD equations are the process-wise contributions. Ranking them (by magnitude, summed over bins and possibly over time) tells you what drives net ice number and mass change.

**What you need to rank process contributions**  
You need the **process tendency terms** (the rates that go into DNFROD and DQFROD), not only the **state** (concentrations). Spectral number and mass concentrations (NF, NW, QF, QW, QFW, QWA, QFA, etc.) tell you *how much* is in each bin; to answer “which process contributes most to growth” you need the **rates** from each process (e.g. IMMERN, DEPON, CONDNFROD, KOLLNFROD, … for number; IMMERQ, DEPOQ, CONDQFROD, … for mass). Those are the terms that sum to DNFROD and DQFROD.

**How spectral concentrations help**  
They help for consistency (e.g. Δ(sum NF) vs sum of DNFROD), for forming totals when you have spectral tendencies (sum over bins), and for per-bin attribution. They do **not** replace the need for tendency terms: “X% from immersion, Y% from deposition” comes from the **rates**, not from NF/QF alone.

**Practical recipe to rank process contributions**

1. **Get the process tendency terms** that feed into DNFROD (number) and DQFROD (mass)—e.g. from the same fields used in the View A stacked-area plot or from the model’s tendency output.
2. **Sum each term over all bins** (and over your time window if you want time-integrated contribution).  
   - **Ice number:** Treat **CONDNFROD** as **zero** when computing *total* number growth (Section 2: it’s redistribution only; sum over bins = 0).  
   - **Ice mass:** Use the “condensation” term (CONDQFROD/CONDQWFROD) as the single deposition/growth contribution; in current output it is **growth + shift combined** (Section 2). If you need growth-only vs shift-only for mass, you would need the model to output the mass tendencies before the call to COND_SHIFTxd, or an approximate post-processing from spectral tendencies and concentrations.
3. **Rank** the summed terms by absolute value (or by signed value for sources vs sinks). The largest terms are the main contributors to ice number growth and mass growth in that period.

So: to know which processes contribute most to growth you need the **process tendency terms** (the rates). Spectral concentrations (NF, QF, etc.) support consistency checks and per-bin analysis but do not replace them. If your View A already uses those tendencies, you have what you need; Section 2 ensures CONDNFROD and the mass condensation terms are interpreted correctly when ranking.

---

## 4. The seeding flare: INP → immersion freezing

The flare does **not** create ice directly. It adds **ice-nucleating particles (INP)** to the temperature-binned array **ninp_T** in the flare cell during burn intervals (`flare.f90`, `lflare_inp`). In `cloudxd`, the same array is passed as **ninp** (level slice).

**Call order in cloudxd:**

1. **flare_burn**(..., ninp, ...) — If time is in a burn window, INP are added via `inp_parxd_flare` and distributed over temperature classes TFR; **ninp** is updated in place.
2. **Deposition nucleation** — `depoxd` (idepo==1) or `depoxd_DM15` (idepo 101–107). These use **interstitial aerosol** (NWINS) or **liquid droplets** (NW, QW), **not** ninp. So deposition can add ice number but is **not** fed by the flare.
3. **Immersion freezing** — `ice_binTxd` (iimfr 101–107), `imfr_inpxd` (iimfr 11), or `ice_asxd` (iimfr 13). These use **ninp**: for each temperature class with TABS ≤ TFR(it), INP in that class can freeze droplets. They output **IMMERN** (number) and **IMMERQ** (mass). So when the flare increases **ninp**, the next call to the immersion routine produces more **IMMERN** and thus more ice crystals.

**Conclusion:** After seeding start (e.g. 12:30), the dominant process that turns **flare INP into extra ice number** is **immersion freezing (IMMERN)**. Deposition nucleation (DEPON) can also add ice if the environment is ice-supersaturated, but it does not use the flare’s ninp.

---

## 5. Deposition nucleation in short

- **depoxd** (idepo == 1): Uses interstitial insoluble aerosol (NWINS, QAINS); outputs **DEPON**, DEPOQ, etc. Does not use ninp.
- **depoxd_DM15** (idepo 101–107): Uses liquid droplets (NW, QW, QA, QS) and ice supersaturation (Diehl & Mitra 2015 style); outputs **DEPON**, etc. Does not use ninp.

Both can be important for total ice number after 12:30 but are **not** the mechanism that uses flare-added INP.

---

## 6. How to analyze ice number increase after 12:30

1. **Time window:** Restrict to model time ≥ seeding start (e.g. 12:30:00). Compare total ice number or summed number tendency before vs after.
2. **Flare effect:** Track **IMMERN** (or its bulk/diagnostic, e.g. DIMMERN_sum). This is the term that responds to flare INP; it should increase when the flare adds INP and conditions allow freezing.
3. **Deposition:** Optionally track **DEPON** (e.g. Ddeponf_sum) to separate deposition-driven ice from immersion.
4. **CONDNFROD:** For **total** ice number, sum over bins should be zero. Do not treat it as a source of new crystals; use it only for per-bin redistribution in spectral budgets. In the View A stacked-area plots, the dark-blue **CONDENSATION** slice in the **Ice Number sources** panel is exactly this redistribution—it can be large per bin or in aggregate over bins, but it does **not** add total ice number.
5. **Output names:** Map model diagnostics (Ddeponf_sum, DIMMERN_sum, DCONDNFROD_sum, etc.) to your variable list or post-processing names.

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

The notebook **`notebooks/meteograms/05-process-budget.ipynb`** implements this investigation. The narrative (main question, View A/B interpretation, answers for Ice Number / Ice Mass / Liquid) is embedded in the notebook’s markdown cells. If you need to **re-apply** this text (e.g. after clearing cells or using another copy of the notebook), use the sections below. If the notebook fails to save or open due to truncated outputs, use **Kernel → Restart & Clear Output** (or **Edit → Clear All Outputs**), then save.

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
