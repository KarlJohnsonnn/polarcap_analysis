# COSMO-SPECS Microphysics Module Parameters

## Dimension Definitions

From `data_sbm.f90` (lines 367-376) and `data_modelconfig.f90`:

```fortran
!----- PARAMETER (Fixed at Compile Time) -----
INTEGER (KIND=iintegers), parameter :: jmax = 66      ! liquid water droplet bins (or 132 for PARCEL mode)
INTEGER (KIND=iintegers), parameter :: simax = 1      ! insoluble aerosol size bins (1 or 66)
INTEGER (KIND=iintegers), parameter :: ninpmax = 30   ! INP temperature bins (for iimfr=10/11)

!----- VARIABLES (Set at Runtime from Namelist) -----
INTEGER (KIND=iintegers) :: nmax = 2                  ! spectral resolution of water droplet spectrum
INTEGER (KIND=iintegers) :: tmax = 2                  ! spectral resolution of insoluble aerosol spectrum
INTEGER (KIND=iintegers) :: smax = 1                  ! soluble aerosol classes
INTEGER (KIND=iintegers) :: itmax = 1                 ! externally mixed aerosol types
INTEGER (KIND=iintegers) :: ipmax = 1                 ! horizontal resolution (cylinder model)

!----- COSMO Grid Dimensions -----
INTEGER (KIND=iintegers) :: ke                        ! number of vertical grid levels (from data_modelconfig)
INTEGER (KIND=iintegers) :: ie, je                    ! zonal and meridional grid points
INTEGER                  :: kz                        ! vertical levels for SBM (passed to subroutines)
```

**Loop Counter Usage in Subroutines:**
- `kz`: Vertical dimension loop counter (1:kz) for 3D fields like `qw(kz,jmax,smax,ipmax)`
- `jmax`: Loop over liquid water droplet size bins (1:jmax)
- `smax`: Loop over soluble aerosol classes (1:smax)
- `simax`: Loop over insoluble aerosol size bins (1:simax)
- `itmax`: Loop over externally mixed aerosol types (1:itmax)
- `ninpmax`: Loop over INP temperature bins (1:ninpmax)
- `ipmax`: Loop over horizontal resolution (1:ipmax)

## Input Parameters (3D/2D Fields)

### Liquid Water Droplet Spectra (Soluble Aerosol + Water)

| Parameter | Dimension | Description | Unit |
|-----------|-----------|-------------|------|
| `nw` | (jmax, smax, ipmax) | Number concentration of liquid water droplets | kg⁻¹ |
| `qw` | (jmax, smax, ipmax) | Total mass mixing ratio of liquid water | kg kg⁻¹ |
| `qws` | (jmax, smax, ipmax) | Soluble aerosol mass in droplets MR (dry) | kg kg⁻¹ |
| `qwa` | (jmax, smax, ipmax) |   Insoluble aerosol mass in droplets MR | kg kg⁻¹ |

### Ice Particle Spectra (Insoluble Aerosol + Ice)

| Parameter | Dimension | Description | Unit |
|-----------|-----------|-------------|------|
| `nf` | (jmax, smax, ipmax) | Number concentration of ice particles | kg⁻¹ |
| `qf` | (jmax, smax, ipmax) | Total mass mixing ratio of ice particles | kg kg⁻¹ |
| `qfs` | (jmax, smax, ipmax) | Soluble aerosol mass in mixed-phase droplets MR  | kg kg⁻¹ |
| `qfa` | (jmax, smax, ipmax) | Insoluble aerosol mass in mixed-phase droplets MR  | kg kg⁻¹ |
| `qfw` | (jmax, smax, ipmax) | Liquid water shell mass mixing ratio (rime coating) | kg kg⁻¹ |

### Insoluble Aerosol (INS) Spectra
| Parameter | Dimension | Description | Unit |
|-----------|-----------|-------------|------|
| `ni` | (simax, itmax, ipmax) | Number concentration of insoluble aerosol particles | kg⁻¹ |
| `qia` | (simax, itmax, ipmax) | Mass mixing ratio of insoluble aerosol | kg kg⁻¹ |
| `nwins` | (simax, itmax, ipmax) | Number concentration of wet insoluble particles (contact freezing) | kg⁻¹ |
| `qains` | (simax, itmax, ipmax) | Mass of insoluble aerosol in wetted form | kg kg⁻¹ |

### Ice Nuclei (INP) - Temperature Dependent

| Parameter | Dimension | Description | Unit |
|-----------|-----------|-------------|------|
| `ninp` | (ninpmax, ipmax) | INP concentration for each temperature bin | kg⁻¹ |
| `ninp_T` | (kz, ninpmax, ipmax) | 3D INP field (with vertical levels kz) | kg⁻¹ |
| `TFR` | (ninpmax) | Temperature grid for INP classification | K |

### Frozen Droplets (Rimed Ice Particles from Collision)

| Parameter | Dimension | Description | Unit |
|-----------|-----------|-------------|------|
| `nfrod` | (jmax, smax, ipmax) | Number concentration of frozen droplets (from collision) | kg⁻¹ |
| `qfrod` | (jmax, smax, ipmax) | Total mass mixing ratio of rimed particles | kg kg⁻¹ |
| `qsfrod` | (jmax, smax, ipmax) | Solid ice mass in rimed particles | kg kg⁻¹ |
| `qafrod` | (jmax, smax, ipmax) | Aerosol mass in rimed particles | kg kg⁻¹ |
| `qwfrod` | (jmax, smax, ipmax) | Liquid water shell mass in rimed particles | kg kg⁻¹ |

### Thermodynamic Variables

| Parameter | Dimension | Description | Unit |
|-----------|-----------|-------------|------|
| `tabs` | scalar | Absolute temperature | K |
| `tt` | scalar | Temperature | K |
| `tt_old` | scalar | Temperature from previous timestep | K |
| `qv` | scalar | Water vapor mixing ratio | kg kg⁻¹ |
| `qc` | scalar | Cloud liquid water (COSMO feedback) | kg kg⁻¹ |
| `qr` | scalar | Rain water (COSMO feedback) | kg kg⁻¹ |
| `qice` | scalar | Cloud ice water (COSMO feedback) | kg kg⁻¹ |
| `qsnow` | scalar | Snow water (COSMO feedback) | kg kg⁻¹ |
| `satt` | scalar | Water saturation ratio (supersaturation) | - |
| `ptot` | scalar | Total air pressure | Pa |
| `rhotot` | scalar | Total air density | kg m⁻³ |
| `rho` | scalar | Air density | kg m⁻³ |
| `pp` | scalar | Pressure perturbation | Pa |

### Mean Particle Masses and Radii

| Parameter | Dimension | Description | Unit |
|-----------|-----------|-------------|------|
| `mquer` | (jmax, smax) | Mean particle mass (liquid) | kg |
| `miquer` | (jmax, smax) | Mean ice particle mass | kg |
| `rquer` | (jmax, smax) | Mean particle radius (liquid, wet) | m |
| `riquer` | (jmax, smax) | Mean ice particle radius | m |
| `siquer` | (jmax, smax) | Mean insoluble aerosol mass | kg |
| `squer` | (jmax, smax) | Mean soluble aerosol mass | kg |

### Spectral Grid Boundaries and Centers

| Parameter | Dimension | Description | Unit |
|-----------|-----------|-------------|------|
| `mgrenz` | (jmax+1) | Mass bin boundaries (liquid water) | kg |
| `mmitte` | (jmax) | Mass bin centers | kg |
| `rgrenz` | (jmax+1) | Radius bin boundaries | m |
| `rmitte` | (jmax) | Radius bin centers | m |
| `sgrenz` | (simax+1) | Mass bin boundaries (aerosol) | kg |
| `smitte` | (simax) | Mass bin centers (aerosol) | kg |

### Grid and Geometry

| Parameter | Dimension | Description | Unit |
|-----------|-----------|-------------|------|
| `dz` | (kz) | Grid spacing in vertical direction | m |
| `hz_lm` | (kz+1) | Height of grid levels (COSMO) | m |
| `pp` | (kz) | Pressure on full levels | Pa |
| `delt` | scalar | Microphysical timestep | s |
| `deltdyn` | scalar | Dynamical timestep | s |

### Diagnostic Variables

| Parameter | Dimension | Description | Unit |
|-----------|-----------|-------------|------|
| `ifreeze` | (itmax) | Freezing nucleus type indicator | - |
| `miv` | (itmax) | Contact angle for ice nucleation | rad |

### Grid Indices for COSMO Coupling

| Parameter | Dimension | Description | Unit |
|-----------|-----------|-------------|------|
| `i_lm`, `j_lm`, `k_lm` | scalar | Grid indices in COSMO domain | - |
| `t_lm` | scalar | COSMO timestep counter | - |
| `i_proc` | scalar | Processor ID | - |

---

## Output Parameters (Tendencies)

### Liquid Water Droplet Tendencies

| Parameter | Dimension | Description | Unit |
|-----------|-----------|-------------|------|
| `dnw` | (jmax, smax) | Number concentration tendency of droplets | kg⁻¹ s⁻¹ |
| `dqw` | (jmax, smax) | Liquid water mass tendency | kg kg⁻¹ s⁻¹ |
| `dqws` | (jmax, smax) | Soluble aerosol mass tendency in droplets | kg kg⁻¹ s⁻¹ |
| `dqwa` | (jmax, smax) | Aerosol mass tendency in droplets | kg kg⁻¹ s⁻¹ |

### Ice Particle Tendencies (Ice-Ice Aggregation, koll_eis_eisxd.f90)

| Parameter | Dimension | Description | Unit |
|-----------|-----------|-------------|------|
| `knf` | (jmax, smax) | Number tendency from ice-ice aggregation | kg⁻¹ s⁻¹ |
| `kqf` | (jmax, smax) | Ice mass tendency from ice-ice aggregation | kg kg⁻¹ s⁻¹ |
| `kqwf` | (jmax, smax) | Liquid water shell tendency from ice-ice aggregation | kg kg⁻¹ s⁻¹ |
| `kqsf` | (jmax, smax) | Soluble aerosol mass tendency from ice-ice aggregation | kg kg⁻¹ s⁻¹ |
| `kqaf` | (jmax, smax) | Aerosol mass tendency from ice-ice aggregation | kg kg⁻¹ s⁻¹ |

### Insoluble Aerosol Tendencies

| Parameter | Dimension | Description | Unit |
|-----------|-----------|-------------|------|
| `dni` | (simax, itmax) | Number tendency of insoluble aerosol | kg⁻¹ s⁻¹ |
| `dqia` | (simax, itmax) | Mass tendency of insoluble aerosol | kg kg⁻¹ s⁻¹ |
| `dnwins` | (simax, itmax) | Number tendency of wet insoluble particles | kg⁻¹ s⁻¹ |
| `dqains` | (simax, itmax) | Mass tendency of insoluble aerosol in wetted form | kg kg⁻¹ s⁻¹ |

### INP Tendencies

| Parameter | Dimension | Description | Unit |
|-----------|-----------|-------------|------|
| `dninp` | (ninpmax) | INP number concentration tendency | kg⁻¹ s⁻¹ |

### Frozen Droplet Tendencies

| Parameter | Dimension | Description | Unit |
|-----------|-----------|-------------|------|
| `dnfrod` | (jmax, smax) | Number tendency of frozen droplets | kg⁻¹ s⁻¹ |
| `dqfrod` | (jmax, smax) | Mass tendency of rimed particles | kg kg⁻¹ s⁻¹ |
| `dqsfrod` | (jmax, smax) | Solid ice mass tendency in rimed particles | kg kg⁻¹ s⁻¹ |
| `dqafrod` | (jmax, smax) | Aerosol mass tendency in rimed particles | kg kg⁻¹ s⁻¹ |
| `dqwfrod` | (jmax, smax) | Liquid water shell mass tendency in rimed particles | kg kg⁻¹ s⁻¹ |

### Water Vapor Tendency

| Parameter | Dimension | Description | Unit |
|-----------|-----------|-------------|------|
| `dqv` | scalar | Water vapor tendency | kg kg⁻¹ s⁻¹ |
| `dqvdyn` | scalar | Dynamical water vapor tendency | kg kg⁻¹ s⁻¹ |

### Temperature Tendency

| Parameter | Dimension | Description | Unit |
|-----------|-----------|-------------|------|
| `delt_ice` | scalar | Temperature change due to latent heat release (phase transitions) | K s⁻¹ |

---

## Microphysical Process Tendencies (Internal/Diagnostic)

### Condensation/Evaporation

| Parameter | Dimension | Description | Unit |
|-----------|-----------|-------------|------|
| `condq` | (jmax, smax) | Condensation mass tendency | kg kg⁻¹ s⁻¹ |
| `condn` | (jmax, smax) | Condensation number tendency | kg⁻¹ s⁻¹ |
| `conds` | (jmax, smax) | Soluble aerosol mass tendency during condensation | kg kg⁻¹ s⁻¹ |
| `conda` | (jmax, smax) | Aerosol mass tendency during condensation | kg kg⁻¹ s⁻¹ |

### Breakup

| Parameter | Dimension | Description | Unit |
|-----------|-----------|-------------|------|
| `breaq` | (jmax, smax) | Mass change due to droplet breakup | kg kg⁻¹ s⁻¹ |
| `brean` | (jmax, smax) | Number change due to breakup | kg⁻¹ s⁻¹ |
| `breas` | (jmax, smax) | Soluble aerosol mass change during breakup | kg kg⁻¹ s⁻¹ |
| `breaa` | (jmax, smax) | Aerosol mass change during breakup | kg kg⁻¹ s⁻¹ |

### Collision (Water-Water)

| Parameter | Dimension | Description | Unit |
|-----------|-----------|-------------|------|
| `kollq` | (jmax, smax) | Mass change due to collision | kg kg⁻¹ s⁻¹ |
| `kolln` | (jmax, smax) | Number change due to collision | kg⁻¹ s⁻¹ |
| `kolls` | (jmax, smax) | Soluble aerosol mass change during collision | kg kg⁻¹ s⁻¹ |
| `kolla` | (jmax, smax) | Aerosol mass change during collision | kg kg⁻¹ s⁻¹ |

### Riming — Drop-Ice Collision (koll_ice_dropsxd.f90)

Liquid drops captured by frozen drops (ice particles). Liquid-side variables
appear in the DNW/DQW budget; ice-side variables in the DNFROD/DQFROD budget.

**Liquid-side loss (drops consumed by ice):**

| Parameter | Dimension | Description | Unit |
|-----------|-----------|-------------|------|
| `kollni` | (jmax, smax) | Liquid drop number loss from riming (in DNW budget) | kg⁻¹ s⁻¹ |
| `kollqi` | (jmax, smax) | Liquid drop mass loss from riming (in DQW budget) | kg kg⁻¹ s⁻¹ |
| `kollsi` | (jmax, smax) | Soluble aerosol mass loss from riming | kg kg⁻¹ s⁻¹ |
| `kollai` | (jmax, smax) | Aerosol mass loss from riming | kg kg⁻¹ s⁻¹ |

**Ice-side gain (frozen drops grow):**

| Parameter | Dimension | Description | Unit |
|-----------|-----------|-------------|------|
| `kollnfrodi` | (jmax, smax) | Frozen drop number redistribution from riming (in DNFROD budget) | kg⁻¹ s⁻¹ |
| `kollqfrodi` | (jmax, smax) | Frozen drop ice-core mass redistribution from riming (in DQFROD budget) | kg kg⁻¹ s⁻¹ |
| `kollafrodi` | (jmax, smax) | Aerosol mass gain on frozen drops from riming | kg kg⁻¹ s⁻¹ |
| `kollsfrodi` | (jmax, smax) | Soluble aerosol gain on frozen drops from riming | kg kg⁻¹ s⁻¹ |
| `kollqwf` | (jmax, smax) | Liquid water shell mass gain from captured drops (rime shell) | kg kg⁻¹ s⁻¹ |

**Alternative riming pathway (koll_contactxd_DM15.f90, ikoll≥101):**

| Parameter | Dimension | Description | Unit |
|-----------|-----------|-------------|------|
| `kollnfrod` | (jmax, smax) | Frozen drop number tendency (DM15 riming kernel) | kg⁻¹ s⁻¹ |
| `kollqfrod` | (jmax, smax) | Frozen drop mass tendency (DM15 riming kernel) | kg kg⁻¹ s⁻¹ |

### Collision (Water-Insoluble Particles) + Contact Freezing (koll_insolxd.f90)

Drop + insoluble aerosol collisions. If T < T_contact, a fraction freezes
(contact freezing); the rest stays liquid (aerosol scavenging).

**Liquid fraction (drop stays liquid, scavenges aerosol):**

| Parameter | Dimension | Description | Unit |
|-----------|-----------|-------------|------|
| `kolln_ins` | (jmax, smax) | Liquid drop number change from insol. aerosol scavenging | kg⁻¹ s⁻¹ |
| `kollq_ins` | (jmax, smax) | Liquid drop mass change from insol. aerosol scavenging | kg kg⁻¹ s⁻¹ |
| `kolla_ins` | (jmax, smax) | Aerosol mass change in liquid drops | kg kg⁻¹ s⁻¹ |
| `kolls_ins` | (jmax, smax) | Soluble aerosol change in liquid drops | kg kg⁻¹ s⁻¹ |

**Frozen fraction (contact freezing: drop + aerosol → frozen drop):**

| Parameter | Dimension | Description | Unit |
|-----------|-----------|-------------|------|
| `kollnfrod_ins` | (jmax, smax) | Frozen drop number from contact freezing | kg⁻¹ s⁻¹ |
| `kollqfrod_ins` | (jmax, smax) | Frozen drop mass from contact freezing | kg kg⁻¹ s⁻¹ |

### Ice-Ice Aggregation (koll_eis_eisxd.f90)

Collision-coalescence of frozen drops with other frozen drops (wet ice
aggregation, requires at least one particle with substantial liquid shell).

| Parameter | Dimension | Description | Unit |
|-----------|-----------|-------------|------|
| `knf` | (jmax, smax) | Frozen drop number tendency from ice-ice aggregation | kg⁻¹ s⁻¹ |
| `kqf` | (jmax, smax) | Frozen drop ice mass tendency from aggregation | kg kg⁻¹ s⁻¹ |
| `kqwf` | (jmax, smax) | Frozen drop liquid water shell tendency from aggregation | kg kg⁻¹ s⁻¹ |
| `kqsf` | (jmax, smax) | Frozen drop soluble aerosol mass tendency from aggregation | kg kg⁻¹ s⁻¹ |
| `kqaf` | (jmax, smax) | Frozen drop aerosol mass tendency from aggregation | kg kg⁻¹ s⁻¹ |

### Collision (INS-INS)

| Parameter | Dimension | Description | Unit |
|-----------|-----------|-------------|------|
| `kollnins` | (simax, itmax) | Number change from INS-INS collision | kg⁻¹ s⁻¹ |
| `kollains` | (simax, itmax) | Aerosol mass change from INS-INS collision | kg kg⁻¹ s⁻¹ |

### Immersion Freezing

| Parameter | Dimension | Description | Unit |
|-----------|-----------|-------------|------|
| `immerq` | (jmax, smax) | Mass change to ice via immersion freezing | kg kg⁻¹ s⁻¹ |
| `immern` | (jmax, smax) | Number change via immersion freezing | kg⁻¹ s⁻¹ |
| `immera` | (jmax, smax) | Aerosol mass change in immersion freezing | kg kg⁻¹ s⁻¹ |
| `immers` | (jmax, smax) | Soluble aerosol mass change in immersion freezing | kg kg⁻¹ s⁻¹ |

### Contact Freezing (koll_insolxd.f90)

Contact freezing occurs when a liquid drop collides with an insoluble aerosol
particle at T < T_contact. The frozen fraction is controlled by `fac_freeze`
from `contact_tempxd.f90`. Only `kollnfrod_ins`/`kollqfrod_ins` are true
contact freezing variables; `kollnfrodi`/`kollqfrodi` are riming (see above).

| Parameter | Dimension | Description | Unit |
|-----------|-----------|-------------|------|
| `kollnfrod_ins` | (jmax, smax) | Frozen drop number from contact freezing (drop + insol. aerosol → ice) | kg⁻¹ s⁻¹ |
| `kollqfrod_ins` | (jmax, smax) | Frozen drop mass from contact freezing | kg kg⁻¹ s⁻¹ |

### Homogeneous Freezing

| Parameter | Dimension | Description | Unit |
|-----------|-----------|-------------|------|
| `homq` | (jmax, smax) | Mass change from homogeneous freezing | kg kg⁻¹ s⁻¹ |
| `homn` | (jmax, smax) | Number change from homogeneous freezing | kg⁻¹ s⁻¹ |
| `homa` | (jmax, smax) | Aerosol mass change in homogeneous freezing | kg kg⁻¹ s⁻¹ |
| `homs` | (jmax, smax) | Soluble aerosol change in homogeneous freezing | kg kg⁻¹ s⁻¹ |

### Deposition (Vapor → Ice)

| Parameter | Dimension | Description | Unit |
|-----------|-----------|-------------|------|
| `depoq` | (jmax, smax) | Ice mass from deposition | kg kg⁻¹ s⁻¹ |
| `depon` | (jmax, smax) | Ice number from deposition | kg⁻¹ s⁻¹ |
| `depoa` | (jmax, smax) | Aerosol mass in deposited ice | kg kg⁻¹ s⁻¹ |
| `depos` | (jmax, smax) | Soluble aerosol in deposited ice | kg kg⁻¹ s⁻¹ |
| `depoqf` | (jmax, smax) | Ice mass from deposition on frozen particles | kg kg⁻¹ s⁻¹ |
| `deponf` | (jmax, smax) | Ice number from deposition on frozen particles | kg⁻¹ s⁻¹ |
| `depoqfa` | (jmax, smax) | Aerosol mass change via deposition on frozen particles | kg kg⁻¹ s⁻¹ |
| `deponi` | (simax, itmax) | Number change from deposition on insoluble particles | kg⁻¹ s⁻¹ |
| `depoqia` | (simax, itmax) | Mass change from deposition on insoluble particles | kg kg⁻¹ s⁻¹ |

### Melting

| Parameter | Dimension | Description | Unit |
|-----------|-----------|-------------|------|
| `dnfmelt` | (jmax, smax) | Number change from ice melting | kg⁻¹ s⁻¹ |
| `dqfmelt` | (jmax, smax) | Mass change from ice melting | kg kg⁻¹ s⁻¹ |
| `dqfwmelt` | (jmax, smax) | Liquid water shell mass change from melting | kg kg⁻¹ s⁻¹ |
| `dqfsmelt` | (jmax, smax) | Solid ice mass change from melting | kg kg⁻¹ s⁻¹ |
| `dqfamelt` | (jmax, smax) | Aerosol mass change during melting | kg kg⁻¹ s⁻¹ |
| `dnwmelt` | (jmax, smax) | Droplet number change from melting | kg⁻¹ s⁻¹ |
| `dqwmelt` | (jmax, smax) | Droplet mass change from melting | kg kg⁻¹ s⁻¹ |

### Refreezing (frierenxd.f90)

| Parameter | Dimension | Description | Unit |
|-----------|-----------|-------------|------|
| `dqffrier` | (jmax, smax) | Ice core mass gain from refreezing of liquid shell on frozen drops | kg kg⁻¹ s⁻¹ |

---

## Budget Equations (from cloudxd.f90)

These are the prognostic increments assembled from all process tendencies.
Sign convention: positive = source, negative = sink for that budget.

```
DNW     = (CONDN + KOLLN + KOLLNI + KOLLN_INS + BREAN − IMMERN − HOMN + dnwmelt) × DELTAT
DQW     = (CONDQ + KOLLQ + KOLLQI + KOLLQ_INS + BREAQ − IMMERQ − HOMQ + dqwmelt) × DELTAT
DNFROD  = (CONDNFROD + KOLLNFROD + KOLLNFRODI + KOLLNFROD_INS + knf + IMMERN + DEPON + HOMN + dnfmelt) × DELTAT
DQFROD  = (CONDQFROD + KOLLQFROD + KOLLQFRODI + KOLLQFROD_INS + kqf + IMMERQ + DEPOQ + HOMQ + dqfmelt + dqffrier) × DELTAT
dqwfrod = (CONDQWFROD + KOLLQFROD + kollqwf + KOLLQFROD_INS + kqwf + IMMERQ + DEPOQ + HOMQ + dqfwmelt) × DELTAT
DELT    = −DQVcond·LV/CP − DQVdep·LS/CP + DELT_ICE
```

Key cross-phase symmetry: IMMERN/IMMERQ and HOMN/HOMQ appear with **−** in the
liquid budget (DNW/DQW) and **+** in the ice budget (DNFROD/DQFROD).

---

## Process Control Flags

| Parameter | Type | Description |
|-----------|------|-------------|
| `icond` | INTEGER | Condensation/evaporation scheme flag |
| `idepo` | INTEGER | Deposition scheme flag |
| `iimfr` | INTEGER | Immersion freezing scheme (10=INP, 11=other) |
| `imelt` | INTEGER | Melting scheme flag |
| `ifrier` | INTEGER | Refreezing scheme flag |
| `ibrea` | INTEGER | Breakup scheme flag |
| `ikoll` | INTEGER | Collision scheme flag |
| `ikofr` | INTEGER | Contact freezing scheme flag |
| `iinsol` | INTEGER | Insoluble aerosol scheme flag |
| `ikeis` | INTEGER | Ice particle scheme flag |
| `ihomfr` | INTEGER | Homogeneous freezing scheme flag |
| `iap` | INTEGER | Initial aerosol spectrum (1,2,3) |

---

## Key Physical Constants (from `konstxd.f90`)

| Constant | Value | Unit | Description |
|----------|-------|------|-------------|
| `T0` | 273.16 | K | Reference temperature (0°C) |
| `LV` | 2.50078e6 | J kg⁻¹ | Latent heat of vaporization |
| `LS` | 2.834e6 | J kg⁻¹ | Latent heat of sublimation |
| `LV_ICE` | LS - LV | J kg⁻¹ | Latent heat difference |
| `CP` | 1005 | J K⁻¹ kg⁻¹ | Specific heat capacity at constant pressure |
| `RHOW` | 1000 | kg m⁻³ | Density of liquid water |
| `RHOI` | 900 | kg m⁻³ | Density of ice |
| `RW` | 461.5 | J K⁻¹ kg⁻¹ | Gas constant for water vapor |
| `RDRY` | 287 | J K⁻¹ kg⁻¹ | Gas constant for dry air |
| `PI` | 3.14159... | - | Pi constant |

---

## Spectral Resolution Notes

The spectral bin structure follows a geometric progression with configurable resolution:

- **JMAX (66 or 132)**: Number of liquid water droplet size bins
- **SMAX (typically 1)**: Number of soluble aerosol classes
- **SIMAX (1 or 66)**: Number of insoluble aerosol size bins
- **ITMAX**: Number of distinct aerosol types (externally mixed)
- **NINPMAX (30)**: Number of temperature bins for ice nucleation (for iimfr=10)
- **IPMAX (1)**: Horizontal resolution (cylindrical model)

Bin boundaries are defined logarithmically:
- `MGRENZ(jmax+1)`: Mass boundaries for water droplets
- `RGRENZ(jmax+1)`: Radius boundaries for water droplets
- `SGRENZ(simax+1)`: Mass boundaries for aerosol particles
- Bin centers: `MMITTE(jmax)`, `RMITTE(jmax)`, `SMITTE(simax)`, `RSMITTE(simax)`

---

## Typical Array Dimensions

### Input/Output Fields
```fortran
! Liquid water spectra
qw(kz, jmax, smax, ipmax)     ! Liquid water mass
nw(kz, jmax, smax, ipmax)     ! Droplet number concentration

! Ice particle spectra
qf(kz, jmax, smax, ipmax)     ! Ice particle mass
nf(kz, jmax, smax, ipmax)     ! Ice particle number concentration

! Insoluble aerosol
qia(kz, simax, itmax, ipmax)  ! Insoluble aerosol mass
ni(kz, simax, itmax, ipmax)   ! Insoluble aerosol number

! Ice nuclei (INP)
ninp(kz, ninpmax, ipmax)      ! INP concentration per temperature bin
```

### Local (2D) Work Arrays
```fortran
! Without vertical dimension - computed per level
condq(jmax, smax)      ! Condensation mass tendency
kollq(jmax, smax)      ! Collision mass change
deponi(simax, itmax)   ! Deposition on insoluble particles
```

---

## All Loop Iteration Counters (Summary)

### Compile-Time Parameters (Fixed)
| Variable | Default | Type | Purpose |
|----------|---------|------|---------|
| `JMAX` | 66 (132*) | INTEGER parameter | Number of liquid water droplet size bins |
| `SIMAX` | 1 (66*) | INTEGER parameter | Number of insoluble aerosol size bins |
| `NINPMAX` | 30 | INTEGER parameter | Number of ice nuclei temperature bins |

*\*Alternative values available via compilation flags or configuration*

### Runtime Variables (From Namelist)
| Variable | Default | Type | Purpose |
|----------|---------|------|---------|
| `NMAX` | 2 | INTEGER | Spectral resolution of water droplet spectrum |
| `TMAX` | 2 | INTEGER | Spectral resolution of insoluble aerosol spectrum |
| `SMAX` | 1 | INTEGER | Number of soluble aerosol classes |
| `ITMAX` | 1 | INTEGER | Number of externally mixed aerosol types |
| `IPMAX` | 1 | INTEGER | Horizontal resolution (cylinder model) |

### Grid Dimensions (From COSMO)
| Variable | Source | Type | Purpose |
|----------|--------|------|---------|
| `KE` | `data_modelconfig` | INTEGER | Total number of vertical grid levels |
| `IE`, `JE` | `data_modelconfig` | INTEGER | Zonal and meridional grid points |
| `KZ` | Subroutine argument | INTEGER | Vertical levels passed to SBM routines |

---


## Key Notes on Dimensions

1. **NMAX and TMAX** determine the resolution factor for geometric progression:
   - `NMAX=2`: Each bin is ~2× smaller in mass than the previous (binary spacing)
   - `TMAX=2`: Same for insoluble aerosol bins

2. **KZ** varies by vertical level and is passed as an argument to subroutines, while **KE** is the total grid size from COSMO

3. **JMAX** and **SIMAX** are typically fixed at compile time for performance, while **SMAX, ITMAX, IPMAX** are usually set to 1 in standard cylindrical model runs

4. **NINPMAX=30** gives 30 temperature bins for ice nucleation (typically from 268K down to ~238K at 1K intervals)

---

## Related Documentation

- **Data Module**: `LM/SRC/data_sbm.f90` - Where dimensions are defined
- **Constants Module**: `LM/specs/core/sconstxd.f90` - Spectral grid setup
- **Control Module**: `LM/specs/core/cloudxd.f90` - Main microphysics driver
- **Grid Module**: `data_modelconfig` - COSMO grid dimensions

---

## References

- **Main Control Module**: `cloudxd.f90` - Orchestrates all microphysical processes
- **Data Module**: `LM/SRC/data_sbm.f90` - Defines all parameters and dimensions
- **Constants Definition**: `konstxd.f90` - Physical constants
- **Spectral Grid**: `sconstxd.f90` - Bin structure and boundaries
- **Initialization**: `initialxd.f90`, `ap_newxd.f90` - Initial aerosol and hydrometeor distributions


## Grep run .out file warnings and paste into new file:
`grep -A1 "cloud QFROD" 20925907.out | grep -v "^--$" | paste -d " " - - | sed 's/[0-9]*:  */ /g' > output_file.txt`

Extract and merge Fortran debug output for cloud QFROD warnings.

Searches for lines containing "cloud QFROD" and merges each match with its 
following line into a single line. Removes line number prefixes (e.g., `184:`).

Usage:
    `grep -A1 "cloud QFROD" input.out | grep -v "^--$" | paste -d " " - - | sed 's/[0-9]*:  */ /g' > output.txt`

Input format:
    ```
    184:  cloud QFROD        189000          16          16          85          52
    184:   7.056930166677684E-003   168899.311551766`
    ```
Output format:
    `cloud QFROD        189000          16  s        16          85          52 7.056930166677684E-003   168899.311551766`
