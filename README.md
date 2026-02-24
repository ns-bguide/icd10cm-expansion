# ICD-10-CM term extraction + enrichment (2026)

Dependency-free pipeline to extract ICD-10-CM terms from `icd10cm_order_2026.txt`, generate canonical forms, generate enriched variants via configurable rules, and (optionally) integrate UMLS atoms when `umls_atoms.csv` + `umls_sources.csv` are present.

## Input format

The script expects a file like `icd10cm_order_2026.txt` with rows shaped like:

```
00001 A00     0 Cholera                                                      Cholera
```

Where:
- **Column 1**: ordering integer (5 digits)
- **Column 2**: ICD-10-CM code (e.g. `A00`, `A0472`)
- **Column 3**: flag (`0` or `1`) used for filtering (in this dataset, `1` behaves like “leaf node”)
- **Column 4**: short description (Official + abbreviations)
- **Column 5**: long description (Official)

Important: some rows have only a single space between the short and long description columns when the short description is long. The parser in [icd10cm_pipeline.py](icd10cm_pipeline.py) uses fixed-width slicing to handle this reliably.

## Output schema

CSV columns:

- `ICD10CMCode`
- `Term`
- `Type`

`Type` encodes provenance:
- `official` (long description)
- `official+abbr` (short description; included only if different from official)
- `canonical:official`, `canonical:official+abbr` (canonicalized variants)
- `enriched:<ruleId>` (additional variants created by rules; e.g. `enriched:A1`)
- `umls:<vocabulary>` (direct UMLS atom term strings; e.g. `umls:SNOMEDCT_US`)
- `umls:<vocabulary>:<ruleId>` (rule-derived variants generated from a UMLS term; e.g. `umls:SNOMEDCT_US:C1`)

**De-dupe:** the pipeline de-dupes globally on `Term`, keeping the first row encountered (including its `ICD10CMCode` and `Type`).

**Casing:** all emitted `Term` values are lowercased. This makes the final output
case-insensitive by construction.

**CSV quoting:** the pipeline writes CSV with consistent quoting (all fields
wrapped in quotes). This is expected; use a CSV parser (not string-splitting)
when reading the file.

## Quick start (Linux/macOS)

From this folder:

1) (Optional) Create and activate a venv:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

2) Run the pipeline:

```bash
python3 icd10cm_pipeline.py --input icd10cm_order_2026.txt --output icd10cm_terms_2026.csv --leaf-only --include-official-abbr
```

## Quick start (Windows)

From this folder:

1) Create and activate a venv (optional but recommended):

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

2) Run the pipeline:

```powershell
python .\icd10cm_pipeline.py --input .\icd10cm_order_2026.txt --output .\icd10cm_terms_2026.csv --leaf-only --include-official-abbr
```

The script prints a summary with row counts by `Type`.

## CLI options

Common flags:

- `--leaf-only`: keep only rows where `FLAG == 1`
- `--include-official-abbr`: include the short description column as `official+abbr`
- `--no-canonical`: disable canonical generation
- `--no-enriched`: disable enrichment generation
- `--enriched-max-per-term N`: cap enrichment fanout per canonical term (`0` = unlimited; default)
- `--no-rule-report`: disable per-rule enrichment impact report

Extra outputs (enabled by default):

- `--no-term-txt`: disable writing a terms-only `.terms.txt` file
- `--term-txt-output PATH`: choose a custom terms-only output path
- `--no-unspecified-review`: disable the review CSV for single-word `, unspecified` stems
- `--unspecified-review-output PATH`: choose a custom review CSV output path

UMLS integration:

- `--umls-atoms PATH` (default: `umls_atoms.csv`)
- `--umls-sources PATH` (default: `umls_sources.csv`)
- `--no-umls`: disable UMLS integration
- `--umls-report-output PATH` (default: `umls_integration_report.csv`)
- `--no-umls-report`: disable writing the UMLS report CSV
- `--no-umls-derivations`: disable applying derivation rules to UMLS-added terms
- `--umls-enriched-max-per-term N`: cap derivations per UMLS term (`0` = unlimited; default)

Example (official terms only, no enrichment):

```powershell
python .\icd10cm_pipeline.py --input .\icd10cm_order_2026.txt --output .\official_only.csv --leaf-only --no-enriched
```

## Canonicalization

Canonical terms are produced by:

- lowercasing
- trimming and collapsing whitespace
- removing trailing punctuation (`. ! ? , ; :`)

Canonicalization runs on the official and (optionally) official+abbr terms.

Note: because the pipeline also lowercases the `official` / `official+abbr` terms,
the canonical form often becomes identical to the base term. The script de-dupes
terms per ICD code, so canonical rows may be omitted when they would be duplicates.

## Enrichment rules (how to add new ones)

Rules are defined in `ENRICHMENT_RULES` in [icd10cm_rules.py](icd10cm_rules.py).

### Rule pattern

Each rule is an `EnrichmentRule(rule_id, description, apply_fn)` where:

- `rule_id` is a stable id (used in output as `enriched:<rule_id>`)
- `apply_fn(term)` yields zero or more candidate variants for that **canonical** term

Guidelines:
- Assume the input term is already canonical (lowercased, trimmed).
- Generate only meaningful variants.
- Rules should still avoid runaway explosions, but by default the pipeline does not cap fanout (set `--enriched-max-per-term` / `--umls-enriched-max-per-term` if you want a limit).
- Use a stable rule id so you can trace provenance in the output `Type`.

### Existing rules

**P rules (parentheses)**
- `P1`: parentheses split. If a term contains `( ... )`, generate:
	- the original term (lowercased)
	- the term with all parenthetical content removed
	- the parenthetical content alone

**A rules (simple modifications)**
- `A1`: replace hyphens with spaces (e.g. `b-cell` → `b cell`)
- `A2`: remove hyphens (e.g. `b-cell` → `bcell`)
- `A3`: remove apostrophes (e.g. `crohn's` → `crohns`)
- `A4`: swap `and` ↔ `&`
- `A5`: swap `or` ↔ `/`

**B rules (abbreviations, bidirectional)**
- `B1`: `syndrome` ↔ `synd`
- `B2`: `chronic` ↔ `chr`
- `B3`: `acute` ↔ `acu`
- `B4`: `left/right` ↔ `lt/rt`

**C rules (phrase normalization)**
- `C1`: `due to` → `because of` and `caused by`
- `C2`: move the suffix `, unspecified` to a prefix `unspecified ...` (focused on multi-word phrases)

**D rules (range expansion)**
- `D1`: expand `stage 1 through stage 4` into `stage 1`, `stage 2`, `stage 3`, `stage 4`

### Adding a new abbreviation rule (example)

If you want `without` ↔ `w/o`:

1) Edit [icd10cm_rules.py](icd10cm_rules.py) and add two `EnrichmentRule(...)` entries to `ENRICHMENT_RULES`:

- `r"\bwithout\b"` → `"w/o"` with rule id `"B5"`
- `r"\bw/o\b"` → `"without"` with rule id `"B5"`

2) Re-run the script and you’ll see additional `enriched:B5` rows.

### Where to edit rules

- Add / remove / reorder rules: `ENRICHMENT_RULES` in [icd10cm_rules.py](icd10cm_rules.py)
- If you add a new rule id, also add a description in `RULE_DESCRIPTIONS` (used for the end-of-run report)

## Enrichment rule report

When enrichment is enabled, the script prints a per-rule summary at the end:

- `terms_affected`: how many canonical terms caused the rule to add at least one variant
- `variants_added`: how many variants were actually added by that rule (after de-dupe and `--enriched-max-per-term`)

Disable the report with:

```powershell
python .\icd10cm_pipeline.py --no-rule-report
```

### Quick verification

After running the pipeline, you can quickly check that a rule is generating rows by:

1) Looking at the console counts for `enriched:<ruleId>` (example: `enriched:P1`)
2) Grepping the output CSV for that `Type` value (example):

```bash
grep -m 5 ',enriched:P1$' icd10cm_terms_2026.csv
```

## Seeing results

There are two places to look:

1) **The output CSV** (default: `icd10cm_terms_2026.csv`)

- Columns: `ICD10CMCode, Term, Type`
- Enriched rows are labeled as `Type=enriched:<ruleId>`

2) **The console summary** printed after the run

- Row counts by `Type`
- (Optional) the per-rule enrichment report (`terms_affected` and `variants_added`)

When UMLS integration is enabled (and both files exist), the script prints an extra summary:

- `UMLS integration: added_rows=...` (how many rows were added)
- `UMLS report written: ...` (CSV report with per-vocabulary counts)

Console output is kept minimal (UMLS per-vocabulary breakdown is written to the report CSV instead).

## UMLS integration

If `umls_atoms.csv` and `umls_sources.csv` are present, the pipeline can enrich terms using UMLS atoms.

1) **Filtering**

- `umls_sources.csv` is used to select allowed vocabularies where `Language == en`.
- Only atoms whose `Source Vocabulary` is in that allowed set are considered.

2) **Integration (direct atoms)**

If a filtered atom's `Query Term` matches a pipeline `Term`, then the atom's `Term String` is emitted as an additional row:

- `ICD10CMCode`: the matching pipeline row's code
- `Term`: the UMLS `Term String` (lowercased)
- `Type`: `umls:<Source Vocabulary>`

3) **Derivations from UMLS terms (optional)**

If enrichment is enabled (default) and `--no-umls-derivations` is not set, the pipeline also applies the same enrichment rules to each UMLS-added term and emits derived variants with:

- `Type`: `umls:<Source Vocabulary>:<ruleId>` (example: `umls:SNOMEDCT_US:C1`)

Control fanout with `--umls-enriched-max-per-term` (`0` = unlimited; default).

4) **UMLS report (enabled by default)**

By default the pipeline writes `umls_integration_report.csv` (configurable via `--umls-report-output`). It contains:

- a `__TOTAL__` row (total atoms kept, total rows added, total derived rows added, etc.)
- per-vocabulary `AtomsKept` and `RowsAdded`

5) **Terms-only file (one term per line)**

By default, the pipeline also writes a terms-only text file next to your CSV:

- If `--output` is `icd10cm_terms_2026.csv`, the terms file is `icd10cm_terms_2026.terms.txt`
- Contains one **unique** term per line (lowercased)

Disable it with:

```powershell
python .\icd10cm_pipeline.py --no-term-txt
```

Or choose a custom path:

```powershell
python .\icd10cm_pipeline.py --term-txt-output .\terms.txt
```

6) **Review file (single-word ', unspecified')**

The pipeline also writes `unspecified_single_word_review.csv` by default. This file lists cases like
`"anthrax, unspecified"` where the stem is a single word and may need manual decision. If the same
term appears in multiple inputs (e.g. official vs official+abbr), the review file aggregates them
into a single row with a `Sources` column.

Disable it with:

```powershell
python .\icd10cm_pipeline.py --no-unspecified-review
```

## Notes / sharing

- Dependencies: none (stdlib only). See [requirements.txt](requirements.txt).
- Large CSVs: `icd10cm_terms_2026.csv` can be big depending on your enrichment settings.

## Troubleshooting

- The output CSV is globally de-duped by `Term`. If you expected the same term to appear under multiple ICD10CM codes, only the first encountered instance will be written.
- If you expected lots of `canonical:*` rows: when base terms are already canonical after lowercasing, canonical rows collapse into `official` / `official+abbr` and are removed by the per-code de-dupe.
- If you need a different de-dup policy (e.g. keep all provenances for the same term), that’s an easy tweak.
