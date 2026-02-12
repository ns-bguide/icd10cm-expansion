# ICD-10-CM term extraction + enrichment (2026)

Small, dependency-free pipeline to extract ICD-10-CM terms from `icd10cm_order_2026.txt`, generate canonical forms, and generate additional enriched variants via configurable rules.

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

**Casing:** all emitted `Term` values are lowercased. This makes the final output
case-insensitive by construction.

## Quick start (Windows)

From this folder:

1) Create and activate a venv (optional but recommended):

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

2) Run the pipeline:

```powershell
python .\icd10cm_pipeline.py --input .\icd10cm_order_2026.txt --output .\icd10cm_terms_2026.csv --leaf-only --include-official-abbr --enriched-max-per-term 10
```

The script prints a summary with row counts by `Type`.

## CLI options

Common flags:

- `--leaf-only`: keep only rows where `FLAG == 1`
- `--include-official-abbr`: include the short description column as `official+abbr`
- `--no-canonical`: disable canonical generation
- `--no-enriched`: disable enrichment generation
- `--enriched-max-per-term N`: cap enrichment fanout per canonical term

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
- Avoid unbounded explosions: use `max_variants` and keep rules conservative.
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
- `C2`: move the suffix `, unspecified` to a prefix `unspecified ...`

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

## Notes / sharing

- Dependencies: none (stdlib only). See [requirements.txt](requirements.txt).
- Large CSVs: `icd10cm_terms_2026.csv` can be big depending on your enrichment settings.

## Troubleshooting

- If output terms look duplicated, remember the script de-dupes terms **per code**, keeping the first provenance encountered.
- If you expected lots of `canonical:*` rows: when base terms are already canonical after lowercasing, canonical rows collapse into `official` / `official+abbr` and are removed by the per-code de-dupe.
- If you need a different de-dup policy (e.g. keep all provenances for the same term), that’s an easy tweak.
