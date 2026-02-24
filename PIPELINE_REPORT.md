# ICD-10-CM + UMLS Term Expansion Pipeline (2026)

This project implements a reproducible methodology for building a **term vocabulary** from ICD-10-CM (and optionally UMLS), suitable for downstream search, matching, and NLP-oriented analyses.

The emphasis is on:

- Broad term coverage (official terms plus systematic variants)
- Traceable provenance for every emitted term
- Conservative quality controls to prevent obvious noise
- A final output with **no duplicate terms**

---

## What the pipeline consumes

1) **ICD-10-CM ordered file**

- Source: the official ordered text file for the target year (e.g., `icd10cm_order_2026.txt`).
- Fields used:
  - ICD-10-CM code
  - Short description (treated as an abbreviated/short label)
  - Long description (treated as the primary official label)

2) **Optional UMLS files (for additional synonyms/variants)**

- `umls_sources.csv`: used to identify which UMLS vocabularies are eligible (English-only).
- `umls_atoms.csv`: provides synonym-like strings (“Term String”) that can be associated with pipeline terms via exact matching on “Query Term”.

---

## What the pipeline produces

### Primary output: a globally unique term CSV

The main output is a CSV with three columns:

- `ICD10CMCode`
- `Term`
- `Type`

Methodological note: the CSV is **globally de-duped by `Term`**. If the same term is observed multiple times (even under different ICD-10-CM codes), it is written once, keeping the first observed provenance.

### Optional companion outputs

- A **terms-only text file** (one term per line) for quick inspection and downstream ingestion.
- A **review CSV** that lists ambiguous “`, unspecified`” cases (single-word stems) for manual adjudication.
- A **UMLS integration report CSV** summarizing how many UMLS atoms were retained and how many rows were added per UMLS vocabulary.

---

## Methodology (high level)

1) **Extract official strings**

- Read the ICD-10-CM source and emit:
  - long description as the primary official term
  - (optionally) short description as an additional official-like term when it differs

2) **Normalize strings**

- Terms are normalized to improve consistency for matching and de-duplication (e.g., lowercasing and whitespace normalization).

3) **Generate systematic variants (enrichment rules)**

The pipeline applies a curated set of rule-based transformations designed to produce plausible alternate surface forms (e.g., punctuation variants, abbreviation variants, and selected phrase rewrites). The goal is to expand coverage while keeping the transformations interpretable.

4) **Quality controls on expansions**

Some transformations can produce noise. The methodology therefore includes targeted safeguards, most notably for the parentheses-based expansion rule, where a maintained blacklist prevents known invalid standalone outputs.

5) **Optional UMLS augmentation**

When UMLS inputs are present, the methodology augments the vocabulary with additional UMLS term strings from English vocabularies. These additions are explicitly labeled by source vocabulary.

6) **Provenance labeling**

Every emitted row is labeled with a `Type` field that records where it came from. Examples include:

- `official`, `official+abbr`
- `canonical:...`
- `enriched:<ruleId>`
- `umls:<vocabulary>`
- `umls:<vocabulary>:<ruleId>` (a rule-derived variant generated from a UMLS term)

This provenance is intended to support downstream analysis (e.g., deciding which term subsets to include).

---

## Key methodological decisions

- **Global uniqueness by term**: the output is treated as a vocabulary, so terms are unique regardless of how many codes could produce them.
- **Traceability**: provenance labels are retained for auditability and ablation studies.
- **Conservative handling of ambiguity**: certain patterns (e.g., single-word “`, unspecified`” stems) are routed to review rather than automatically rewritten.
- **Curated noise suppression**: the parentheses-only blacklist is explicitly maintained to remove recurrent invalid outputs while preserving useful variants.

---

## Where the methodology is implemented

- Main pipeline: [icd10cm_pipeline.py](icd10cm_pipeline.py)
- Enrichment rules: [icd10cm_rules.py](icd10cm_rules.py)
- Parentheses-only blacklist (P1): [p1_blacklist.py](p1_blacklist.py)
