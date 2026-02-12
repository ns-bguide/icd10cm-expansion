"""ICD-10-CM term extraction + enrichment pipeline.

Input:  icd10cm_order_YYYY.txt-like files with columns:
  ORDER CODE FLAG SHORT_DESCRIPTION LONG_DESCRIPTION

Output schema (CSV):
  ICD10CMCode,Term,Type

Type values are designed to help you trace provenance:
  - official
  - official+abbr
  - canonical:official
  - canonical:official+abbr
  - enriched:<ruleId>

Notes
-----
- The last column (long description) is treated as the Official term.
- The second-to-last column (short description) is treated as Official+abbr.
- The FLAG column is preserved only for filtering (leaf-only). In this dataset
  it appears to be 1 for leaf nodes and 0 for non-leaf/grouping rows.

This script uses only the Python standard library.
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, List, Optional, Sequence, Set, Tuple

from icd10cm_rules import EnrichmentStats, RULE_DESCRIPTIONS, enrich


LINE_RE = re.compile(
    r"^(?P<order>\d{5})\s+"
    r"(?P<code>\S+)\s+"
    r"(?P<flag>[01])\s+"
    r"(?P<short>.*?)\s{2,}"
    r"(?P<long>.*)\s*$"
)

# Fixed-width column boundaries observed in icd10cm_order_2026.txt.
# These allow parsing lines where the short description reaches the end of its
# field, leaving only a single space before the long description.
ORDER_SLICE = slice(0, 5)
CODE_SLICE = slice(6, 13)
FLAG_INDEX = 14
DESC_START_INDEX = 16
LONG_DESC_INDEX = 76

FINAL_PUNCT_RE = re.compile(r"[\s\u00a0]*[\.!?,;:]+\s*$")
WS_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class IcdRow:
    order: int
    code: str
    flag: int
    short_desc: str
    long_desc: str


def parse_line(line: str) -> Optional[IcdRow]:
    raw = line.rstrip("\r\n")
    if not raw.strip():
        return None

    # Prefer fixed-width parsing when the line looks like the expected layout.
    # This is necessary for rows where the short+abbr description is long enough
    # that there is only a single space between short and long descriptions.
    if len(raw) >= LONG_DESC_INDEX and raw[ORDER_SLICE].isdigit():
        flag_ch = raw[FLAG_INDEX : FLAG_INDEX + 1]
        if flag_ch in ("0", "1"):
            order = int(raw[ORDER_SLICE])
            code = raw[CODE_SLICE].strip()
            short_desc = raw[DESC_START_INDEX:LONG_DESC_INDEX].rstrip()
            long_desc = raw[LONG_DESC_INDEX:].strip()
            if not long_desc:
                long_desc = short_desc
            return IcdRow(
                order=order,
                code=code,
                flag=int(flag_ch),
                short_desc=short_desc,
                long_desc=long_desc,
            )

    # Fallback: whitespace + 2+ spaces between the description columns.
    m = LINE_RE.match(raw)
    if m:
        return IcdRow(
            order=int(m.group("order")),
            code=m.group("code"),
            flag=int(m.group("flag")),
            short_desc=m.group("short").strip(),
            long_desc=m.group("long").strip(),
        )

    # Last resort: parse order/code/flag and treat remaining text as both short/long.
    m2 = re.match(r"^(?P<order>\d{5})\s+(?P<code>\S+)\s+(?P<flag>[01])\s+(?P<rest>.*)$", raw)
    if m2:
        rest = m2.group("rest").strip()
        return IcdRow(
            order=int(m2.group("order")),
            code=m2.group("code"),
            flag=int(m2.group("flag")),
            short_desc=rest,
            long_desc=rest,
        )

    return None


def canonicalize(term: str) -> str:
    term = term.strip().lower()
    term = FINAL_PUNCT_RE.sub("", term)
    term = WS_RE.sub(" ", term).strip()
    return term


def _normalize_spaces(term: str) -> str:
    return WS_RE.sub(" ", term).strip()


def iter_rows(path: Path) -> Iterator[IcdRow]:
    with path.open("r", encoding="utf-8", errors="replace") as f:
        for raw in f:
            row = parse_line(raw)
            if row is not None:
                yield row


def emit_rows(
    icd_row: IcdRow,
    *,
    include_official_abbr: bool,
    include_canonical: bool,
    include_enriched: bool,
    enriched_max_per_term: int,
    enrichment_stats: Optional[EnrichmentStats] = None,
) -> List[Tuple[str, str]]:
    """Return list of (term, type)."""

    output: List[Tuple[str, str]] = []

    official = icd_row.long_desc
    official_abbr = icd_row.short_desc

    output.append((official, "official"))
    if include_official_abbr and official_abbr and official_abbr != official:
        output.append((official_abbr, "official+abbr"))

    if include_canonical:
        for term, base_type in list(output):
            canon = canonicalize(term)
            output.append((canon, f"canonical:{base_type}"))

    if include_enriched:
        # Enrich canonical terms only (avoids mixing casing/punctuation variants).
        canon_terms = [(t, ty) for (t, ty) in output if ty.startswith("canonical:")]
        for canon_term, _canon_type in canon_terms:
            for variant, rule_id in enrich(
                canon_term,
                max_variants=enriched_max_per_term,
                stats=enrichment_stats,
            ):
                output.append((variant, f"enriched:{rule_id}"))

    # De-dup terms per ICD row while keeping first provenance.
    seen_term: Set[str] = set()
    deduped: List[Tuple[str, str]] = []
    for term, ty in output:
        term = term.strip()
        if not term or term in seen_term:
            continue
        seen_term.add(term)
        deduped.append((term, ty))

    return deduped


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Extract + enrich ICD-10-CM terms")
    parser.add_argument(
        "--input",
        default="icd10cm_order_2026.txt",
        help="Path to icd10cm_order_*.txt (default: icd10cm_order_2026.txt)",
    )
    parser.add_argument(
        "--output",
        default="icd10cm_terms_2026.csv",
        help="Output CSV path (default: icd10cm_terms_2026.csv)",
    )
    parser.add_argument(
        "--leaf-only",
        action="store_true",
        help="Keep only rows where FLAG == 1 (likely leaf nodes)",
    )
    parser.add_argument(
        "--include-official-abbr",
        action="store_true",
        help="Include the short description column as official+abbr when different",
    )
    parser.add_argument(
        "--no-canonical",
        action="store_true",
        help="Disable canonical term generation",
    )
    parser.add_argument(
        "--no-enriched",
        action="store_true",
        help="Disable enrichment rule variants",
    )
    parser.add_argument(
        "--enriched-max-per-term",
        type=int,
        default=25,
        help="Max variants per canonical term (default: 25)",
    )
    parser.add_argument(
        "--no-rule-report",
        action="store_true",
        help="Disable per-rule enrichment report printing",
    )

    args = parser.parse_args(argv)

    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        print(f"ERROR: input not found: {input_path}", file=sys.stderr)
        return 2

    counts = Counter()
    parsed_rows = 0
    written_rows = 0

    enrichment_stats: Optional[EnrichmentStats] = None
    if not args.no_enriched:
        enrichment_stats = EnrichmentStats()

    with output_path.open("w", newline="", encoding="utf-8") as out_f:
        writer = csv.writer(out_f)
        writer.writerow(["ICD10CMCode", "Term", "Type"])

        for row in iter_rows(input_path):
            parsed_rows += 1
            if args.leaf_only and row.flag != 1:
                continue

            emitted = emit_rows(
                row,
                include_official_abbr=bool(args.include_official_abbr),
                include_canonical=not args.no_canonical,
                include_enriched=not args.no_enriched,
                enriched_max_per_term=int(args.enriched_max_per_term),
                enrichment_stats=enrichment_stats,
            )
            for term, ty in emitted:
                writer.writerow([row.code, term, ty])
                counts[ty] += 1
                written_rows += 1

    print(f"Parsed ICD rows: {parsed_rows}")
    if args.leaf_only:
        print("Filter: leaf-only (FLAG == 1)")
    print(f"Output rows written: {written_rows}")
    for ty, n in counts.most_common():
        print(f"  {ty}: {n}")

    if enrichment_stats is not None and not args.no_rule_report:
        print("\nEnrichment rule impact (canonical terms only):")
        print(f"  Canonical terms processed for enrichment: {enrichment_stats.terms_seen}")
        rows: List[Tuple[str, int, int, str]] = []
        for rule_id in sorted(RULE_DESCRIPTIONS.keys()):
            affected = int(enrichment_stats.affected_terms.get(rule_id, 0))
            added = int(enrichment_stats.variants_added.get(rule_id, 0))
            if affected == 0 and added == 0:
                continue
            rows.append((rule_id, affected, added, RULE_DESCRIPTIONS.get(rule_id, "")))

        # Sort by most affected terms, then by variants added.
        rows.sort(key=lambda r: (r[1], r[2], r[0]), reverse=True)
        for rule_id, affected, added, desc in rows:
            desc = (desc or "").replace("\n", " ").strip()
            if desc:
                print(f"  {rule_id:<3} terms_affected={affected:<6} variants_added={added:<6} {desc}")
            else:
                print(f"  {rule_id:<3} terms_affected={affected:<6} variants_added={added:<6}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
