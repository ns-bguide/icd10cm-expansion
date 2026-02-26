"""Core term extraction pipeline (experimental).

Goal
----
Produce a high-level "core term" for each input title/term to improve robustness
in downstream matching.

This is intentionally separate from `icd10cm_pipeline.py` so the methodology can
be evaluated independently before integrating into the main vocabulary build.

Inputs
------
- ICD-10-CM ordered file (e.g. icd10cm_order_2026.txt)
- Optional UMLS atoms + sources (umls_atoms.csv, umls_sources.csv)

Outputs
-------
A CSV mapping each source term to an extracted core term, plus emitted core-term
variants.

This script uses only the Python standard library.
"""

from __future__ import annotations

import argparse
import csv
import math
import re
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple

from icd10cm_pipeline import _load_umls_allowed_vocabularies, iter_rows
from icd10cm_rules import ENRICHMENT_RULES, enrich


WS_RE = re.compile(r"\s+")
TOKEN_RE = re.compile(r"[a-z0-9]+(?:'[a-z]+)?")

# These are common trailing qualifiers/attachments in ICD-like titles. We use
# them as cut points to propose a shorter candidate before scoring.
#
# Important: avoid cutting on generic prepositions like "of" because the head
# concept often occurs after them (e.g. "other forms of plague").
CUT_PATTERNS: Sequence[re.Pattern[str]] = (
    re.compile(r"\bsecondary\s+to\b"),
    re.compile(r"\bdue\s+to\b"),
    re.compile(r"\bwith(out)?\b"),
    re.compile(r"\bwithout\b"),
    re.compile(r"[,;:]"),
)

STOPWORDS: Set[str] = {
    "a",
    "an",
    "and",
    "or",
    "of",
    "in",
    "on",
    "from",
    "to",
    "for",
    "with",
    "without",
    "due",
    "secondary",
    "by",
    "other",
    "unspecified",
    "reason",
    "because",
    # Common modifiers that are rarely the best standalone core.
    "symptomatic",
    "forms",
    "form",
    "type",
    "types",
}


OTHER_FORMS_OF_RE = re.compile(r"^other\s+forms\s+of\s+(?P<head>.+)$")
FORMS_OF_RE = re.compile(r"^forms\s+of\s+(?P<head>.+)$")

OF_SPLIT_RE = re.compile(r"^(?P<head>.+?)\s+of\s+(?P<tail>.+)$")


def _strip_leading_other(tokens: List[str]) -> List[str]:
    if tokens and tokens[0] == "other":
        return tokens[1:]
    return tokens


def _strip_stopwords(tokens: List[str]) -> List[str]:
    return [t for t in tokens if t and t not in STOPWORDS]


def _join(tokens: Iterable[str]) -> str:
    return " ".join(t for t in tokens if t)


@dataclass(frozen=True)
class SourceTerm:
    dataset: str  # icd10cm|umls
    ref: str  # ICD10CM code or UMLS vocabulary
    field: str  # official|official+abbr|query_term|term_string
    text: str


def _normalize(text: str) -> str:
    return WS_RE.sub(" ", (text or "").strip().lower())


def _tokenize(text: str) -> List[str]:
    return TOKEN_RE.findall(_normalize(text))


def _first_cut_prefix(text: str) -> str:
    t = _normalize(text)
    best_i: Optional[int] = None
    for rx in CUT_PATTERNS:
        m = rx.search(t)
        if not m:
            continue
        i = m.start()
        # Avoid cutting too early (e.g. terms that start with a stopword).
        if i < 4:
            continue
        if best_i is None or i < best_i:
            best_i = i
    if best_i is None:
        return t
    return t[:best_i].strip(" ,;:-")


def extract_core_term(text: str, token_freq: Counter) -> str:
    """Extract a core term using a simple, explainable heuristic.

    Steps:
    1) Normalize and take a prefix before common qualifier cut points.
    2) If the prefix is short enough, return it.
    3) If the prefix is long, pick the best low-frequency n-gram span near the
       beginning, favoring earlier spans and multiword spans.

    This is not intended to be perfect; it is an experimental baseline.
    """

    raw = _normalize(text)
    # Special case: "other forms of X" => core is X.
    m = OTHER_FORMS_OF_RE.match(raw)
    if m:
        head = _normalize(m.group("head"))
        if head:
            return head
    m = FORMS_OF_RE.match(raw)
    if m:
        head = _normalize(m.group("head"))
        if head:
            return head

    # Structural case: "<head> of <tail>".
    # Many ICD-like terms are shaped like "syphilis of lung and bronchus" where
    # the clinically essential head is in <head> but the anatomical/site detail
    # is in <tail>. We invert to "<tail> <head>".
    m = OF_SPLIT_RE.match(raw)
    if m:
        head_toks = _strip_stopwords(_tokenize(m.group("head")))
        tail_toks = _tokenize(m.group("tail"))
        tail_toks = _strip_leading_other(tail_toks)
        tail_toks = _strip_stopwords(tail_toks)
        if head_toks and tail_toks:
            return _join(tail_toks + head_toks)

    prefix = _first_cut_prefix(raw)
    if not prefix:
        prefix = raw

    tokens = _tokenize(prefix)
    stripped = _strip_leading_other(tokens)
    if stripped:
        tokens = stripped
    if not tokens:
        return prefix

    # If it's already concise, use it.
    if len(tokens) <= 4:
        cleaned = _strip_stopwords(tokens)
        if cleaned:
            return _join(cleaned)
        return _join(tokens)

    # Ending-head heuristic: if the last token is a plausible head concept
    # (non-stopword), build a core around it by selecting nearby rare modifiers.
    head = tokens[-1]
    if head not in STOPWORDS:
        window = tokens[max(0, len(tokens) - 7) : -1]
        window = _strip_leading_other(window)
        candidates = [t for t in window if t not in STOPWORDS]
        # Prefer up to 2 rare modifiers closest to the head.
        if candidates:
            # Sort by (frequency asc, distance asc)
            dist = {t: (len(window) - 1 - i) for i, t in enumerate(window)}
            ranked = sorted(
                set(candidates),
                key=lambda t: (int(token_freq.get(t, 0)), int(dist.get(t, 0))),
            )
            chosen = ranked[:2]
            # Preserve original order when emitting.
            chosen_set = set(chosen)
            ordered = [t for t in window if t in chosen_set]
            core = _join(ordered + [head])
            if core:
                return core

    # Consider spans near the beginning and near the end.
    N = min(len(tokens), 10)
    max_span = 3

    def token_score(tok: str) -> float:
        # Lower is better (rarer). Add 1 for stability.
        return math.log1p(float(token_freq.get(tok, 0)))

    best: Tuple[float, int, int] = (float("inf"), 0, min(2, N))

    # Head candidates near the start.
    for i in range(0, N):
        for span_len in range(1, max_span + 1):
            j = i + span_len
            if j > N:
                continue
            span = tokens[i:j]
            if all(t in STOPWORDS for t in span):
                continue

            avg = sum(token_score(t) for t in span) / float(len(span))
            stop_pen = 0.40 * sum(1 for t in span if t in STOPWORDS)
            # Prefer earlier spans and multiword spans, but keep it modest.
            penalty = 0.05 * i - 0.08 * (len(span) - 1)
            score = avg + penalty + stop_pen
            if score < best[0]:
                best = (score, i, j)

    # Tail candidates near the end (useful when the head concept is later).
    tail_window = min(len(tokens), 10)
    tail_start = max(0, len(tokens) - tail_window)
    for j in range(len(tokens), tail_start, -1):
        for span_len in range(1, max_span + 1):
            i = j - span_len
            if i < tail_start:
                continue
            span = tokens[i:j]
            if all(t in STOPWORDS for t in span):
                continue
            avg = sum(token_score(t) for t in span) / float(len(span))
            stop_pen = 0.40 * sum(1 for t in span if t in STOPWORDS)
            # Prefer later spans and multiword spans.
            penalty = 0.05 * (len(tokens) - j) - 0.08 * (len(span) - 1)
            score = avg + penalty + stop_pen
            if score < best[0]:
                best = (score, i, j)

    _score, i, j = best
    core_tokens = tokens[i:j]

    # If the best span is a stopword-heavy single token, fall back to first 3.
    if len(core_tokens) == 1 and core_tokens[0] in STOPWORDS:
        core_tokens = tokens[: min(3, len(tokens))]

    return " ".join(core_tokens)


def _core_expansions(core: str) -> List[Tuple[str, str]]:
    """Return (term, type) expansions for a core term."""

    core = _normalize(core)
    out: List[Tuple[str, str]] = []

    if not core:
        return out

    out.append((core, "core"))

    # Remove possessive: "patient's" -> "patient"
    dep = re.sub(r"\b([a-z0-9]+)'s\b", r"\1", core)
    dep = _normalize(dep)
    if dep and dep != core:
        out.append((dep, "core:depossessive"))

    # Patient abbreviations (common in clinical text)
    # - patient's/patient -> pt
    # - patients -> pts
    pt1 = re.sub(r"\bpatient's\b", "pt", core)
    pt1 = re.sub(r"\bpatient\b", "pt", pt1)
    pt1 = _normalize(pt1)
    if pt1 and pt1 != core:
        out.append((pt1, "core:abbr:pt"))

    pts = re.sub(r"\bpatients\b", "pts", core)
    pts = _normalize(pts)
    if pts and pts != core:
        out.append((pts, "core:abbr:pts"))

    # Reuse a conservative subset of enrichment rules for core terms.
    allowed_rule_ids = {"A1", "A2", "A3", "A4", "A5", "B1", "B2", "B3", "B4"}
    core_rules = [r for r in ENRICHMENT_RULES if r.rule_id in allowed_rule_ids]
    for v, rule_id in enrich(core, rules=core_rules, max_variants=0):
        out.append((v, f"core_enriched:{rule_id}"))

    # De-dupe while keeping first type.
    seen: Set[str] = set()
    deduped: List[Tuple[str, str]] = []
    for term, ty in out:
        term = _normalize(term)
        if not term or term in seen:
            continue
        seen.add(term)
        deduped.append((term, ty))

    return deduped


def iter_source_terms(
    *,
    icd_input: Path,
    leaf_only: bool,
    include_official_abbr: bool,
    include_icd: bool,
    include_umls: bool,
    umls_fields: Set[str],
    umls_atoms: Path,
    umls_sources: Path,
) -> Iterator[SourceTerm]:
    if include_icd:
        for row in iter_rows(icd_input):
            if leaf_only and int(row.flag) != 1:
                continue
            off = _normalize(row.long_desc)
            if off:
                yield SourceTerm("icd10cm", row.code, "official", off)
            if include_official_abbr:
                ab = _normalize(row.short_desc)
                if ab and ab != off:
                    yield SourceTerm("icd10cm", row.code, "official+abbr", ab)

    if include_umls:
        if not umls_atoms.exists() or not umls_sources.exists():
            return
        allowed = _load_umls_allowed_vocabularies(umls_sources)
        if not allowed:
            return

        with umls_atoms.open("r", newline="", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for r in reader:
                vocab = (r.get("Source Vocabulary") or r.get("Source") or "").strip()
                if not vocab or vocab not in allowed:
                    continue

                qt = _normalize(r.get("Query Term") or "")
                ts = _normalize(r.get("Term String") or "")

                if "query" in umls_fields and qt:
                    yield SourceTerm("umls", vocab, "query_term", qt)
                if "term" in umls_fields and ts:
                    yield SourceTerm("umls", vocab, "term_string", ts)


def main(argv: Optional[Sequence[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Experimental core-term extraction pipeline")
    p.add_argument("--icd-input", default="icd10cm_order_2026.txt")
    p.add_argument(
        "--leaf-only",
        action="store_true",
        help="Keep only ICD rows where FLAG == 1 (likely leaf nodes)",
    )
    p.add_argument("--include-official-abbr", action="store_true")
    p.add_argument("--no-icd", action="store_true", help="Disable ICD-10-CM input")

    p.add_argument("--umls-atoms", default="umls_atoms.csv")
    p.add_argument("--umls-sources", default="umls_sources.csv")
    p.add_argument("--no-umls", action="store_true", help="Disable UMLS input")
    p.add_argument(
        "--umls-fields",
        default="query,term",
        help="Comma-separated subset of UMLS fields to process: query,term (default: query,term)",
    )

    p.add_argument(
        "--output",
        default="core_term_extraction.csv",
        help="Output CSV path (default: core_term_extraction.csv)",
    )
    p.add_argument(
        "--no-term-txt",
        action="store_true",
        help="Disable writing a terms-only .txt file (unique emitted terms)",
    )
    p.add_argument(
        "--term-txt-output",
        default=None,
        help="Terms-only output path (default: derive from --output)",
    )

    p.add_argument(
        "--max-rows",
        type=int,
        default=0,
        help="Optional cap for debugging (0 = unlimited)",
    )

    args = p.parse_args(argv)

    icd_input = Path(str(args.icd_input))
    umls_atoms = Path(str(args.umls_atoms))
    umls_sources = Path(str(args.umls_sources))
    out_path = Path(str(args.output))

    include_icd = not bool(args.no_icd)
    include_umls = not bool(args.no_umls)
    umls_fields = {
        part.strip().lower()
        for part in str(args.umls_fields).split(",")
        if part.strip()
    }
    if umls_fields - {"query", "term"}:
        print("ERROR: --umls-fields must be a subset of: query,term", file=sys.stderr)
        return 2

    if include_icd and not icd_input.exists():
        print(f"ERROR: ICD input not found: {icd_input}", file=sys.stderr)
        return 2

    # Pass 1: collect token frequencies over all source terms.
    token_freq: Counter = Counter()
    n_source = 0

    max_rows = int(args.max_rows)
    for st in iter_source_terms(
        icd_input=icd_input,
        leaf_only=bool(args.leaf_only),
        include_official_abbr=bool(args.include_official_abbr),
        include_icd=include_icd,
        include_umls=include_umls,
        umls_fields=umls_fields,
        umls_atoms=umls_atoms,
        umls_sources=umls_sources,
    ):
        n_source += 1
        for tok in _tokenize(st.text):
            token_freq[tok] += 1
        if max_rows > 0 and n_source >= max_rows:
            break

    print(f"Loaded source terms: {n_source}")
    print(f"Unique tokens: {len(token_freq)}")

    term_txt_path: Optional[Path] = None
    if not args.no_term_txt:
        if args.term_txt_output:
            term_txt_path = Path(str(args.term_txt_output))
        else:
            if out_path.suffix.lower() == ".csv":
                term_txt_path = out_path.with_suffix(".terms.txt")
            else:
                term_txt_path = Path(str(out_path) + ".terms.txt")

    # Pass 2: write mapping.
    # Output is de-duped by CoreTerm (one row per unique core term).
    core_seen: Set[str] = set()

    # Terms-only output (if enabled) contains unique CoreTerm values.
    unique_terms: Set[str] = set()
    unique_term_list: List[str] = []

    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, quoting=csv.QUOTE_ALL)
        w.writerow(
            [
                "Origin",
                "OriginTerm",
                "CoreTerm",
            ]
        )

        n_written = 0
        n_seen = 0

        for st in iter_source_terms(
            icd_input=icd_input,
            leaf_only=bool(args.leaf_only),
            include_official_abbr=bool(args.include_official_abbr),
            include_icd=include_icd,
            include_umls=include_umls,
            umls_fields=umls_fields,
            umls_atoms=umls_atoms,
            umls_sources=umls_sources,
        ):
            n_seen += 1
            core = extract_core_term(st.text, token_freq)
            core = _normalize(core)
            if not core:
                continue

            # Drop identity mappings: if the extracted core term is identical to
            # the origin term, it doesn't add information.
            if _normalize(st.text) == core:
                continue

            if core in core_seen:
                continue
            core_seen.add(core)

            origin = f"{st.dataset}:{st.ref}:{st.field}"
            w.writerow([origin, st.text, core])
            n_written += 1

            if term_txt_path is not None and core not in unique_terms:
                unique_terms.add(core)
                unique_term_list.append(core)

            if max_rows > 0 and n_seen >= max_rows:
                break

    if term_txt_path is not None:
        with term_txt_path.open("w", encoding="utf-8") as tf:
            for t in unique_term_list:
                tf.write(t)
                tf.write("\n")
        print(f"Terms-only file written: {term_txt_path} (unique terms: {len(unique_term_list)})")

    print(f"Output written: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
