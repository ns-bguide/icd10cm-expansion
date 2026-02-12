"""Enrichment rules for ICD-10-CM term expansion.

This module is intentionally self-contained (stdlib only) so rules are easy to
add/edit without touching the pipeline logic.

Key concepts
------------
- Input terms are *canonical* (lowercase, trimmed, normalized whitespace).
- Rules yield candidate variants; `enrich()` de-dupes and enforces max fanout.
- Stats (optional) track how many terms each rule affects and how many variants
  are actually added.
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple


WS_RE = re.compile(r"\s+")


def normalize_spaces(term: str) -> str:
    return WS_RE.sub(" ", term).strip()


@dataclass(frozen=True)
class EnrichmentRule:
    rule_id: str
    description: str
    apply: Callable[[str], Iterable[str]]


@dataclass
class EnrichmentStats:
    # How many canonical terms each rule fired on (per enrich() call).
    affected_terms: Counter
    # How many variants were successfully added (after de-dupe + max cap).
    variants_added: Counter
    # Total canonical terms passed into enrich().
    terms_seen: int = 0

    def __init__(self) -> None:
        self.affected_terms = Counter()
        self.variants_added = Counter()
        self.terms_seen = 0


def _normalize_dashes(term: str) -> str:
    return term.replace("–", "-").replace("—", "-")


def _rule_hyphen_to_space(term: str) -> Iterable[str]:
    if not any(ch in term for ch in ("-", "–", "—")):
        return []
    t = _normalize_dashes(term)
    return [t.replace("-", " ")]


def _rule_hyphen_remove(term: str) -> Iterable[str]:
    if not any(ch in term for ch in ("-", "–", "—")):
        return []
    t = _normalize_dashes(term)
    return [t.replace("-", "")]


def _rule_remove_apostrophes(term: str) -> Iterable[str]:
    if "'" not in term and "’" not in term:
        return []
    return [term.replace("’", "").replace("'", "")]


def _rule_swap_and_amp(term: str) -> Iterable[str]:
    out: List[str] = []
    if " and " in term:
        out.append(term.replace(" and ", " & "))
    if " & " in term:
        out.append(term.replace(" & ", " and "))
    return out


def _rule_swap_or_slash(term: str) -> Iterable[str]:
    out: List[str] = []
    if " or " in term:
        out.append(term.replace(" or ", " / "))
    if " / " in term:
        out.append(term.replace(" / ", " or "))
    return out


def _regex_sub_rule(pattern: str, replacement: str) -> Callable[[str], Iterable[str]]:
    rx = re.compile(pattern)

    def _apply(term: str) -> Iterable[str]:
        if not rx.search(term):
            return []
        return [rx.sub(replacement, term)]

    return _apply


def _rule_due_to_variants(term: str) -> Iterable[str]:
    rx = re.compile(r"\bdue\s+to\b")
    if not rx.search(term):
        return []
    return [
        rx.sub("because of", term),
        rx.sub("caused by", term),
    ]


def _rule_unspecified_suffix_to_prefix(term: str) -> Iterable[str]:
    suffix = ", unspecified"
    if not term.endswith(suffix):
        return []
    stem = term[: -len(suffix)].rstrip()
    if stem.endswith(","):
        stem = stem[:-1].rstrip()
    if not stem:
        return []
    return [f"unspecified {stem}"]


# Add/edit rules here.
ENRICHMENT_RULES: Sequence[EnrichmentRule] = (
    EnrichmentRule("A1", "Replace hyphens with spaces", _rule_hyphen_to_space),
    EnrichmentRule("A2", "Remove hyphens", _rule_hyphen_remove),
    EnrichmentRule("A3", "Remove apostrophes", _rule_remove_apostrophes),
    EnrichmentRule("A4", "Swap 'and' <-> '&'", _rule_swap_and_amp),
    EnrichmentRule("A5", "Swap 'or' <-> '/'", _rule_swap_or_slash),
    # B rules (abbreviations, bidirectional). Word boundaries avoid partial hits.
    EnrichmentRule("B1", "syndrome <-> synd", _regex_sub_rule(r"\\bsyndrome\\b", "synd")),
    EnrichmentRule("B1", "syndrome <-> synd", _regex_sub_rule(r"\\bsynd\\b", "syndrome")),
    EnrichmentRule("B2", "chronic <-> chr", _regex_sub_rule(r"\\bchronic\\b", "chr")),
    EnrichmentRule("B2", "chronic <-> chr", _regex_sub_rule(r"\\bchr\\b", "chronic")),
    EnrichmentRule("B3", "acute <-> acu", _regex_sub_rule(r"\\bacute\\b", "acu")),
    EnrichmentRule("B3", "acute <-> acu", _regex_sub_rule(r"\\bacu\\b", "acute")),
    EnrichmentRule("B4", "left/right <-> lt/rt", _regex_sub_rule(r"\\bleft\\b", "lt")),
    EnrichmentRule("B4", "left/right <-> lt/rt", _regex_sub_rule(r"\\bright\\b", "rt")),
    EnrichmentRule("B4", "left/right <-> lt/rt", _regex_sub_rule(r"\\blt\\b", "left")),
    EnrichmentRule("B4", "left/right <-> lt/rt", _regex_sub_rule(r"\\brt\\b", "right")),
    EnrichmentRule("C1", "due to -> because of|caused by", _rule_due_to_variants),
    EnrichmentRule("C2", "suffix ', unspecified' -> prefix 'unspecified'", _rule_unspecified_suffix_to_prefix),
)


RULE_DESCRIPTIONS: Dict[str, str] = {
    # Keep one canonical description per rule id for reporting.
    "A1": "Replace hyphens with spaces",
    "A2": "Remove hyphens",
    "A3": "Remove apostrophes",
    "A4": "Swap 'and' <-> '&'",
    "A5": "Swap 'or' <-> '/'",
    "B1": "syndrome <-> synd",
    "B2": "chronic <-> chr",
    "B3": "acute <-> acu",
    "B4": "left/right <-> lt/rt",
    "C1": "due to -> because of|caused by",
    "C2": "suffix ', unspecified' -> prefix 'unspecified'",
}


def enrich(
    term: str,
    *,
    max_variants: int = 50,
    rules: Sequence[EnrichmentRule] = ENRICHMENT_RULES,
    stats: Optional[EnrichmentStats] = None,
) -> List[Tuple[str, str]]:
    """Return (variant, rule_id) pairs. Input term should already be canonical."""

    if stats is not None:
        stats.terms_seen += 1

    variants: List[Tuple[str, str]] = []
    seen: Set[str] = {term}
    fired_rules: Set[str] = set()
    local_added: Counter = Counter()

    def add(new_term: str, rule_id: str) -> bool:
        nonlocal variants
        if len(variants) >= max_variants:
            return False
        new_term = normalize_spaces(new_term)
        if not new_term or new_term in seen:
            return False
        seen.add(new_term)
        variants.append((new_term, rule_id))
        fired_rules.add(rule_id)
        local_added[rule_id] += 1
        return True

    for rule in rules:
        for candidate in rule.apply(term):
            add(candidate, rule.rule_id)

    if stats is not None:
        for rule_id in fired_rules:
            stats.affected_terms[rule_id] += 1
        for rule_id, n in local_added.items():
            stats.variants_added[rule_id] += n

    return variants
