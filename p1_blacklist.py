"""Blacklist/allowlist for P1 (parentheses split) rule.

These lists are used ONLY for the P1 rule's "parentheses-only" outputs
(i.e. emitting just the content inside parentheses as its own term).

All entries must be lowercase and whitespace-normalized.
"""

from __future__ import annotations

from typing import Set


# Parenthetical content that should never be emitted as a standalone term.
BAD_P1_PARENS_ONLY_TOKENS: Set[str] = {
    "acute",
    "angle",
    "arc",
    "atv",
    "bone",
    "chronic",
    "crnt",
    "del",
    "diagnosis",
    "disorder",
    "e",
    "event",
    "fatal",
    "fetal",
    "finding",
    "focal",
    "from",
    "hill",
    "in",
    "into",
    "iris",
    "joint",
    "lab",
    "latus",
    "lead",
    "lens",
    "mechanical",
    "nail",
    "nonthermal",
    "nos",
    "of",
    "old",
    "on",
    "parts",
    "passenger",
    "poly",
    "pubis",
    "separation",
    "stemi",
    "surg",
    "tac",
    "teno",
    "total",
    "tophi",
    "unintentional",
    "upper",
    "uteri",
    "valve",
    "driver",
}


# Multi-token parenthetical content that should never be emitted.
BAD_P1_PARENS_ONLY_PHRASES: Set[str] = {
    "___ mm",
    "acute chronic",
    "allograft autograft",
    "assisted driver passenger",
    "bifurcation replacement",
    "bullous aphakic",
    "chip fracture",
    "complete partial",
    "complete total",
    "congestive congestive",
    "degenerative inflammatory",
    "del syndrome",
    "driver passenger",
    "due to",
    "essential progressive",
    "focal partial",
    "including sutures",
    "in bed",
    "in remission",
    "joint ligament",
    "lab test",
    "mechanical bypass",
    "mechanical lead",
    "meiotic nondisjunction",
    "mitotic nondisjunction",
    "morphologic abnormality",
    "mucoid sanguinous serous",
    "navigational concept",
    "nonmagnetic old",
    "observable entity",
    "on from",
    "or disorder",
    "or disorder etiology",
    "or disorder manifestation",
    "out of",
    "part of",
    "parts of",
    "parts of unintentional",
    "physical finding",
    "postinflammatory post-traumatic",
    "qualifier value",
    "separation upper",
    "thoracic part",
    "vertebral diagnosis",
}


# If you later find a legitimate single-token parentheses-only output that is NOT
# an acronym-like token (digits, or <=5 letters), add it here.
P1_SINGLE_TOKEN_ALLOWLIST: Set[str] = set()
