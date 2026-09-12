"""Provider-neutral deterministic text preparation and embeddings for local MVP use."""

from __future__ import annotations

from decimal import Decimal
import hashlib
import math
import re


EMBEDDING_DIMENSION = 1536
EMBEDDING_MODEL = "fixture-embedding-v1"
EMBEDDING_VERSION = "1"


def normalize_product_text(value: str) -> str:
    words = re.findall(r"[a-z0-9%]+", value.casefold())
    ignored = {"sale", "special", "save", "only", "ea", "each", "kg", "g", "l", "ml"}
    cleaned = [word for word in words if word not in ignored and not word.isdigit()]
    return " ".join(cleaned)


def deterministic_embedding(text: str) -> list[Decimal]:
    values = [0.0] * EMBEDDING_DIMENSION
    for token in normalize_product_text(text).split():
        digest = hashlib.sha256(token.encode()).digest()
        index = int.from_bytes(digest[:4], "big") % EMBEDDING_DIMENSION
        values[index] += 1.0 if digest[4] % 2 == 0 else -1.0
    magnitude = math.sqrt(sum(value * value for value in values)) or 1.0
    return [Decimal(f"{value / magnitude:.8f}") for value in values]


def vector_literal(values: list[Decimal]) -> str:
    return "[" + ",".join(str(value) for value in values) + "]"
