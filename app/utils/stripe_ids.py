"""
Stripe ID normalization for deduplication.

Stripe uses different prefixes for the same logical entity (e.g. sub_, py_, pi_, in_, ch_).
The part after the underscore is the stable identifier. In practice, the first N characters
of that suffix are often identical across object types (pi_, ch_, py_) for the same logical
payment; later characters can differ or be used for sharding. Using a fixed prefix length
(17 chars) for dedup is a safe, consistent way to treat such IDs as the same without
relying on full-suffix equality (which can fail if Stripe varies the tail by type).
"""
from typing import Optional

# First N characters of the suffix (after the underscore) used for dedup.
# Stripe IDs can share this prefix across pi_/ch_/py_/in_/sub_ for the same logical entity.
STRIPE_ID_DEDUP_PREFIX_LEN = 17


def normalize_stripe_id(value: Optional[str]) -> str:
    """
    Return the full part after the first underscore for Stripe IDs.
    Use for display or when you need the full suffix.
    """
    if not value:
        return ""
    s = (value or "").strip()
    if "_" in s:
        return s.split("_", 1)[1]
    return s


def normalize_stripe_id_for_dedup(value: Optional[str], prefix_len: int = STRIPE_ID_DEDUP_PREFIX_LEN) -> str:
    """
    Return the first prefix_len characters of the suffix (after the first underscore).
    Use this for all deduplication: payments are treated as the same if this prefix matches.
    If the suffix is shorter than prefix_len, returns the full suffix.
    """
    suffix = normalize_stripe_id(value)
    if not suffix:
        return ""
    return suffix[:prefix_len]
