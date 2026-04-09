from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Set

import yaml


@dataclass
class AnonymisationConfig:
    """Configuration loaded from YAML for anonymisation behaviour."""

    classification_allowlist: Optional[Set[str]] = None
    seed_column: Optional[str] = None
    entity_seed_columns: Optional[Dict[str, str]] = None

    def get_seed_column(self, entity_name: Optional[str] = None) -> Optional[str]:
        """Return the seed column for the given entity.

        Checks entity_seed_columns first, then falls back to seed_column.
        """
        if entity_name and self.entity_seed_columns:
            entity_col = self.entity_seed_columns.get(entity_name)
            if entity_col:
                return entity_col
        return self.seed_column


def load_config(path: Optional[str] = None, text: Optional[str] = None) -> AnonymisationConfig:
    """Load configuration from a YAML file path or YAML text.

    Schema:
    - classification_allowlist: [list of Purview classification names]
    - seed_column: default column name used as the anonymisation seed
    - entity_seed_columns: per-entity overrides mapping entity name to seed column
    """
    if not path and not text:
        return AnonymisationConfig()

    raw_data = _yaml_safe_load(text) if text else _load_yaml_file(path)
    data = raw_data or {}

    allowlist = data.get("classification_allowlist")
    if allowlist is not None:
        allowlist = set(allowlist)

    entity_seed_columns = data.get("entity_seed_columns")
    if entity_seed_columns is not None:
        entity_seed_columns = dict(entity_seed_columns)

    return AnonymisationConfig(
        classification_allowlist=allowlist,
        seed_column=data.get("seed_column"),
        entity_seed_columns=entity_seed_columns,
    )


def _load_yaml_file(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return _yaml_safe_load(f) or {}


def _yaml_safe_load(stream) -> dict:
    return yaml.safe_load(stream)
