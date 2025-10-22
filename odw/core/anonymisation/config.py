from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Set


@dataclass
class AnonymisationConfig:
    """Configuration loaded from YAML for anonymisation behaviour."""

    classification_allowlist: Optional[Set[str]] = None


def load_config(path: Optional[str] = None, text: Optional[str] = None) -> AnonymisationConfig:
    """Load configuration from a YAML file path or YAML text.

    Schema:
    - classification_allowlist: [list of Purview classification names]
    """
    if not path and not text:
        return AnonymisationConfig()

    data = _yaml_safe_load(text) if text else _load_yaml_file(path) or {}

    allowlist = data.get("classification_allowlist")
    if allowlist is not None:
        allowlist = set(allowlist)

    return AnonymisationConfig(classification_allowlist=allowlist)


def _load_yaml_file(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return _yaml_safe_load(f) or {}


def _yaml_safe_load(stream) -> dict:
    try:
        import yaml  # type: ignore
    except Exception as ex:
        raise ImportError("PyYAML is required to load YAML config. Install 'pyyaml'.") from ex
    return yaml.safe_load(stream)
