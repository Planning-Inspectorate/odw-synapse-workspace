from __future__ import annotations

import json
from typing import Dict, Iterable, List, Optional, Sequence, Set

import requests
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.column import Column

from .base import (
    Strategy,
    _seed_col,
    default_strategies,
)
from .config import AnonymisationConfig

# --- Purview HTTP helpers (mirroring purview_df_anonymiser.py) ---

def _purview_base_url(purview_name: str) -> str:
    return f"https://{purview_name}.catalog.purview.azure.com/api/atlas/v2"


def _http_get(url: str, headers: Dict[str, str], timeout: int = 60) -> dict:
    r = requests.get(url, headers=headers, timeout=timeout)
    if not r.ok:
        raise Exception(f"HTTP {r.status_code} – {r.text[:800]}")
    return r.json()


def _get_access_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://purview.azure.net/.default",
    }
    resp = requests.post(token_url, data=data, timeout=60)
    if not resp.ok:
        raise Exception(f"Token request failed [{resp.status_code}]: {resp.text}")
    return resp.json()["access_token"]


def _get_guid_by_unique_attrs(
    purview_name: str, type_name: str, qualified_name: str, api_version: str = "2023-09-01", headers: Optional[Dict[str, str]] = None
) -> str:
    from urllib.parse import quote

    qn = quote(qualified_name, safe="")
    url = (
        f"{_purview_base_url(purview_name)}/entity/uniqueAttribute/type/{quote(type_name, safe='')}"
        f"?attr%3AqualifiedName={qn}&api-version={api_version}"
    )
    data = _http_get(url, headers=headers or {})
    if "entity" in data and "guid" in data["entity"]:
        return data["entity"]["guid"]
    if "guid" in data:
        return data["guid"]
    raise Exception(f"Could not resolve GUID for {type_name} :: {qualified_name}")


def _get_entity_with_refs(
    purview_name: str, guid: str, api_version: str = "2023-09-01", headers: Optional[Dict[str, str]] = None
) -> dict:
    url = f"{_purview_base_url(purview_name)}/entity/guid/{guid}?minExtInfo=true&ignoreRelationships=false&api-version={api_version}"
    return _http_get(url, headers=headers or {})


def _extract_classified_columns(entity_with_refs: dict) -> List[dict]:
    rel_attrs = entity_with_refs.get("entity", {}).get("relationshipAttributes", {})
    attached_schema = rel_attrs.get("attachedSchema", [])
    schema_guid = attached_schema[0]["guid"] if attached_schema else None
    if not schema_guid:
        return []

    # referredEntities already contains column entities with classifications
    columns = entity_with_refs.get("referredEntities", {})
    out = []
    for guid, col in columns.items():
        classifications = col.get("classifications", [])
        if classifications:
            out.append(
                {
                    "column_guid": guid,
                    "column_name": col.get("attributes", {}).get("name"),
                    "column_type": col.get("typeName"),
                    "classifications": [c.get("typeName") for c in classifications],
                }
            )
    return out


def fetch_purview_classifications_by_qualified_name(
    purview_name: str,
    tenant_id: str,
    client_id: str,
    client_secret: str,
    asset_type_name: str,
    asset_qualified_name: str,
    api_version: str = "2023-09-01",
) -> List[dict]:
    """Resolve the asset by type and qualified-name, then return a list of column classifications.

    Returns a list of dicts: { column_name, classifications, column_guid, column_type }
    """
    token = _get_access_token(tenant_id, client_id, client_secret)
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    guid = _get_guid_by_unique_attrs(purview_name, asset_type_name, asset_qualified_name, api_version, headers)
    entity = _get_entity_with_refs(purview_name, guid, api_version, headers)
    return _extract_classified_columns(entity)


# --- Engine ---

class AnonymisationEngine:
    def __init__(
        self,
        strategies: Optional[Sequence[Strategy]] = None,
        config: Optional[AnonymisationConfig] = None,
    ) -> None:
        self.strategies: List[Strategy] = list(strategies or default_strategies())
        self.config = config or AnonymisationConfig()

    def apply(
        self,
        df: DataFrame,
        columns_with_classifications: Sequence[dict],
        classification_allowlist: Optional[Iterable[str]] = None,
    ) -> DataFrame:
        """Apply strategies to columns based on Purview classifications.

        columns_with_classifications: Iterable of dicts with keys: column_name, classifications
        classification_allowlist: if provided, only apply rules for classifications in this set
        """
        allowlist: Optional[Set[str]] = set(classification_allowlist) if classification_allowlist else self.config.classification_allowlist
        out = df
        seed = _seed_col(df)
        existing_cols = set(df.columns)

        for item in columns_with_classifications or []:
            if not isinstance(item, dict):
                continue
            col = item.get("column_name")
            if not col or col not in existing_cols:
                continue
            classes = set(item.get("classifications") or [])
            if allowlist is not None:
                classes = {c for c in classes if c in allowlist}
            if not classes:
                continue

            context = {"is_lm": ("line manager" in col.lower())}

            # Precedence: first strategy whose classification_names intersect applies
            for strat in self.strategies:
                if classes.intersection(strat.classification_names):
                    out = strat.apply(out, col, seed, context)
                    break

        return out

    def apply_from_purview(
        self,
        df: DataFrame,
        purview_name: str,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        asset_type_name: str,
        asset_qualified_name: str,
        api_version: str = "2023-09-01",
        classification_allowlist: Optional[Iterable[str]] = None,
    ) -> DataFrame:
        cols = fetch_purview_classifications_by_qualified_name(
            purview_name=purview_name,
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            asset_type_name=asset_type_name,
            asset_qualified_name=asset_qualified_name,
            api_version=api_version,
        )
        return self.apply(df, cols, classification_allowlist=classification_allowlist)
