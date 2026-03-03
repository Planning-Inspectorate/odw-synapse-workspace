from __future__ import annotations

import os
import json
import re
import logging
from typing import Dict, Iterable, List, Optional, Sequence, Set

import requests
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.column import Column
from azure.identity import DefaultAzureCredential  # type: ignore

from .base import (
    BaseStrategy,
    default_strategies,
)
from .config import AnonymisationConfig

# Prefer core util logging if available; fallback to module logger
logger = logging.getLogger(__name__)
try:
    from odw.core.util.logging_util import LoggingUtil  # type: ignore

    _HAS_LOGGING_UTIL = True
except Exception:
    LoggingUtil = None  # type: ignore
    _HAS_LOGGING_UTIL = False


def _log_event(event: str, level: int = logging.INFO, **fields):
    """PII-safe structured logging helper using core util when available.

    - Formats a single-line JSON payload prefixed to identify the component.
    - Filters None values and converts sets to sorted lists.
    - Avoid logging raw data values (PII).
    """
    try:
        payload: Dict[str, object] = {"event": event}
        if fields:
            norm: Dict[str, object] = {}
            for k, v in fields.items():
                if v is None:
                    continue
                if isinstance(v, set):
                    norm[k] = sorted(v)  # type: ignore[assignment]
                else:
                    norm[k] = v
            payload.update(norm)
        message = f"[AnonymisationEngine] {json.dumps(payload, sort_keys=True)}"
        if _HAS_LOGGING_UTIL:
            try:
                # Only use LoggingUtil if it has already been initialised elsewhere to avoid
                # side effects in unit tests and environments without notebookutils.
                if getattr(LoggingUtil, "_INSTANCE", None) is not None:
                    if level >= logging.ERROR:
                        LoggingUtil().log_error(message)
                    else:
                        LoggingUtil().log_info(message)
                    return
            except Exception:
                # Fallback to std logging if util cannot be used
                pass
        logger.log(level, message)
    except Exception:
        # Last-resort fallback to avoid raising from logging
        try:
            if _HAS_LOGGING_UTIL and getattr(LoggingUtil, "_INSTANCE", None) is not None:
                LoggingUtil().log_info(f"[AnonymisationEngine] {event}")
            else:
                logger.log(level, f"[AnonymisationEngine] {event}")
        except Exception:
            logger.log(level, f"[AnonymisationEngine] {event}")


# --- Defaults and helpers for simplified apply_from_purview API ---
DEFAULT_PURVIEW_NAME = os.getenv("ODW_PURVIEW_NAME", "pins-pview")
DEFAULT_TENANT_ID = os.getenv("ODW_TENANT_ID", "5878df98-6f88-48ab-9322-998ce557088d")
DEFAULT_CLIENT_ID = os.getenv("ODW_CLIENT_ID", "5750ab9b-597c-4b0d-b0f0-f4ef94e91fc0")
DEFAULT_STORAGE_ACCOUNT_DFS_HOST = os.getenv("ODW_STORAGE_ACCOUNT_DFS_HOST", "")


def _norm_col_name(s: Optional[str]) -> str:
    """Normalise a column name for matching across different naming styles.

    - trim + lowercase
    - drop non-alphanumeric characters (so first_name == firstName == firstname)
    - fix common typos (adress -> address)
    """
    raw = (s or "").strip().lower()
    cleaned = re.sub(r"[^a-z0-9]", "", raw)
    cleaned = cleaned.replace("adress", "address")
    return cleaned


def _resolve_client_secret() -> str:
    """Resolve client secret via KeyVault when available, else fall back to Azure Identity sentinel.

    - If env ODW_CLIENT_SECRET is set, use it.
    - Else try (KeyVaultName, SecretName) from Spark conf 'keyVaultName' and env ODW_PURVIEW_SECRET_NAME.
    - Else return 'AZURE_IDENTITY' to trigger DefaultAzureCredential.
    """
    val = os.getenv("ODW_CLIENT_SECRET")
    if val:
        return val
    try:
        from notebookutils import mssparkutils  # type: ignore

        try:
            # Try Spark conf first
            from pyspark.sql import SparkSession  # type: ignore

            sc = SparkSession.builder.getOrCreate().sparkContext
            vault_name = sc.getConf("keyVaultName", None)
        except Exception:
            vault_name = os.getenv("ODW_KEYVAULT_NAME")
        secret_name = os.getenv("ODW_PURVIEW_SECRET_NAME")
        if vault_name and secret_name:
            return mssparkutils.credentials.getSecret(vault_name, secret_name)
    except Exception:
        pass
    return "AZURE_IDENTITY"


def _build_asset_qualified_name_from_params(*, storage_host: str, source_folder: str, entity_name: Optional[str], file_name: Optional[str]) -> str:
    """Build the Purview qualified name for ADLS Gen2 resource sets.

    Notes:
    - ServiceBus assets are JSON files with a timestamp in the filename. The pattern must
      include literal Purview placeholders (e.g. {Year}, {Month}, {Day}, {Hour}, {N}).
    - Horizon assets use a provided file name under a dated folder.
    - entraid assets are JSON files named after the entity under a dated folder.
    """
    host = (storage_host or "").rstrip("/")
    if source_folder == "ServiceBus":
        if not entity_name:
            raise ValueError("entity_name is required for source_folder='ServiceBus'")
        # Example:
        # https://<host>/odw-raw/ServiceBus/<entity>/{Year}-{Month}-{Day}/
        #   <entity>_{Year}-{Month}-{Day}T{Hour}:{N}:{N}.{N}+{N}:{N}.json
        return (
            f"https://{host}/odw-raw/{source_folder}/{entity_name}/"
            f"{{Year}}-{{Month}}-{{Day}}/"
            f"{entity_name}_{{Year}}-{{Month}}-{{Day}}T{{Hour}}:{{N}}:{{N}}.{{N}}+{{N}}:{{N}}.json"
        )
    if source_folder == "Horizon":
        if not file_name:
            raise ValueError("file_name is required for source_folder='Horizon'")
        return f"https://{host}/odw-raw/{source_folder}/{{Year}}-{{Month}}-{{Day}}/{file_name}"
    if source_folder == "entraid":
        if not entity_name:
            raise ValueError("entity_name is required for source_folder='entraid'")
        return f"https://{host}/odw-raw/{source_folder}/{entity_name}/{{Year}}-{{Month}}-{{Day}}/{entity_name}.json"
    raise ValueError("source_folder must be one of 'ServiceBus', 'Horizon', 'entraid'")


# --- Purview HTTP helpers (mirroring purview_df_anonymiser.py) ---


def _purview_base_url(purview_name: str) -> str:
    return f"https://{purview_name}.catalog.purview.azure.com/api/atlas/v2"


def _http_get(url: str, headers: Dict[str, str], timeout: int = 60) -> dict:
    r = requests.get(url, headers=headers, timeout=timeout)
    if not r.ok:
        raise Exception(f"HTTP {r.status_code} – {r.text[:800]}")
    return r.json()


def _get_access_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    """Acquire an access token for Purview.

    Prefers client-credential flow when client_secret is provided. If client_secret is blank
    or equals one of {AZURE_IDENTITY, USE_AZURE_IDENTITY, DEFAULT}, use DefaultAzureCredential.
    """
    if not client_secret or str(client_secret).strip().upper() in {"AZURE_IDENTITY", "USE_AZURE_IDENTITY", "DEFAULT"}:
        return DefaultAzureCredential(exclude_interactive_browser_credential=True).get_token("https://purview.azure.net/.default").token

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


def _get_entity_with_refs(purview_name: str, guid: str, api_version: str = "2023-09-01", headers: Optional[Dict[str, str]] = None) -> dict:
    url = f"{_purview_base_url(purview_name)}/entity/guid/{guid}?minExtInfo=true&ignoreRelationships=false&api-version={api_version}"
    return _http_get(url, headers=headers or {})


def _extract_classified_columns(
    entity_with_refs: dict,
    purview_name: Optional[str] = None,
    headers: Optional[Dict[str, str]] = None,
    api_version: str = "2023-09-01",
) -> List[dict]:
    """Return classified columns for an asset.

    Many Purview assets link to a separate schema entity which holds the columns.
    This function will, when possible, follow the attached schema GUID and read
    its referred entities and relationships to collect column classifications.
    Falls back to scanning the original entity's referredEntities if necessary.
    """
    rel_attrs = (entity_with_refs.get("entity", {}) or {}).get("relationshipAttributes", {}) or {}
    attached_schema = rel_attrs.get("attachedSchema", []) or []
    schema_guid = attached_schema[0].get("guid") if attached_schema else None

    def _collect_from_referred(source: dict, only_guids: Optional[Set[str]] = None) -> List[dict]:
        cols = source.get("referredEntities", {}) or {}
        out: List[dict] = []
        for guid, col in cols.items():
            if only_guids is not None and guid not in only_guids:
                continue
            classifications = col.get("classifications", []) or []
            if classifications:
                out.append(
                    {
                        "column_guid": guid,
                        "column_name": (col.get("attributes", {}) or {}).get("name"),
                        "column_type": col.get("typeName"),
                        "classifications": [c.get("typeName") for c in classifications],
                    }
                )
        return out

    # Prefer extracting from the schema entity if present
    if schema_guid:
        schema_entity = None
        if purview_name:
            try:
                schema_entity = _get_entity_with_refs(purview_name, schema_guid, api_version, headers)
            except Exception:
                schema_entity = None
        # If we couldn't fetch the schema entity, try to work with what we have
        source = schema_entity or entity_with_refs

        # Try to identify the column GUIDs from schema relationships
        schema_rel = (source.get("entity", {}) or {}).get("relationshipAttributes", {}) or {}
        rel_keys = [
            "columns",
            "column",
            "tableColumns",
            "attributes",
            "fields",
            "schemaElements",
        ]
        guids: Set[str] = set()
        for k in rel_keys:
            items = schema_rel.get(k) or []
            for it in items:
                gid = it.get("guid") if isinstance(it, dict) else None
                if gid:
                    guids.add(gid)

        extracted = _collect_from_referred(source, only_guids=guids if guids else None)
        if extracted:
            return extracted

    # Fallback: scan original entity's referredEntities
    return _collect_from_referred(entity_with_refs)


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

    Strategy:
    1) Resolve GUID for (type, qualifiedName) and read entity with refs;
       attempt to extract columns directly (covers most assets).
    2) Fallback: follow attachedSchema -> fetch schema entity -> iterate its
       referred entities, fetch each with refs, and try extraction again. This
       handles cases where the actual column entities are one hop deeper.
    """
    token = _get_access_token(tenant_id, client_id, client_secret)
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    guid = _get_guid_by_unique_attrs(purview_name, asset_type_name, asset_qualified_name, api_version, headers)
    entity = _get_entity_with_refs(purview_name, guid, api_version, headers)

    # Primary extraction attempt
    cols = _extract_classified_columns(entity, purview_name=purview_name, headers=headers, api_version=api_version)
    if cols:
        return cols

    # Fallback: attached schema -> its referred entities -> extract from each
    try:
        rel_attrs = (entity.get("entity", {}) or {}).get("relationshipAttributes", {}) or {}
        attached_schema = rel_attrs.get("attachedSchema", []) or []
        schema_guid = attached_schema[0].get("guid") if attached_schema else None
    except Exception:
        schema_guid = None

    if schema_guid:
        try:
            schema_ent = _get_entity_with_refs(purview_name, schema_guid, api_version, headers)
        except Exception:
            schema_ent = None

        if schema_ent:
            ref_ents = schema_ent.get("referredEntities", {}) or {}
            for ref in ref_ents.values():
                ref_guid = (ref or {}).get("guid")
                if not ref_guid:
                    continue
                try:
                    deep_ent = _get_entity_with_refs(purview_name, ref_guid, api_version, headers)
                    deep_cols = _extract_classified_columns(deep_ent, purview_name=purview_name, headers=headers, api_version=api_version)
                    if deep_cols:
                        return deep_cols
                except Exception:
                    # Ignore and try the next referred entity
                    continue

    # Nothing found
    return []


# --- Engine ---


class AnonymisationEngine:
    def __init__(
        self,
        strategies: Optional[Sequence[BaseStrategy]] = None,
        config: Optional[AnonymisationConfig] = None,
        run_id: Optional[str] = None,
    ):
        self.strategies: List[BaseStrategy] = list(strategies or default_strategies())
        self.config = config or AnonymisationConfig()
        # Correlation ID for auditing; can be provided by caller or read from env
        self.run_id: Optional[str] = run_id or os.getenv("ODW_RUN_ID")

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
        seed = BaseStrategy.seed_col(df)
        # Case-insensitive + trimmed mapping from normalised name -> actual df column
        norm_to_actual = {_norm_col_name(c): c for c in df.columns}

        _log_event(
            "apply.start",
            run_id=self.run_id,
            df_columns=len(df.columns),
            provided_classifications=len(list(columns_with_classifications or [])),
            classification_allowlist=list(allowlist) if allowlist else None,
        )

        applied_info: List[dict] = []

        for item in columns_with_classifications or []:
            if not isinstance(item, dict):
                continue
            col_raw = item.get("column_name")
            col_norm = _norm_col_name(col_raw)
            actual_col = norm_to_actual.get(col_norm)
            if not actual_col:
                continue
            classes = set(item.get("classifications") or [])
            if allowlist is not None:
                classes = {c for c in classes if c in allowlist}
            if not classes:
                continue

            context = {"is_lm": ("line manager" in actual_col.lower()), "classifications": classes}

            # Precedence: first strategy whose classification_names intersect applies
            for strat in self.strategies:
                if classes.intersection(strat.classification_names):
                    out = strat.apply(out, actual_col, seed, context)
                    applied_info.append(
                        {
                            "column": actual_col,
                            "strategy": type(strat).__name__,
                            "classifications": list(classes),
                        }
                    )
                    break

        _log_event(
            "apply.summary",
            run_id=self.run_id,
            applied_count=len(applied_info),
            columns=[i["column"] for i in applied_info],
            strategies=sorted({i["strategy"] for i in applied_info}),
        )

        return out

    def _apply_from_purview_explicit(
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
        _log_event(
            "purview.resolve.start",
            purview_name=purview_name,
            asset_type_name=asset_type_name,
            asset_qualified_name=asset_qualified_name,
            api_version=api_version,
            run_id=self.run_id,
        )
        cols = fetch_purview_classifications_by_qualified_name(
            purview_name=purview_name,
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            asset_type_name=asset_type_name,
            asset_qualified_name=asset_qualified_name,
            api_version=api_version,
        )
        _log_event(
            "purview.resolve.done",
            classifications_found=len(cols or []),
            run_id=self.run_id,
        )

        # Determine which columns will be anonymised (respecting classification_allowlist and strategies)
        allowlist: Optional[Set[str]] = set(classification_allowlist) if classification_allowlist else self.config.classification_allowlist
        df_aug = df
        target_cols: List[str] = []
        norm_to_actual = {_norm_col_name(c): c for c in df.columns}
        for item in cols or []:
            if not isinstance(item, dict):
                continue
            col_name = item.get("column_name")
            col_norm = _norm_col_name(col_name)
            actual_col = norm_to_actual.get(col_norm)
            if not actual_col:
                continue
            classes = set(item.get("classifications") or [])
            if allowlist is not None:
                classes = {c for c in classes if c in allowlist}
            if not classes:
                continue
            if any(classes.intersection(strat.classification_names) for strat in self.strategies):
                target_cols.append(actual_col)

        _log_event(
            "apply_from_purview.columns_selected",
            selected=len(target_cols),
            columns=sorted(set(target_cols)),
            classification_allowlist=list(allowlist) if allowlist else None,
            run_id=self.run_id,
        )

        # Preserve originals for change detection
        for c in target_cols:
            df_aug = df_aug.withColumn(f"__orig__{c}", F.col(c))

        # Apply anonymisation
        out = self.apply(df_aug, cols, classification_allowlist=classification_allowlist)

        # Compute how many rows were actually changed across any anonymised column
        changed_exprs: List[Column] = []
        for c in target_cols:
            orig = f"__orig__{c}"
            if orig in out.columns and c in out.columns:
                changed_exprs.append(~F.col(c).eqNullSafe(F.col(orig)))

        any_changed: Optional[Column] = None
        for expr in changed_exprs:
            any_changed = expr if any_changed is None else (any_changed | expr)

        changed_rows = out.filter(any_changed if any_changed is not None else F.lit(False)).count()
        _log_event(
            "apply_from_purview.summary",
            changed_rows=changed_rows,
            columns=sorted(set(target_cols)),
            run_id=self.run_id,
        )

        # Drop helper columns
        drop_cols = [f"__orig__{c}" for c in target_cols if f"__orig__{c}" in out.columns]
        if drop_cols:
            out = out.drop(*drop_cols)

        return out

    def apply_from_purview(
        self,
        df: DataFrame,
        *args,
        **kwargs,
    ) -> DataFrame:
        """Apply anonymisation using either explicit Purview params (back-compat) or simplified params.

        Back-compat explicit form (existing callers):
            apply_from_purview(df, purview_name=..., tenant_id=..., client_id=..., client_secret=...,
                               asset_type_name=..., asset_qualified_name=..., api_version="2023-09-01",
                               classification_allowlist=None)

        Simplified form (new):
            apply_from_purview(df, entity_name=..., file_name=..., source_folder=...)
        """
        # If explicit parameters are provided, delegate to the explicit implementation
        explicit_keys = {"purview_name", "tenant_id", "client_id", "client_secret", "asset_type_name", "asset_qualified_name"}
        if explicit_keys.intersection(kwargs.keys()) or (len(args) >= 6):
            return self._apply_from_purview_explicit(df, *args, **kwargs)

        # Simplified path
        entity_name: Optional[str] = kwargs.get("entity_name") if "entity_name" in kwargs else (args[0] if len(args) > 0 else None)
        file_name: Optional[str] = kwargs.get("file_name") if "file_name" in kwargs else (args[1] if len(args) > 1 else None)
        source_folder: Optional[str] = kwargs.get("source_folder") if "source_folder" in kwargs else (args[2] if len(args) > 2 else None)
        if not source_folder:
            raise ValueError("source_folder is required (one of 'ServiceBus', 'Horizon', 'entraid')")

        # Resolve storage host:
        storage_host = os.getenv("ODW_STORAGE_ACCOUNT_DFS_HOST") or ""
        if not storage_host:
            try:
                from odw.core.util.util import Util  # type: ignore

                storage_host = Util.get_storage_account() or ""
            except Exception:
                storage_host = ""
        if not storage_host:
            raise ValueError(
                "Could not resolve storage account host. Set ODW_STORAGE_ACCOUNT_DFS_HOST in the notebook "
                "or provide explicit Purview parameters via the explicit apply_from_purview API."
            )

        asset_qualified_name = _build_asset_qualified_name_from_params(
            storage_host=storage_host,
            source_folder=source_folder,
            entity_name=entity_name,
            file_name=file_name,
        )
        client_secret = _resolve_client_secret()

        return self._apply_from_purview_explicit(
            df,
            purview_name=DEFAULT_PURVIEW_NAME,
            tenant_id=DEFAULT_TENANT_ID,
            client_id=DEFAULT_CLIENT_ID,
            client_secret=client_secret,
            asset_type_name="azure_datalake_gen2_resource_set",
            asset_qualified_name=asset_qualified_name,
            api_version=kwargs.get("api_version", "2023-09-01"),
            classification_allowlist=kwargs.get("classification_allowlist"),
        )
