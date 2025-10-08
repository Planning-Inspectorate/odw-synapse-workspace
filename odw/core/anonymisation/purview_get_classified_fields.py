"""
Anonymisation: Purview utilities to find columns classified in a given container.

This module queries Microsoft Purview (Apache Atlas basic search) for column entities
with specific classifications, then filters by container name in the qualifiedName.

Environment variables respected:
    PURVIEW_ACCOUNT_NAME            default for --purview-account-name
    ODW_CONTAINER_NAME              default for --container
    PURVIEW_CLASSIFICATION_NAMES    JSON array of classification names to match
    AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET  (service principal fallback)

Notes:
- Prefers Managed Identity or DefaultAzureCredential; falls back to service principal via MSAL if available.
- Does not print tokens or secrets.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple
import os
import json
import time
import logging

try:
    import requests  # type: ignore
except Exception as _ex:  # pragma: no cover
    raise RuntimeError("'requests' is required. Install it in your environment.") from _ex


# Public API ------------------------------------------------------------------

@dataclass(frozen=True)
class PurviewColumnClassification:
    guid: Optional[str]
    entity_type: Optional[str]
    column_name: Optional[str]
    qualified_name: Optional[str]
    parent_qualified_name: Optional[str]
    classification_names: Tuple[str, ...]


def get_default_classification_candidates() -> List[str]:
    return [
        "Email Address",
        "MICROSOFT.PERSONAL.EMAIL",
        "MICROSOFT.PII.EmailAddress",
    ]


def find_classified_columns(
    purview_account_name: str,
    *,
    container_filter: str = "odw-raw",
    classification_candidates: Optional[Sequence[str]] = None,
    page_size: int = 100,
    max_pages: int = 50,
    timeout: int = 60,
    auth: Optional[Callable[[], str]] = None,
) -> List[PurviewColumnClassification]:
    """
    Query Purview (Atlas basic search) for columns classified within a container.

    Returns a list of PurviewColumnClassification.
    """
    if not purview_account_name:
        raise ValueError("purview_account_name is required")

    base_url = f"https://{purview_account_name}.purview.azure.com"
    classifications = list(classification_candidates or _classification_candidates_from_env() or get_default_classification_candidates())

    raw_items = _search_columns_via_atlas_basic(
        base_url=base_url,
        classifications=classifications,
        container_filter=container_filter,
        page_size=page_size,
        max_pages=max_pages,
        timeout=timeout,
        auth=auth,
    )
    rows = _normalize_results(raw_items, container_filter)
    return rows


def to_rows(records: Iterable[PurviewColumnClassification]) -> List[Dict[str, Any]]:
    """Convert to plain dict rows, convenient for JSON/CSV serialization."""
    return [
        {
            "guid": r.guid,
            "entityType": r.entity_type,
            "column_name": r.column_name,
            "qualifiedName": r.qualified_name,
            "parentQualifiedName": r.parent_qualified_name,
            "classification_names": list(r.classification_names),
        }
        for r in records
    ]


def to_dataframe(records: Iterable[PurviewColumnClassification]):  # type: ignore[override]
    """Return a pandas.DataFrame if pandas is installed, else raise ImportError."""
    try:
        import pandas as pd  # type: ignore
    except Exception as _ex:  # pragma: no cover
        raise ImportError("pandas is required for to_dataframe; install pandas or use to_rows") from _ex
    return pd.DataFrame(to_rows(list(records)))


# Internal helpers -------------------------------------------------------------

def _classification_candidates_from_env() -> Optional[List[str]]:
    raw = os.getenv("PURVIEW_CLASSIFICATION_NAMES")
    if not raw:
        return None
    try:
        val = json.loads(raw)
        if isinstance(val, list) and all(isinstance(x, str) for x in val):
            return val
    except Exception:
        # Also allow comma-separated fallback
        pass
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return parts or None


def _get_token(auth: Optional[Callable[[], str]] = None) -> str:
    """
    Acquire an access token for the Purview resource scope.
    - If 'auth' is a callable, it must return a bearer token string.
    - Else tries Managed Identity / DefaultAzureCredential.
    - Else falls back to service principal via MSAL if env vars are present.
    """
    scope = "https://purview.azure.net/.default"

    if auth is not None:
        token = auth()
        if not isinstance(token, str) or not token:
            raise RuntimeError("Provided auth() must return a non-empty token string")
        return token

    tenant = os.getenv("AZURE_TENANT_ID", "").strip()
    client = os.getenv("AZURE_CLIENT_ID", "").strip()
    secret = os.getenv("AZURE_CLIENT_SECRET", "").strip()

    # Try azure-identity chain
    try:
        from azure.identity import (  # type: ignore
            ChainedTokenCredential,
            ManagedIdentityCredential,
            DefaultAzureCredential,
        )

        cred_chain = ChainedTokenCredential(
            ManagedIdentityCredential(),
            DefaultAzureCredential(
                exclude_interactive_browser_credential=True,
                exclude_visual_studio_code_credential=True,
            ),
        )
        return cred_chain.get_token(scope).token
    except Exception as e_chain:
        # SP fallback via MSAL if configured
        if tenant and client and secret:
            try:
                import msal  # type: ignore

                app = msal.ConfidentialClientApplication(
                    client, authority=f"https://login.microsoftonline.com/{tenant}", client_credential=secret
                )
                result = app.acquire_token_for_client(scopes=[scope])
                if "access_token" in result:
                    return result["access_token"]
                raise RuntimeError(f"MSAL auth failed: {result}")
            except Exception as e_sp:  # pragma: no cover
                raise RuntimeError(f"Failed to acquire token via azure-identity and service principal. {e_sp}") from e_sp
        raise RuntimeError(f"Failed to acquire token via azure-identity chain. {e_chain}") from e_chain


def _purview_request(
    *,
    base_url: str,
    method: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    retries: int = 3,
    timeout: int = 60,
    auth: Optional[Callable[[], str]] = None,
) -> requests.Response:
    url = f"{base_url}{path}"
    last_exc: Optional[Exception] = None
    for attempt in range(retries):
        token = _get_token(auth)
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        try:
            resp = requests.request(method, url, headers=headers, params=params, json=json_body, timeout=timeout)
        except Exception as ex:
            last_exc = ex
            time.sleep(min(2 ** attempt, 5))
            continue
        if resp.status_code in (401, 403, 429) and attempt < retries - 1:
            time.sleep(1 + attempt)
            continue
        if resp.status_code >= 500 and attempt < retries - 1:
            time.sleep(1 + attempt)
            continue
        return resp
    if last_exc:
        raise last_exc
    return resp  # type: ignore[name-defined]


def _search_columns_via_atlas_basic(
    *,
    base_url: str,
    classifications: Sequence[str],
    container_filter: str,
    page_size: int = 100,
    max_pages: int = 50,
    timeout: int = 60,
    auth: Optional[Callable[[], str]] = None,
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    for cls in classifications:
        offset = 0
        for _ in range(max_pages):
            params = {
                "classification": cls,
                "typeName": "column",
                "query": container_filter,
                "limit": page_size,
                "offset": offset,
            }
            resp = _purview_request(
                base_url=base_url,
                method="GET",
                path="/catalog/api/atlas/v2/search/basic",
                params=params,
                retries=3,
                timeout=timeout,
                auth=auth,
            )
            if not resp.ok:
                logging.warning("Atlas basic search failed for %s: %s %s", cls, resp.status_code, resp.text[:200])
                break
            data = resp.json()
            items = data.get("entities") or []
            if not items:
                break
            results.extend(items)
            if len(items) < page_size:
                break
            offset += page_size
    return results


def _normalize_results(raw_items: List[Dict[str, Any]], container_filter: str) -> List[PurviewColumnClassification]:
    rows: List[PurviewColumnClassification] = []
    for it in raw_items:
        guid = it.get("guid") or it.get("id")
        type_name = it.get("typeName")
        attrs = it.get("attributes") or {}
        name = attrs.get("name") or attrs.get("columnName")
        qn = attrs.get("qualifiedName") or it.get("qualifiedName")
        classes = it.get("classifications") or []
        cls_names: List[str] = []
        for c in classes:
            if isinstance(c, dict):
                n = c.get("typeName") or c.get("name")
                if n:
                    cls_names.append(str(n))
        parent_qn = qn.split("#", 1)[0] if isinstance(qn, str) else None
        rec = PurviewColumnClassification(
            guid=str(guid) if guid is not None else None,
            entity_type=str(type_name) if type_name is not None else None,
            column_name=str(name) if name is not None else None,
            qualified_name=str(qn) if qn is not None else None,
            parent_qualified_name=str(parent_qn) if parent_qn is not None else None,
            classification_names=tuple(sorted(set(cls_names))),
        )
        if rec.qualified_name and container_filter.lower() in rec.qualified_name.lower():
            rows.append(rec)
    return rows


# CLI -------------------------------------------------------------------------

def _parse_args(argv: Optional[Sequence[str]] = None):
    import argparse

    parser = argparse.ArgumentParser(
        description="Find Purview classified columns within a container",
    )
    parser.add_argument(
        "--purview-account-name",
        "-p",
        default=os.getenv("PURVIEW_ACCOUNT_NAME", ""),
        help="Purview account name (without domain). Can also be set via PURVIEW_ACCOUNT_NAME.",
    )
    parser.add_argument(
        "--container",
        "-c",
        default=os.getenv("ODW_CONTAINER_NAME", "odw-raw"),
        help="Container filter applied to qualifiedName (default: odw-raw)",
    )
    parser.add_argument(
        "--classifications",
        "-C",
        default=os.getenv("PURVIEW_CLASSIFICATION_NAMES", ""),
        help="JSON array or comma-separated classification names (default includes common email labels)",
    )
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--max-pages", type=int, default=50)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument(
        "--output",
        "-o",
        default="",
        help="Optional output path. If ends with .csv writes CSV; else JSON.",
    )
    return parser.parse_args(argv)


def _parse_classifications(raw: str) -> Optional[List[str]]:
    if not raw:
        return None
    try:
        val = json.loads(raw)
        if isinstance(val, list) and all(isinstance(x, str) for x in val):
            return val
    except Exception:
        pass
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return parts or None


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    if not args.purview_account_name:
        print("Error: --purview-account-name (or PURVIEW_ACCOUNT_NAME) is required", flush=True)
        return 2

    classifications = _parse_classifications(args.classifications) or get_default_classification_candidates()

    records = find_classified_columns(
        purview_account_name=args.purview_account_name,
        container_filter=args.container,
        classification_candidates=classifications,
        page_size=args.page_size,
        max_pages=args.max_pages,
        timeout=args.timeout,
    )

    # Serialize
    rows = to_rows(records)
    if args.output:
        if args.output.lower().endswith(".csv"):
            try:
                import pandas as pd  # type: ignore
            except Exception:
                # Minimal CSV writer without pandas
                import csv
                fieldnames = list(rows[0].keys()) if rows else [
                    "guid", "entityType", "column_name", "qualifiedName", "parentQualifiedName", "classification_names"
                ]
                with open(args.output, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    for r in rows:
                        # Flatten classification_names for CSV
                        r = dict(r)
                        r["classification_names"] = ";".join(r.get("classification_names", []))
                        writer.writerow(r)
                print(f"Wrote CSV: {args.output}")
                return 0
            # Pandas path
            df = pd.DataFrame(rows)
            df.to_csv(args.output, index=False)
            print(f"Wrote CSV: {args.output}")
        else:
            with open(args.output, "w", encoding="utf-8") as f:
                json.dump(rows, f, indent=2)
            print(f"Wrote JSON: {args.output}")
        return 0

    # No output path: print a summary and a few rows
    print(f"Total classified columns found in container {args.container}: {len(rows)}")
    for r in rows[:20]:
        print(
            f"- {r['column_name']} | {r['qualifiedName']} | classifications={r['classification_names']}"
        )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
