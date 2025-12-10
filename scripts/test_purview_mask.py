#!/usr/bin/env python3
import argparse
import os
from typing import List, Dict

from pyspark.sql import SparkSession

from odw.core.anonymisation import AnonymisationEngine, fetch_purview_classifications_by_qualified_name


def get_env(name: str, default: str | None = None) -> str:
    v = os.environ.get(name, default)
    if v is None:
        raise SystemExit(f"Missing required env var: {name}")
    return v


def main():
    parser = argparse.ArgumentParser(description="Test masking of emailAddress using Purview classifications for DaRT_LPA.csv")
    parser.add_argument("--purview-name", default=os.environ.get("PURVIEW_NAME"), help="Purview account name (e.g. pins-pview)")
    parser.add_argument("--tenant-id", default=os.environ.get("PURVIEW_TENANT_ID"), help="AAD Tenant ID")
    parser.add_argument("--client-id", default=os.environ.get("PURVIEW_CLIENT_ID"), help="AAD App (client) ID")
    parser.add_argument("--client-secret", default=os.environ.get("PURVIEW_CLIENT_SECRET"), help="AAD App secret")
    parser.add_argument(
        "--asset-qualified-name",
        default=os.environ.get("PURVIEW_ASSET_QUALIFIED_NAME"),
        help="Qualified name of the ADLS Gen2 resource set for DaRT_LPA.csv",
    )
    parser.add_argument(
        "--asset-type-name",
        default=os.environ.get("PURVIEW_ASSET_TYPE_NAME", "azure_datalake_gen2_resource_set"),
        help="Purview type name to resolve (default: azure_datalake_gen2_resource_set)",
    )
    args = parser.parse_args()

    purview_name = args.purview_name or get_env("PURVIEW_NAME")
    tenant_id = args.tenant_id or get_env("PURVIEW_TENANT_ID")
    client_id = args.client_id or get_env("PURVIEW_CLIENT_ID")
    client_secret = args.client_secret or get_env("PURVIEW_CLIENT_SECRET")

    # Resolve asset qualified name if not provided, preferring Synapse mssparkutils
    asset_qn = args.asset_qualified_name
    if not asset_qn:
        storage_host = os.environ.get("DATA_LAKE_STORAGE_HOST") or os.environ.get("DATA_LAKE_STORAGE")
        if storage_host and not storage_host.endswith(".dfs.core.windows.net"):
            storage_host = f"{storage_host}.dfs.core.windows.net"
        if not storage_host:
            try:
                from notebookutils import mssparkutils  # type: ignore
                storage_host = (mssparkutils.notebook.run('/utils/py_utils_get_storage_account') or '').strip('/')
            except Exception:
                storage_host = "pinsstodwdevuks9h80mb.dfs.core.windows.net"
        asset_qn = f"https://{storage_host}/odw-raw/Horizon/{{Year}}-{{Month}}-{{Day}}/DaRT_LPA.csv"

    # 1) Get Purview classifications for the asset
    cols: List[Dict] = fetch_purview_classifications_by_qualified_name(
        purview_name=purview_name,
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
        asset_type_name=args.asset_type_name,
        asset_qualified_name=asset_qn,
    )

    email_items = [c for c in cols if (c.get("column_name") or "").lower() == "emailaddress"]
    if not email_items:
        print("No classifications found for column 'emailAddress'. Columns returned:")
        print(cols[:10])
        raise SystemExit(2)

    email_classes = set(email_items[0].get("classifications") or [])
    print(f"emailAddress classifications: {sorted(email_classes)}")

    # 2) Build a small sample DF with the target column
    spark = SparkSession.builder.master("local[*]").appName("anon-test-dart-lpa").getOrCreate()
    try:
        df = spark.createDataFrame(
            [{"emailAddress": "john.doe@example.com"}, {"emailAddress": "a@b.com"}, {"emailAddress": None}]
        )

        engine = AnonymisationEngine()
        out = engine.apply(
            df,
            columns_with_classifications=[{"column_name": "emailAddress", "classifications": list(email_classes)}],
        )

        rows = out.select("emailAddress").collect()
        masked = [r[0] for r in rows]

        print("Masked results:")
        for m in masked:
            print(" -", m)

        # Validate masking behaviour: mask local part and preserve domain
        expected = ["j******e@example.com", "a@b.com", None]
        ok = masked == expected
        if not ok:
            raise SystemExit("emailAddress masking FAILED (expected to preserve domain and mask local part)")
        print("OK: emailAddress masked as expected")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
