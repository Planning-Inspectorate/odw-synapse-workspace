"""load the S62A schema CSVs (output of s62a_run_local.py) into a local SQL Server (crown-developments docker db).

mirrors the Synapse LOAD pipeline: each table is upserted on `id` (MERGE: update matching rows, insert new ones),
in dependency order, with User.csv loaded after Contact. _exceptions/_rejected/_load_order CSVs and Lpa are not loaded here.

connection: defaults to SQL_CONNECTION_STRING_ADMIN in crown-developments/packages/database/.env
(sqlserver://host:port;database=..;user=..;password=..;trustServerCertificate=true). Override with --env-file,
or with LOCAL_DB_HOST/PORT/NAME/USER/PASSWORD environment variables.

usages:
    python s62a_load_local.py                       # newest run in OUTPUT/
    python s62a_load_local.py --schema-dir OUTPUT/run_x/s62a_schema_y
    python s62a_load_local.py --documents-dir <.../OUTPUT/DOCUMENTS/latest>   # also Folder, Document
    python s62a_load_local.py --dry-run             # validate files vs DB columns, write nothing

requires: pip install pymssql
"""
import argparse
import csv
import os
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
DEFAULT_ENV_FILE = Path.home() / "PINS/crown-developments/packages/database/.env"
BATCH = 500

# Synapse LOAD order (Lpa is not loaded by Synapse)
LOAD_ORDER = [
    "Address", "Organisation", "Contact", "OrganisationToContact", "User", "S62aCase", "S62aDates", "S62aFees",
    "S62aEvent", "S62aResidential", "S62aNonResidential", "S62aToApplicant", "S62aCaseInspector",
    "S62aVehicleParking", "S62aCaseWasteType", "ApplicationNote", "S62aResidentialHousing",
    "S62aNonResidentialFloorspace", "S62aNonResidentialFloorspaceArea", "ApplicationHistory",
]
# LOAD_DOCUMENTS pipeline: loaded only with --documents-dir
DOCUMENT_TABLES = ["Folder", "Document"]

csv.field_size_limit(sys.maxsize)


def connection_settings(env_file):
    cfg = {"host": "localhost", "port": 1433, "database": "pins_crown_dev", "user": "sa", "password": None}
    if env_file.exists():
        m = re.search(r'^SQL_CONNECTION_STRING_ADMIN="?([^"\n]+)"?', env_file.read_text(), re.M)
        if m:
            url = m.group(1).split("://", 1)[-1]
            hostport, *pairs = url.split(";")
            host, _, port = hostport.partition(":")
            cfg.update(host=host or cfg["host"], port=int(port or 1433))
            for p in pairs:
                k, _, v = p.partition("=")
                k = k.strip().lower()
                if k in ("database", "user", "password"):
                    cfg[k] = v
    for key, env in (("host", "LOCAL_DB_HOST"), ("port", "LOCAL_DB_PORT"), ("database", "LOCAL_DB_NAME"),
                     ("user", "LOCAL_DB_USER"), ("password", "LOCAL_DB_PASSWORD")):
        if os.environ.get(env):
            cfg[key] = os.environ[env]
    if not cfg["password"]:
        sys.exit(f"No DB password found in {env_file} or LOCAL_DB_PASSWORD")
    return cfg


def newest_schema_dir(output_dir):
    dirs = [p for p in output_dir.glob("run_*/s62a_schema_*") if p.is_dir()]
    if not dirs:
        sys.exit(f"No s62a_schema_* folders under {output_dir}")
    return max(dirs, key=lambda p: (p.parent.name, p.name))


def table_columns(cur, table):
    cur.execute(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME=%s "
        "ORDER BY ORDINAL_POSITION", (table,))
    return [r[0] for r in cur.fetchall()]


def read_rows(path):
    with path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if header is None:
            return [], []
        return header, [[None if v == "" else v for v in row] for row in reader]


def load_table(conn, table, path, dry_run):
    header, rows = read_rows(path)
    if not rows:
        print(f"{table:34} skipped (no rows)")
        return
    cur = conn.cursor()
    db_cols = table_columns(cur, table)
    if not db_cols:
        raise RuntimeError(f"table dbo.{table} not found in database")
    lower = {c.lower(): c for c in db_cols}
    cols = [lower[h.lower()] for h in header if h.lower() in lower]
    extra = [h for h in header if h.lower() not in lower]
    if extra:
        print(f"  WARNING {table}: CSV columns not in DB, ignored: {', '.join(extra)}")
    if "id" not in [c.lower() for c in cols]:
        raise RuntimeError(f"{table}: no 'id' column to upsert on")
    idx = [i for i, h in enumerate(header) if h.lower() in lower]
    if dry_run:
        print(f"{table:34} {len(rows):5} rows OK (dry run)")
        return

    q = lambda c: f"[{c}]"
    col_list = ", ".join(q(c) for c in cols)
    cur.execute(f"SELECT TOP 0 {col_list} INTO #stage FROM [dbo].[{table}]")
    ph = ", ".join(["%s"] * len(cols))
    for i in range(0, len(rows), BATCH):
        cur.executemany(f"INSERT INTO #stage ({col_list}) VALUES ({ph})",
                        [tuple(r[j] for j in idx) for r in rows[i:i + BATCH]])
    non_id = [c for c in cols if c.lower() != "id"]
    update = f"WHEN MATCHED THEN UPDATE SET {', '.join(f't.{q(c)} = s.{q(c)}' for c in non_id)} " if non_id else ""
    cur.execute(
        f"MERGE [dbo].[{table}] AS t USING #stage AS s ON t.[id] = s.[id] {update}"
        f"WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({', '.join('s.' + q(c) for c in cols)});")
    cur.execute("DROP TABLE #stage")
    conn.commit()
    print(f"{table:34} {len(rows):5} rows upserted")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--schema-dir", type=Path, help="s62a_schema_* folder (default: newest under --output-dir)")
    ap.add_argument("--output-dir", type=Path, default=HERE / "OUTPUT")
    ap.add_argument("--user-csv", type=Path, help="User.csv (default: the run folder containing the schema folder)")
    ap.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE)
    ap.add_argument("--documents-dir", type=Path,
                    help="folder with Folder.csv/Document.csv (from s62a_documents_local.py); also loads Folder, Document")
    ap.add_argument("--allow-remote", action="store_true", help="Allow a host other than localhost/127.0.0.1")
    ap.add_argument("--yes", action="store_true", help="Skip the confirmation prompt")
    ap.add_argument("--dry-run", action="store_true", help="Check files against DB columns without writing")
    ap.add_argument("--only", nargs="+", metavar="TABLE", help="Load only these tables")
    args = ap.parse_args()

    schema_dir = (args.schema_dir or newest_schema_dir(args.output_dir)).resolve()
    user_csv = (args.user_csv or schema_dir.parent / "User.csv").resolve()
    print(f"Schema folder: {schema_dir}")

    import pymssql
    cfg = connection_settings(args.env_file)
    if cfg["host"].lower() not in ("localhost", "127.0.0.1", "::1") and not args.allow_remote:
        sys.exit(f"Refusing to load into non-local host '{cfg['host']}' (use --allow-remote to override)")
    conn = pymssql.connect(server=cfg["host"], port=str(cfg["port"]), user=cfg["user"], password=cfg["password"],
                           database=cfg["database"], autocommit=False)
    print(f"Connected to {cfg['host']}:{cfg['port']}/{cfg['database']} as {cfg['user']}")
    cur = conn.cursor()
    cur.execute("SELECT @@SERVERNAME, DB_NAME(), @@VERSION")
    server, db, version = cur.fetchone()
    print(f"Server reports: instance={server} database={db} ({version.splitlines()[0]})")
    cur.execute("SELECT COUNT(*) FROM [dbo].[S62aCase]")
    print(f"Existing S62aCase rows in target: {cur.fetchone()[0]}")
    if not args.dry_run and not args.yes and input(f"Write to database '{db}'? [y/N] ").strip().lower() != "y":
        sys.exit("Aborted")

    try:
        plan = [(t, user_csv if t == "User" else schema_dir / f"{t}.csv") for t in LOAD_ORDER]
        if args.documents_dir:
            plan += [(t, args.documents_dir.resolve() / f"{t}.csv") for t in DOCUMENT_TABLES]
        for table, path in plan:
            if args.only and table not in args.only:
                continue
            if not path.exists():
                print(f"{table:34} skipped (missing {path.name})")
                continue
            try:
                load_table(conn, table, path, args.dry_run)
            except Exception:
                conn.rollback()
                print(f"\nFAILED on {table}; earlier tables stay committed (re-run is safe, loads are upserts)")
                raise
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
