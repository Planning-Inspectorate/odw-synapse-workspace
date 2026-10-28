"""run the S62A transform stages locally, then optionally upload the result for the Synapse LOAD pipeline.

runs the code cells of the Synapse notebooks (single source of truth) with LOCAL_DIR set:
    s62a_transform -> s62a_validate -> s62a_user -> s62a_schema_align

input folder (flat) must contain:
    MASTER LEGACY cases S62A - final.xlsx, Spreadsheet to MPA schema mapping.xlsx,
    LPAs.xlsx, S62A_Horizon_ENUM.xlsx, entraid_users.json (copy of s62a/PIPELINE/INPUT/ENTRA/entraid_users.json,
    refreshed by the USER pipeline's "Copy Entra ID Users" activity)

usages:
    python s62a_run_local.py
    python s62a_run_local.py --upload --env test

after --upload, trigger the LOAD pipeline only (not S62A/TRANSFORM, which would overwrite the upload).
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
NOTEBOOK_DIR = HERE.parents[1] / "workspace" / "notebook"
STAGES = ["s62a_transform", "s62a_validate", "s62a_schema_align"]
ACCOUNTS = {
    "dev": "pinssts62adevukseftlkl",
    "test": "pinssts62atestuksnkavj4",
    "prod": "pinssts62aproduksrz8i4g",
}
CONTAINER = "s62a"
SCHEMA_DEST = "PIPELINE/OUTPUT/SCHEMA"
USERS_DEST = "PIPELINE/OUTPUT/USERS"


REQUIRED_INPUTS = [
    "MASTER LEGACY cases S62A - final.xlsx",
    "Spreadsheet to MPA schema mapping.xlsx",
    "LPAs.xlsx",
    "S62A_Horizon_ENUM.xlsx",
    "entraid_users.json",
]


def preflight(input_dir):
    import pandas as pd
    problems = []
    if int(pd.__version__.split(".")[0]) >= 3:
        problems.append(f"pandas {pd.__version__} is not supported, run: pip install \"pandas<3\"")
    problems += [f"missing input file: {f}" for f in REQUIRED_INPUTS if not (input_dir / f).exists()]
    if problems:
        sys.exit("Preflight failed:\n  " + "\n  ".join(problems))


def run_notebook(name, work_dir):
    cells = json.loads((NOTEBOOK_DIR / f"{name}.json").read_text(encoding="utf-8"))["properties"]["cells"]
    ns = {"__name__": "__main__", "display": print}
    first_code = True
    print(f"\n===== {name} =====")
    for i, cell in enumerate(cells):
        if cell["cell_type"] != "code":
            continue
        src = "".join(cell["source"])
        if src.lstrip().startswith("%"):
            continue
        exec(compile(src, f"{name}[cell {i}]", "exec"), ns)
        if first_code:
            ns["LOCAL_DIR"] = str(work_dir)
            first_code = False
    return ns


def newest(work_dir, prefix):
    return max((p for p in work_dir.glob(f"{prefix}_*") if p.is_dir()), key=lambda p: p.name)


def az(*args):
    cmd = ["az", "storage", "blob", *args, "--auth-mode", "login"]
    print(" ".join(cmd))
    subprocess.run(cmd, check=True)


def upload(schema_dir, work_dir, env):
    account = ACCOUNTS[env]
    stamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    latest = f"{SCHEMA_DEST}/latest"
    az("delete-batch", "--account-name", account, "--source", CONTAINER, "--pattern", f"{latest}/*")
    for dest in (f"{SCHEMA_DEST}/{stamp}", latest):
        az("upload-batch", "--account-name", account, "--destination", CONTAINER,
           "--destination-path", dest, "--source", str(schema_dir), "--overwrite")
    az("upload-batch", "--account-name", account, "--destination", CONTAINER, "--destination-path", USERS_DEST,
       "--source", str(work_dir), "--pattern", "User.csv", "--overwrite")
    print(f"Uploaded to {account}/{CONTAINER}/{latest} and {USERS_DEST}/User.csv")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--input-dir", type=Path, default=HERE / "INPUT")
    ap.add_argument("--output-dir", type=Path, default=HERE / "OUTPUT")
    ap.add_argument("--skip-validate", action="store_true")
    ap.add_argument("--upload", action="store_true", help="Upload the schema CSVs to the s62a storage account")
    ap.add_argument("--env", choices=ACCOUNTS, help="Required with --upload")
    args = ap.parse_args()
    if args.upload and not args.env:
        ap.error("--upload requires --env")

    input_dir = args.input_dir.resolve()
    preflight(input_dir)
    work_dir = args.output_dir.resolve() / f"run_{datetime.utcnow():%Y%m%d%H%M%S}"
    work_dir.mkdir(parents=True)
    for f in input_dir.iterdir():
        if f.is_file():
            shutil.copy2(f, work_dir)
    os.chdir(work_dir)

    run_notebook("s62a_transform", work_dir)
    transform_dir = newest(work_dir, "s62a_transform")
    # Later stages look up staging files by name in LOCAL_DIR
    for f in transform_dir.glob("*.csv"):
        shutil.copy2(f, work_dir)

    if not args.skip_validate:
        run_notebook("s62a_validate", work_dir)

    run_notebook("s62a_user", work_dir)   # writes User.csv, users.csv (name -> id lookup used below)

    run_notebook("s62a_schema_align", work_dir)
    schema_dir = newest(work_dir, "s62a_schema")
    print(f"\nSchema CSVs: {schema_dir}")

    if args.upload:
        upload(schema_dir, work_dir, args.env)


if __name__ == "__main__":
    sys.exit(main())
