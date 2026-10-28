"""run the s62a_folders_documents Synapse notebook locally on PySpark, producing Folder.csv / Document.csv.

executes the notebook's own code cells (single source of truth, nothing is copied), with the Synapse-only bits shimmed:
mssparkutils/notebookutils, display, and the abfss:// ROOT/INPUT_DIR/OUTPUT_DIR paths (redirected to local folders).
the notebook's case filter reads S62aCase.csv, taken from the schema folder of the run chosen below.

input folder: INPUT/DOCUMENT (the document extract CSVs).

usages:
    python s62a_documents_local.py                         # newest OUTPUT/run_*/s62a_schema_*
    python s62a_documents_local.py --schema-dir OUTPUT/run_x/s62a_schema_y

output: <run folder>/documents_<timestamp>/ (Folder.csv, Document.csv, _document_exceptions.csv, ...)
then load with: python s62a_load_local.py --documents-dir <that folder>

requires: pip install "pandas<3" pyspark   (and Java 17+)
"""
import argparse
import json
import os
import shutil
import sys
import time
import types
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
NOTEBOOK = HERE.parents[1] / "workspace" / "notebook" / "s62a_folders_documents.json"


def newest_schema_dir(output_dir):
    dirs = [p for p in output_dir.glob("run_*/s62a_schema_*") if p.is_dir()]
    if not dirs:
        sys.exit(f"No s62a_schema_* folders under {output_dir}")
    return max(dirs, key=lambda p: (p.parent.name, p.name))


def local_path(p):
    p = str(p)
    return p[len("file:"):] if p.startswith("file:") else p


def install_shim():
    fs = types.SimpleNamespace(
        cp=lambda src, dst, recurse=False: (Path(local_path(dst)).parent.mkdir(parents=True, exist_ok=True),
                                            shutil.copy2(local_path(src), local_path(dst))),
        rm=lambda p, recurse=False: shutil.rmtree(local_path(p), ignore_errors=True),
        mkdirs=lambda p: Path(local_path(p)).mkdir(parents=True, exist_ok=True),
    )
    utils = types.SimpleNamespace(
        env=types.SimpleNamespace(getWorkspaceName=lambda: "local-dev"),
        fs=fs,
        credentials=types.SimpleNamespace(getSecretWithLS=lambda *a, **k: ""),
    )
    mod = types.ModuleType("notebookutils")
    mod.mssparkutils = utils
    sys.modules["notebookutils"] = mod


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--input-dir", type=Path, default=HERE / "INPUT" / "DOCUMENT")
    ap.add_argument("--output-dir", type=Path, default=HERE / "OUTPUT")
    ap.add_argument("--schema-dir", type=Path, help="s62a_schema_* folder providing S62aCase.csv (default: newest)")
    args = ap.parse_args()

    input_dir = args.input_dir.resolve()
    if not list(input_dir.rglob("*.csv")):
        sys.exit(f"No extract CSVs under {input_dir}")
    schema_dir = (args.schema_dir or newest_schema_dir(args.output_dir)).resolve()
    if not (schema_dir / "S62aCase.csv").exists():
        sys.exit(f"{schema_dir} has no S62aCase.csv")

    work = schema_dir.parent / f"documents_{datetime.utcnow():%Y%m%d%H%M%S}"
    case_dir = work / "OUTPUT" / "SCHEMA" / "latest"
    case_dir.mkdir(parents=True)
    shutil.copy2(schema_dir / "S62aCase.csv", case_dir)
    print(f"Schema folder: {schema_dir}\nWork folder:   {work}")

    # pyspark's toPandas() timestamp conversion fails on some local timezone setups
    os.environ["TZ"] = "UTC"
    time.tzset()
    install_shim()
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master("local[*]").appName("s62a_documents_local")
             .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    cells = json.loads(NOTEBOOK.read_text(encoding="utf-8"))["properties"]["cells"]
    ns = {"__name__": "__main__", "spark": spark, "display": lambda *a, **k: None}
    for i, cell in enumerate(cells):
        if cell["cell_type"] != "code":
            continue
        src = "".join(cell["source"])
        if src.lstrip().startswith("%"):
            continue
        exec(compile(src, f"s62a_folders_documents[cell {i}]", "exec"), ns)
        if "ROOT" in ns and ns["ROOT"] != str(work):
            # redirect the abfss paths the notebook derived from its config
            ns["ROOT"], ns["INPUT_DIR"], ns["OUTPUT_DIR"] = str(work), str(input_dir), str(work / "OUTPUT" / "DOCUMENTS")

    latest = work / "OUTPUT" / "DOCUMENTS" / "latest"
    print(f"\nDocuments output: {latest}")
    print(f"then: python s62a_load_local.py --documents-dir {latest}")


if __name__ == "__main__":
    sys.exit(main())
