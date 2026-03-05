"""
Run migration + assessment on all example Tableau files and produce a confidence report.

Usage:
    python scripts/run_confidence_report.py
"""
import glob
import json
import os
import shutil
import sys
import time
import io
import contextlib

# Ensure project root is importable
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.chdir(ROOT)

from tableau_export.extract_tableau_data import TableauExtractor
from fabric_import.assessment import run_assessment
from fabric_import.import_to_fabric import FabricImporter


def find_tableau_files():
    """Find all .twb and .twbx files under examples/."""
    patterns = [
        os.path.join(ROOT, "examples", "**", "*.twb"),
        os.path.join(ROOT, "examples", "**", "*.twbx"),
    ]
    files = []
    for p in patterns:
        files.extend(glob.glob(p, recursive=True))
    return sorted(set(files))


def run_extraction(tableau_file):
    """Extract Tableau objects, return extracted dict and status."""
    buf = io.StringIO()
    try:
        with contextlib.redirect_stdout(buf):
            ext = TableauExtractor(tableau_file, "tableau_export/")
            result = ext.extract_all()
    except Exception:
        return None

    if not result:
        return None

    extracted = {}
    for jf in glob.glob(os.path.join("tableau_export", "*.json")):
        key = os.path.splitext(os.path.basename(jf))[0]
        with open(jf, "r", encoding="utf-8") as fh:
            extracted[key] = json.load(fh)
    return extracted


def run_fabric_generation(name, output_dir):
    """Run fabric artifact generation, return artifact counts."""
    buf = io.StringIO()
    try:
        with contextlib.redirect_stdout(buf):
            importer = FabricImporter()
            importer.import_all(
                artifacts=["lakehouse", "dataflow", "notebook", "semanticmodel", "pipeline", "pbi"],
                report_name=name,
                output_dir=output_dir,
            )
    except Exception as e:
        return {"error": str(e)}

    # Count generated artifacts — FabricImporter puts them directly in output_dir
    project_dir = output_dir
    counts = {"tables": 0, "pages": 0, "visuals": 0, "queries": 0, "cells": 0}

    # Lakehouse tables
    lh_def = os.path.join(project_dir, f"{name}.Lakehouse", "lakehouse_definition.json")
    if os.path.exists(lh_def):
        try:
            with open(lh_def, "r", encoding="utf-8") as f:
                lh = json.load(f)
            counts["tables"] = len(lh.get("tables", []))
        except Exception:
            pass

    # Dataflow queries (folder-based — .m files in queries/)
    df_queries_dir = os.path.join(project_dir, f"{name}.Dataflow", "queries")
    if os.path.exists(df_queries_dir):
        counts["queries"] = len([f_name for f_name in os.listdir(df_queries_dir)
                                 if f_name.endswith(".m")])
    else:
        # Fallback: check dataflow_definition.json
        df_file = os.path.join(project_dir, f"{name}.Dataflow", "dataflow_definition.json")
        if os.path.exists(df_file):
            try:
                with open(df_file, "r", encoding="utf-8") as f:
                    df = json.load(f)
                counts["queries"] = len(df.get("queries", df.get("entities", [])))
            except Exception:
                counts["queries"] = 1

    # Notebook cells — count cells in .ipynb
    nb_file = os.path.join(project_dir, f"{name}.Notebook", "etl_pipeline.ipynb")
    if os.path.exists(nb_file):
        try:
            with open(nb_file, "r", encoding="utf-8") as f:
                nb = json.load(f)
            counts["cells"] = len(nb.get("cells", []))
        except Exception:
            pass

    # PBI pages/visuals
    report_dir = os.path.join(project_dir, f"{name}.Report", "definition", "pages")
    if os.path.exists(report_dir):
        page_dirs = [d for d in os.listdir(report_dir) if os.path.isdir(os.path.join(report_dir, d))]
        counts["pages"] = len(page_dirs)
        vis_count = 0
        for pd_name in page_dirs:
            vis_dir = os.path.join(report_dir, pd_name, "visuals")
            if os.path.exists(vis_dir):
                vis_count += len([v for v in os.listdir(vis_dir) if os.path.isdir(os.path.join(vis_dir, v))])
        counts["visuals"] = vis_count

    return counts


def compute_confidence_score(report):
    """
    Compute a 0-100 confidence score from assessment results.

    Scoring:
    - Start at 100
    - Each WARN check: -3 points
    - Each FAIL check: -10 points
    - Floor at 0
    """
    score = 100
    score -= report.total_warn * 3
    score -= report.total_fail * 10
    return max(0, min(100, score))


def confidence_label(score):
    if score >= 90:
        return "HIGH"
    if score >= 70:
        return "MEDIUM"
    if score >= 50:
        return "LOW"
    return "VERY LOW"


def main():
    files = find_tableau_files()
    print(f"\nFound {len(files)} Tableau files\n")

    out_base = os.path.join(ROOT, "artifacts", "confidence_test")
    # Try to clean; ignore failures (OneDrive locks on Windows)
    if os.path.exists(out_base):
        try:
            shutil.rmtree(out_base)
        except Exception:
            pass
    os.makedirs(out_base, exist_ok=True)

    results = []

    for f in files:
        name = os.path.splitext(os.path.basename(f))[0]
        ext_str = os.path.splitext(f)[1]

        print(f"  Processing: {name}{ext_str} ...", end=" ", flush=True)
        t0 = time.time()

        # Step 1: Extract
        extracted = run_extraction(f)
        if not extracted:
            print("EXTRACTION FAILED")
            results.append({
                "file": f"{name}{ext_str}",
                "migration": "FAIL",
                "assessment": "N/A",
                "confidence": 0,
                "label": "FAIL",
                "pass": 0, "warn": 0, "fail": 0, "checks": 0,
                "tables": 0, "queries": 0, "visuals": 0, "pages": 0,
                "duration": round(time.time() - t0, 2),
            })
            continue

        # Step 2: Generate Fabric artifacts
        out_dir = os.path.join(out_base, name)
        os.makedirs(out_dir, exist_ok=True)
        counts = run_fabric_generation(name, out_dir)
        migration_ok = "error" not in counts

        # Step 3: Assessment
        try:
            report = run_assessment(extracted, workbook_name=name)
            score = compute_confidence_score(report)
            label = confidence_label(score)
        except Exception:
            report = None
            score = 0
            label = "ERROR"

        duration = round(time.time() - t0, 2)

        results.append({
            "file": f"{name}{ext_str}",
            "migration": "OK" if migration_ok else "FAIL",
            "assessment": report.overall_score if report else "ERROR",
            "confidence": score,
            "label": label,
            "pass": report.total_pass if report else 0,
            "warn": report.total_warn if report else 0,
            "fail": report.total_fail if report else 0,
            "checks": report.total_checks if report else 0,
            "tables": counts.get("tables", 0),
            "queries": counts.get("queries", 0),
            "visuals": counts.get("visuals", 0),
            "pages": counts.get("pages", 0),
            "duration": duration,
        })

        status = "OK" if migration_ok else "FAIL"
        assess_str = report.overall_score if report else "ERR"
        print(f"{status} | {assess_str:>6} | {score}% ({label}) | {duration}s")

    # -- Summary table --
    print("\n" + "=" * 120)
    print("MIGRATION CONFIDENCE REPORT")
    print("=" * 120)
    header = (
        f"{'File':<38} {'Migr':>5} {'Assess':>7} {'Score':>6} "
        f"{'Label':>9} {'Pass':>5} {'Warn':>5} {'Fail':>5} "
        f"{'Tbls':>5} {'Qry':>5} {'Vis':>5} {'Pgs':>4} {'Time':>6}"
    )
    print(header)
    print("-" * 120)
    for r in results:
        line = (
            f"{r['file']:<38} "
            f"{r['migration']:>5} "
            f"{r['assessment']:>7} "
            f"{r['confidence']:>5}% "
            f"{r['label']:>9} "
            f"{r['pass']:>5} "
            f"{r['warn']:>5} "
            f"{r['fail']:>5} "
            f"{r['tables']:>5} "
            f"{r['queries']:>5} "
            f"{r['visuals']:>5} "
            f"{r['pages']:>4} "
            f"{r['duration']:>5}s"
        )
        print(line)
    print("-" * 120)

    # Overall stats
    total = len(results)
    ok = sum(1 for r in results if r["migration"] == "OK")
    green = sum(1 for r in results if r["assessment"] == "GREEN")
    yellow = sum(1 for r in results if r["assessment"] == "YELLOW")
    red = sum(1 for r in results if r["assessment"] == "RED")
    avg_score = sum(r["confidence"] for r in results) / total if total else 0
    total_time = sum(r["duration"] for r in results)

    print(f"\n  Migration success: {ok}/{total}")
    print(f"  Assessment:  GREEN={green}  YELLOW={yellow}  RED={red}")
    print(f"  Average confidence score: {avg_score:.1f}%")
    print(f"  Total time: {total_time:.1f}s")
    print()

    # Save JSON report
    report_path = os.path.join(out_base, "confidence_report.json")
    with open(report_path, "w", encoding="utf-8") as fh:
        json.dump({
            "generated": time.strftime("%Y-%m-%d %H:%M:%S"),
            "total_files": total,
            "migration_success": ok,
            "assessment_green": green,
            "assessment_yellow": yellow,
            "assessment_red": red,
            "average_confidence": round(avg_score, 1),
            "results": results,
        }, fh, indent=2)
    print(f"  Report saved: {report_path}")


if __name__ == "__main__":
    main()
