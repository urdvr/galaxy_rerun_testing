#!/usr/bin/env python3
"""
Build workflow artifacts from a Galaxy IWC-style workflows directory.

Given an input workflows directory, this script:
- Replicates the nested directory structure into an output directory.
- Copies over the following files when present in each workflow folder:
  - README.md
  - {workflow_name}.ga (Galaxy workflow definition). If multiple .ga files exist, all are copied.
- Copies the entire 'test_data' (or 'test-data') directory recursively when present (preserving structure).
- Generates a simplified job YAML by extracting the "job" mapping from the corresponding
  test YAML file ("{workflow_name}-test.yml" or "{workflow_name}-tests.yml") and writes it as
  "{workflow_name}.yml" alongside the copied files in the output structure.

Notes:
- If there are multiple .ga files in a directory, the script copies all of them.
  The job YAML generation will attempt to pick the test file that best matches a single .ga
  (using name similarity). If ambiguous, it will fall back to the first discovered tests file.
- If no tests YAML is found, the job YAML is not generated for that folder.

Example usage:
    python build_workflow_artifacts.py \
        --workflows-dir workflows \
        --output-dir out_workflows

This script was inspired by and reuses the YAML extraction logic of extract_job_yml.py.
"""
from __future__ import annotations

import argparse
import os
import shutil
import sys
from pathlib import Path
from typing import Optional, Tuple, List

import yaml


TEST_SUFFIXES = ("-test.yml", "-tests.yml")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Replicate workflows tree and collect key artifacts")
    p.add_argument("--workflows-dir", required=True, type=Path, help="Path to the source workflows directory")
    p.add_argument("--output-dir", required=True, type=Path, help="Path to the output directory")
    p.add_argument("--dry-run", action="store_true", help="Show what would be done without writing files")
    p.add_argument("--verbose", "-v", action="count", default=0, help="Increase verbosity (use -vv for more)")
    return p.parse_args()


def log(msg: str, *, level: str = "INFO", v: int = 0, args: argparse.Namespace | None = None) -> None:
    if args is None:
        print(f"[{level}] {msg}")
        return
    # Only print if verbosity threshold is met
    if args.verbose >= v:
        print(f"[{level}] {msg}")


def ensure_dir(path: Path, *, args: argparse.Namespace) -> None:
    if args.dry_run:
        log(f"Would create directory: {path}", v=1, args=args)
        return
    path.mkdir(parents=True, exist_ok=True)


def copy_file(src: Path, dst: Path, *, args: argparse.Namespace) -> None:
    if args.dry_run:
        log(f"Would copy {src} -> {dst}", v=0, args=args)
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    log(f"Copied {src} -> {dst}", v=1, args=args)


def copy_tree(src_dir: Path, dst_dir: Path, *, args: argparse.Namespace) -> None:
    if args.dry_run:
        log(f"Would copy directory {src_dir} -> {dst_dir}", v=0, args=args)
        return
    dst_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(src_dir, dst_dir, dirs_exist_ok=True)
    log(f"Copied directory {src_dir} -> {dst_dir}", v=1, args=args)


def read_tests_job_mapping(tests_yaml_path: Path) -> Optional[dict]:
    """Extract the 'job' mapping from an IWC tests YAML file.

    Supports both list-root and dict-root formats.
    Returns the mapping or None if not found or on YAML errors.
    """
    try:
        with tests_yaml_path.open("r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
    except Exception as e:
        log(f"Failed to read tests YAML {tests_yaml_path}: {e}", level="WARN")
        return None

    job_data = None
    try:
        if isinstance(data, list) and data:
            # Common format: list of docs, each a mapping with 'doc', 'job', 'outputs', ...
            for item in data:
                if isinstance(item, dict) and "job" in item:
                    job_data = item.get("job")
                    if job_data:
                        break
        elif isinstance(data, dict):
            job_data = data.get("job")
    except Exception as e:
        log(f"Error parsing tests YAML structure {tests_yaml_path}: {e}", level="WARN")
        return None

    if not isinstance(job_data, dict):
        return None
    return job_data


def write_job_yaml(job_mapping: dict, out_path: Path, *, args: argparse.Namespace) -> None:
    if args.dry_run:
        log(f"Would write job YAML to {out_path}", v=0, args=args)
        return
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(job_mapping, fh, default_flow_style=False, sort_keys=False)
    log(f"Wrote job YAML: {out_path}", v=1, args=args)


def find_ga_files(dir_path: Path) -> List[Path]:
    return [p for p in dir_path.iterdir() if p.is_file() and p.suffix == ".ga"]


def find_tests_files(dir_path: Path) -> List[Path]:
    files = []
    for p in dir_path.iterdir():
        if p.is_file() and p.suffix.lower() == ".yml":
            lower_name = p.name.lower()
            if any(lower_name.endswith(suf) for suf in TEST_SUFFIXES):
                files.append(p)
    return files


def similarity_score(a: str, b: str) -> int:
    """A simple similarity metric for filenames: higher is better.
    Compares normalized names with hyphen/underscore and case folded.
    """
    def norm(s: str) -> str:
        return s.replace("-", "_").lower()

    a_n, b_n = norm(a), norm(b)
    score = 0
    if a_n == b_n:
        score += 10
    # Shared prefix length bonus
    prefix = os.path.commonprefix([a_n, b_n])
    score += len(prefix)
    return score


def pick_matching_tests(ga: Path, tests_files: List[Path]) -> Optional[Path]:
    if not tests_files:
        return None
    if len(tests_files) == 1:
        return tests_files[0]
    # Pick the tests file with the best name match to the GA stem
    ga_stem = ga.stem
    best = max(tests_files, key=lambda t: similarity_score(ga_stem, t.stem))
    return best


def process_directory(src_dir: Path, dst_dir: Path, *, args: argparse.Namespace) -> None:
    # Always ensure directory exists in output to replicate structure
    ensure_dir(dst_dir, args=args)

    # Copy README.md if present
    readme = src_dir / "README.md"
    if readme.exists():
        copy_file(readme, dst_dir / readme.name, args=args)

    # Copy test data directories recursively if present (support both names)
    for test_dir_name in ("test_data", "test-data"):
        test_data_src = src_dir / test_dir_name
        if test_data_src.exists() and test_data_src.is_dir():
            test_data_dst = dst_dir / test_dir_name
            copy_tree(test_data_src, test_data_dst, args=args)

    ga_files = find_ga_files(src_dir)
    tests_files = find_tests_files(src_dir)

    # Copy all GA files
    for ga in ga_files:
        copy_file(ga, dst_dir / ga.name, args=args)

    # Generate job YAML for each GA if we can find a matching tests file
    for ga in ga_files:
        matched_tests = pick_matching_tests(ga, tests_files)
        if not matched_tests:
            if tests_files:
                # There are tests files but none matched well; pick the first to be helpful
                matched_tests = tests_files[0]
            else:
                log(f"No tests YAML found for {ga}", level="INFO", v=1, args=args)
                continue

        job_mapping = read_tests_job_mapping(matched_tests)
        if not job_mapping:
            log(f"No 'job' mapping found in {matched_tests}", level="INFO", v=1, args=args)
            continue

        out_job_path = dst_dir / f"{ga.stem}.yml"
        write_job_yaml(job_mapping, out_job_path, args=args)


def main() -> int:
    args = parse_args()

    workflows_dir: Path = args.workflows_dir.resolve()
    output_dir: Path = args.output_dir.resolve()

    if not workflows_dir.exists() or not workflows_dir.is_dir():
        log(f"Workflows directory does not exist or is not a directory: {workflows_dir}", level="ERROR")
        return 2

    log(f"Replicating structure from {workflows_dir} -> {output_dir}", v=0, args=args)

    # Walk the tree
    for root, dirs, files in os.walk(workflows_dir):
        src_dir = Path(root)
        rel = src_dir.relative_to(workflows_dir)
        dst_dir = output_dir / rel

        # Process this directory (create, copy README/.ga, generate job yml)
        process_directory(src_dir, dst_dir, args=args)

    log("Done", v=0, args=args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
