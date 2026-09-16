"""Build zip-artifacts for the four Lambdas (L1..L4) with shared code.

Each function archive preserves the repo's package layout so `lambdas.*` and
`producer.*`/`ml.*` imports resolve at runtime:
- lambdas/<function>/   (handler.py + lambda_function.py)
- lambdas/common/       (always)
- producer/ or ml/      (only for the functions that import them)

The AWS function handler is therefore dotted, e.g.
`lambdas.l2_firehose_transform.lambda_function.lambda_handler`.

Archives land in lambdas/_build/ and a SHA256SUMS manifest is written for CI
traceability. Built zips are deterministic (sorted entries, fixed mtimes).
"""

from __future__ import annotations

import hashlib
import shutil
import sys
import zipfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
LAMBDA_ROOT = REPO_ROOT / "lambdas"
BUILD_DIR = LAMBDA_ROOT / "_build"

FIXED_MTIME = 946_684_800  # 2000-01-01T00:00:00Z

FUNCTIONS = (
    "l1_alert_notifier",
    "l2_firehose_transform",
    "l3_patients_api",
    "l4_scheduled_iceberg",
)

# function name -> extra shared packages (from REPO_ROOT) beyond lambdas/common.
EXTRA_PACKAGES: dict[str, tuple[str, ...]] = {
    "l2_firehose_transform": ("producer",),
    "l4_scheduled_iceberg": ("ml",),
}

EXCLUDED_PARTS = {"__pycache__", ".pyc", ".pytest_cache", ".mypy_cache"}


def _select_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        if any(part in EXCLUDED_PARTS for part in path.parts):
            continue
        files.append(path)
    return files


def _package(name: str) -> Path:
    staging = BUILD_DIR / f"staging-{name}"
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True, exist_ok=True)

    def copy_into(src_root: Path, prefix: Path) -> None:
        for file_path in _select_files(src_root):
            rel = prefix.joinpath(file_path.relative_to(src_root))
            dst = staging / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(file_path, dst)

    copy_into(LAMBDA_ROOT / name, Path("lambdas") / name)
    copy_into(LAMBDA_ROOT / "common", Path("lambdas") / "common")
    # lambdas/__init__.py only (avoid recursing into every function dir)
    init_py = LAMBDA_ROOT / "__init__.py"
    dst_init = staging / "lambdas" / "__init__.py"
    dst_init.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(init_py, dst_init)
    for package in EXTRA_PACKAGES.get(name, ()):
        copy_into(REPO_ROOT / package, Path(package))

    out_zip = BUILD_DIR / f"{name}.zip"
    with zipfile.ZipFile(out_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for file_path in sorted(staging.rglob("*")):
            if not file_path.is_file():
                continue
            info = zipfile.ZipInfo.from_file(
                file_path, arcname=file_path.relative_to(staging).as_posix()
            )
            info.date_time = (2000, 1, 1, 0, 0, 0)
            info.compress_type = zipfile.ZIP_DEFLATED
            zf.writestr(info, file_path.read_bytes())
    shutil.rmtree(staging)
    return out_zip


def write_manifest() -> Path:
    manifest = BUILD_DIR / "SHA256SUMS"
    lines: list[str] = []
    for archive in sorted(BUILD_DIR.glob("*.zip")):
        digest = hashlib.sha256(archive.read_bytes()).hexdigest()
        lines.append(f"{digest}  {archive.name}")
    manifest.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return manifest


def main() -> int:
    BUILD_DIR.mkdir(parents=True, exist_ok=True)
    artifacts: list[str] = []
    for name in FUNCTIONS:
        out_zip = _package(name)
        artifacts.append(out_zip.name)
        print(f"built {out_zip.relative_to(REPO_ROOT)} ({out_zip.stat().st_size} bytes)")
    manifest = write_manifest()
    print(f"manifest {manifest.relative_to(REPO_ROOT)}")
    print(f"{len(artifacts)} artifacts: {', '.join(artifacts)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
