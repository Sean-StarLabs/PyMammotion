#!/usr/bin/env python3
"""Build the focused HA fork wheel from a pinned upstream release wheel."""

from __future__ import annotations

import argparse
import ast
import hashlib
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import zipfile

BASE_VERSION = "0.8.14"
FORK_VERSION = "0.8.14.post1"
CREDENTIAL_NAMES = {
    "APP_KEY",
    "APP_SECRET",
    "MAMMOTION_OAUTH2_CLIENT_ID",
    "MAMMOTION_OAUTH2_CLIENT_SECRET",
}
CONST_PATH = "pymammotion/const.py"
START_SENTINEL = "# --- credentials: injected at build time via scripts/update_credentials.py — do not edit ---"
END_SENTINEL = "# --- end credentials ---"


class BuildError(ValueError):
    """The official input or source tree does not match the expected contract."""


def _top_level_target(node: ast.stmt) -> str | None:
    if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
        return node.targets[0].id
    if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
        return node.target.id
    return None


def _credential_nodes(source: str, *, injected: bool) -> tuple[list[ast.stmt], int, int]:
    tree = ast.parse(source)
    selected: list[ast.stmt] = []
    assignments: dict[str, ast.stmt] = {}
    helpers: list[ast.stmt] = []
    for node in tree.body:
        target = _top_level_target(node)
        if target in CREDENTIAL_NAMES:
            if target in assignments:
                raise BuildError(f"duplicate credential assignment: {target}")
            assignments[target] = node
            selected.append(node)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "_r":
            helpers.append(node)
            selected.append(node)
    if set(assignments) != CREDENTIAL_NAMES:
        raise BuildError("credential assignment block is incomplete")
    if injected:
        if len(helpers) != 1 or any(not isinstance(n, ast.Assign) for n in assignments.values()):
            raise BuildError("official wheel credential block has unexpected syntax")
        for node in assignments.values():
            value = node.value  # type: ignore[attr-defined]
            if not (
                isinstance(value, ast.Call)
                and isinstance(value.func, ast.Name)
                and value.func.id == "_r"
                and len(value.args) == 1
                and isinstance(value.args[0], ast.Tuple)
                and all(isinstance(item, ast.Constant) and type(item.value) is int for item in value.args[0].elts)
            ):
                raise BuildError("official wheel credential assignment is not an encoded release value")
    else:
        if helpers or any(not isinstance(n, ast.AnnAssign) for n in assignments.values()):
            raise BuildError("source credential sentinel block has unexpected syntax")
        if any(
            not isinstance(n.value, ast.Constant) or n.value.value != ""  # type: ignore[attr-defined]
            for n in assignments.values()
        ):
            raise BuildError("source credential sentinel block is not blank")
    start = min(n.lineno for n in selected)
    end = max(n.end_lineno or n.lineno for n in selected)
    return selected, start, end


def _sentinel_region(source: str) -> tuple[list[str], int, int]:
    lines = source.splitlines(keepends=True)
    starts = [i for i, line in enumerate(lines) if line.rstrip("\r\n") == START_SENTINEL]
    ends = [i for i, line in enumerate(lines) if line.rstrip("\r\n") == END_SENTINEL]
    if len(starts) != 1 or len(ends) != 1 or starts[0] >= ends[0]:
        raise BuildError("credential sentinel markers are missing or ambiguous")
    return lines, starts[0], ends[0]


def inject_release_block(source_const: str, wheel_const: str) -> str:
    """Return source const.py with only the official release credential block added."""
    source_nodes, _, _ = _credential_nodes(source_const, injected=False)
    wheel_nodes, _, _ = _credential_nodes(wheel_const, injected=True)
    source_lines, source_marker_start, source_marker_end = _sentinel_region(source_const)
    wheel_lines, wheel_marker_start, wheel_marker_end = _sentinel_region(wheel_const)
    if any(
        not source_marker_start < n.lineno - 1 <= (n.end_lineno or n.lineno) - 1 < source_marker_end
        for n in source_nodes
    ):
        raise BuildError("source credential block is outside its sentinels")
    if any(
        not wheel_marker_start < n.lineno - 1 <= (n.end_lineno or n.lineno) - 1 < wheel_marker_end for n in wheel_nodes
    ):
        raise BuildError("official credential block is outside its sentinels")
    if (
        source_lines[: source_marker_start + 1] != wheel_lines[: wheel_marker_start + 1]
        or source_lines[source_marker_end:] != wheel_lines[wheel_marker_end:]
    ):
        raise BuildError("noncredential const.py content differs from the official release")
    return "".join(
        source_lines[: source_marker_start + 1]
        + wheel_lines[wheel_marker_start + 1 : wheel_marker_end]
        + source_lines[source_marker_end:]
    )


def _wheel_inputs(path: Path, expected_sha256: str) -> tuple[bytes, str]:
    data = path.read_bytes()
    actual = hashlib.sha256(data).hexdigest()
    if actual.lower() != expected_sha256.lower():
        raise BuildError("official wheel SHA-256 does not match the required value")
    if path.name != f"pymammotion-{BASE_VERSION}-py3-none-any.whl":
        raise BuildError("official wheel filename does not match the base release")
    try:
        with zipfile.ZipFile(path) as archive:
            metadata_files = [n for n in archive.namelist() if n.endswith(".dist-info/METADATA")]
            expected_metadata = f"pymammotion-{BASE_VERSION}.dist-info/METADATA"
            if metadata_files != [expected_metadata] or CONST_PATH not in archive.namelist():
                raise BuildError("input is not a single pymammotion wheel")
            metadata = archive.read(metadata_files[0]).decode("utf-8")
            fields = dict(
                line.split(": ", 1) for line in metadata.splitlines() if line.startswith(("Name: ", "Version: "))
            )
            if fields != {"Name": "pymammotion", "Version": BASE_VERSION}:
                raise BuildError("official wheel name or base version does not match")
            const_source = archive.read(CONST_PATH).decode("utf-8")
            _credential_nodes(const_source, injected=True)
            return data, const_source
    except (OSError, zipfile.BadZipFile, UnicodeDecodeError) as err:
        raise BuildError("cannot read official wheel") from err


def _archive_head(repo: Path, destination: Path) -> int:
    result = subprocess.run(
        ["git", "-C", str(repo), "archive", "--format=tar", "HEAD"],
        check=True,
        stdout=subprocess.PIPE,
    )
    import tarfile

    with tarfile.open(fileobj=__import__("io").BytesIO(result.stdout), mode="r:") as archive:
        archive.extractall(destination, filter="data")
    epoch = subprocess.run(
        ["git", "-C", str(repo), "show", "-s", "--format=%ct", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    )
    return int(epoch.stdout.strip())


def build(repo: Path, official_wheel: Path, expected_sha256: str, output: Path) -> None:
    _, release_const = _wheel_inputs(official_wheel, expected_sha256)
    output = output.resolve()
    if not output.parent.exists():
        output.parent.mkdir(mode=0o700, parents=True)
    elif output.parent.stat().st_mode & 0o077:
        raise BuildError("output directory must be private (mode 0700)")
    with tempfile.TemporaryDirectory(prefix="pymammotion-fork-") as scratch:
        tree = Path(scratch) / "src"
        tree.mkdir()
        epoch = _archive_head(repo.resolve(), tree)
        pyproject = tree / "pyproject.toml"
        pyproject_text = pyproject.read_text(encoding="utf-8")
        marker = re.compile(rf'(?m)^version = "{re.escape(BASE_VERSION)}"$')
        if len(marker.findall(pyproject_text)) != 1:
            raise BuildError("source pyproject version is not the expected base version")
        pyproject.write_text(marker.sub(f'version = "{FORK_VERSION}"', pyproject_text), encoding="utf-8")
        const_path = tree / CONST_PATH
        const_path.write_text(
            inject_release_block(const_path.read_text(encoding="utf-8"), release_const), encoding="utf-8"
        )
        build_dir = Path(scratch) / "dist"
        build_dir.mkdir(mode=0o700)
        env = os.environ.copy()
        env["SOURCE_DATE_EPOCH"] = str(epoch)
        subprocess.run(
            [os.sys.executable, "-m", "hatchling", "build", "-t", "wheel", "-d", str(build_dir)],
            cwd=tree,
            env=env,
            check=True,
        )
        wheels = list(build_dir.glob("pymammotion-*.whl"))
        if len(wheels) != 1 or wheels[0].name != f"pymammotion-{FORK_VERSION}-py3-none-any.whl":
            raise BuildError("build backend produced an unexpected wheel")
        shutil.copyfile(wheels[0], output)
        output.chmod(0o600)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--official-wheel", type=Path, required=True)
    parser.add_argument("--sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    build(args.repo, args.official_wheel, args.sha256, args.output)


if __name__ == "__main__":
    main()
