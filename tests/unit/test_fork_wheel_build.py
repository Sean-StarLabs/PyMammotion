from __future__ import annotations

import hashlib
import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import zipfile

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "build_fork_wheel.py"
SPEC = importlib.util.spec_from_file_location("build_fork_wheel", SCRIPT)
assert SPEC and SPEC.loader
builder = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(builder)


SOURCE_CONST = '''"""test constants"""
import os
# --- credentials: injected at build time via scripts/update_credentials.py — do not edit ---
APP_KEY: str = ""
APP_SECRET: str = ""
MAMMOTION_OAUTH2_CLIENT_ID: str = ""
MAMMOTION_OAUTH2_CLIENT_SECRET: str = ""
# --- end credentials ---
APP_VERSION = "unchanged"
MAMMOTION_DOMAIN = "unchanged"
'''
WHEEL_CONST = '''"""test constants"""
import os
# --- credentials: injected at build time via scripts/update_credentials.py — do not edit ---
def _r(d: tuple[int, ...]) -> str:
    return bytes(d).decode()
APP_KEY = _r((1, 2))
APP_SECRET = _r((3, 4))
MAMMOTION_OAUTH2_CLIENT_ID = _r((5, 6))
MAMMOTION_OAUTH2_CLIENT_SECRET = _r((7, 8))
# --- end credentials ---
APP_VERSION = "unchanged"
MAMMOTION_DOMAIN = "unchanged"
'''


def make_wheel(path: Path, *, version: str = "0.8.14", const: str = WHEEL_CONST) -> str:
    with zipfile.ZipFile(path, "w") as wheel:
        wheel.writestr("pymammotion/const.py", const)
        wheel.writestr(
            "pymammotion-0.8.14.dist-info/METADATA",
            f"Metadata-Version: 2.4\nName: pymammotion\nVersion: {version}\n",
        )
        wheel.writestr("pymammotion-0.8.14.dist-info/WHEEL", "Wheel-Version: 1.0\n")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_wrong_wheel_hash_is_rejected(tmp_path: Path) -> None:
    wheel = tmp_path / "pymammotion-0.8.14-py3-none-any.whl"
    make_wheel(wheel)
    with pytest.raises(builder.BuildError, match="SHA-256"):
        builder._wheel_inputs(wheel, "0" * 64)


def test_missing_blank_sentinel_is_rejected() -> None:
    with pytest.raises(builder.BuildError, match="sentinel block"):
        builder.inject_release_block(SOURCE_CONST.replace('APP_KEY: str = ""', 'APP_KEY: str = "present"'), WHEEL_CONST)


def test_wheel_version_mismatch_is_rejected(tmp_path: Path) -> None:
    wheel = tmp_path / "pymammotion-0.8.14-py3-none-any.whl"
    digest = make_wheel(wheel, version="0.8.13")
    with pytest.raises(builder.BuildError, match="version"):
        builder._wheel_inputs(wheel, digest)


def test_wheel_filename_mismatch_is_rejected(tmp_path: Path) -> None:
    wheel = tmp_path / "renamed.whl"
    digest = make_wheel(wheel)
    with pytest.raises(builder.BuildError, match="filename"):
        builder._wheel_inputs(wheel, digest)


def test_noncredential_drift_is_rejected() -> None:
    with pytest.raises(builder.BuildError, match="noncredential"):
        builder.inject_release_block(SOURCE_CONST, WHEEL_CONST.replace('"unchanged"', '"drift"', 1))


def test_injection_preserves_source_and_noncredential_content() -> None:
    before = SOURCE_CONST
    patched = builder.inject_release_block(SOURCE_CONST, WHEEL_CONST)
    assert SOURCE_CONST == before
    patched_lines, patched_start, patched_end = builder._sentinel_region(patched)
    source_lines, source_start, source_end = builder._sentinel_region(SOURCE_CONST)
    assert patched_lines[: patched_start + 1] == source_lines[: source_start + 1]
    assert patched_lines[patched_end:] == source_lines[source_end:]


def test_build_leaves_source_untouched_and_loads_wheel_without_env(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    package = repo / "pymammotion"
    package.mkdir(parents=True)
    (repo / "pyproject.toml").write_text(
        '[build-system]\nrequires = ["hatchling"]\nbuild-backend = "hatchling.build"\n'
        '[project]\nname = "pymammotion"\nversion = "0.8.14"\nrequires-python = ">=3.13"\n'
        '[tool.hatch.build.targets.wheel]\npackages = ["pymammotion"]\n',
        encoding="utf-8",
    )
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "const.py").write_text(SOURCE_CONST, encoding="utf-8")
    before = {p.relative_to(repo): p.read_bytes() for p in repo.rglob("*") if p.is_file() and ".git" not in p.parts}
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    subprocess.run(["git", "-C", str(repo), "add", "pyproject.toml", "pymammotion"], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(repo),
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "-qm",
            "fixture",
        ],
        check=True,
    )
    official = tmp_path / "pymammotion-0.8.14-py3-none-any.whl"
    digest = make_wheel(official)
    output = tmp_path / "private" / "pymammotion-0.8.14.post1-py3-none-any.whl"
    builder.build(repo, official, digest, output)
    assert before == {
        p.relative_to(repo): p.read_bytes() for p in repo.rglob("*") if p.is_file() and ".git" not in p.parts
    }
    assert output.stat().st_mode & 0o777 == 0o600
    with zipfile.ZipFile(output) as wheel:
        assert "Version: 0.8.14.post1\n" in wheel.read("pymammotion-0.8.14.post1.dist-info/METADATA").decode()
        const_file = tmp_path / "built-const.py"
        const_file.write_bytes(wheel.read("pymammotion/const.py"))
    code = (
        "import runpy; c=runpy.run_path(" + repr(str(const_file)) + "); "
        "n=('APP_KEY','APP_SECRET','MAMMOTION_OAUTH2_CLIENT_ID','MAMMOTION_OAUTH2_CLIENT_SECRET'); "
        "assert all(isinstance(c[k], str) and c[k] for k in n)"
    )
    env = os.environ.copy()
    for name in builder.CREDENTIAL_NAMES:
        env.pop(name, None)
    subprocess.run([sys.executable, "-c", code], check=True, env=env, capture_output=True, text=True)
