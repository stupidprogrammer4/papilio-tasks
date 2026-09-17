import shutil
import tarfile
import zipfile
from pathlib import Path

import pytest

build = pytest.importorskip(
    "hatchling.build", reason="Requires the project's declared build backend"
)


def wheel_contents(path):
    with zipfile.ZipFile(path) as archive:
        return {name: archive.read(name) for name in archive.namelist()}


def test_distribution_excludes_local_copies_and_roundtrips(
    tmp_path, monkeypatch
):
    root = Path(__file__).resolve().parents[1]
    project = tmp_path / "project"
    project.mkdir()
    shutil.copytree(
        root / "papilio_tasks",
        project / "papilio_tasks",
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
    for name in (
        "pyproject.toml",
        "README.md",
        "CHANGELOG.md",
        "LICENSE",
        ".gitignore",
    ):
        shutil.copy2(root / name, project / name)
    # A nested installed copy must not match the package's root selection.
    nested = project / ".local-review" / "env" / "site-packages"
    (nested / "papilio_tasks").mkdir(parents=True)
    (nested / "papilio_tasks" / "old.py").write_text("OLD_PACKAGE = True\n")
    for name in ("README.md", "LICENSE", "pyproject.toml"):
        (nested / name).write_text("local build fixture\n")
    for name in ("AGENTS.md", "WORK.md"):
        (project / name).write_text("local-only record\n")
    info = project / ".git" / "info"
    info.mkdir(parents=True)
    (info / "exclude").write_text(".local-review/\nAGENTS.md\nWORK.md\n")

    output = tmp_path / "dist"
    output.mkdir()
    monkeypatch.chdir(project)
    direct = wheel_contents(output / build.build_wheel(str(output)))
    sdist = output / build.build_sdist(str(output))

    expected = {
        path.relative_to(project).as_posix(): path.read_bytes()
        for path in (project / "papilio_tasks").rglob("*")
        if path.is_file()
    }
    assert "papilio_tasks/py.typed" in expected
    assert {
        name: data
        for name, data in direct.items()
        if name.startswith("papilio_tasks/")
    } == expected
    assert all(
        name.startswith("papilio_tasks/")
        or name.split("/", 1)[0].endswith(".dist-info")
        for name in direct
    )

    unpacked = tmp_path / "unpacked"
    with tarfile.open(sdist) as archive:
        files = {
            member.name.split("/", 1)[1]: archive.extractfile(member).read()
            for member in archive.getmembers()
            if member.isfile()
        }
        assert set(files) == set(expected) | {
            "README.md",
            "CHANGELOG.md",
            "LICENSE",
            "pyproject.toml",
            "PKG-INFO",
            ".gitignore",
        }
        assert {name: files[name] for name in expected} == expected
        archive.extractall(unpacked, filter="data")

    monkeypatch.chdir(next(unpacked.iterdir()))
    rebuilt_dir = tmp_path / "rebuilt"
    rebuilt_dir.mkdir()
    rebuilt = wheel_contents(rebuilt_dir / build.build_wheel(str(rebuilt_dir)))
    assert rebuilt == direct
