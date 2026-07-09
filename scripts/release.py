#!/usr/bin/env python3
"""Prepare a release from Conventional Commits."""

from __future__ import annotations

import argparse
import datetime as dt
import os
import re
import subprocess
import sys
from pathlib import Path

PACKAGE_NAME = "langchain-mcp-adapters-reversal-websockets"
RELEASE_FILES = [
    Path("pyproject.toml"),
    Path("uv.lock"),
    Path("CHANGELOG.md"),
]
RELEASE_RELEVANT_PATHS = [
    ".github/actions/",
    ".github/workflows/",
    "langchain_mcp_adapters/",
    "tests/",
    "Makefile",
    "README.md",
    "pyproject.toml",
    "uv.lock",
]
COMMIT_RE = re.compile(r"^(\w+)(?:\(([^)]+)\))?(!)?: (.+)$")


def git(args: list[str]) -> str:
    return subprocess.check_output(
        ["git", *args], text=True, stderr=subprocess.PIPE
    ).strip()


def run(command: str, args: list[str], *, dry_run: bool) -> None:
    if dry_run:
        print(f"[dry-run] {command} {' '.join(args)}")
        return
    subprocess.check_call([command, *args])


def get_current_version() -> str:
    content = Path("pyproject.toml").read_text()
    match = re.search(r'(?m)^version = "([^"]+)"$', content)
    if not match:
        raise RuntimeError("Could not find project.version in pyproject.toml")
    return match.group(1)


def get_last_release_tag() -> str:
    try:
        return git(
            [
                "describe",
                "--tags",
                "--match",
                "v[0-9]*",
                "--match",
                f"{PACKAGE_NAME}==*",
                "--abbrev=0",
            ],
        )
    except subprocess.CalledProcessError:
        return ""


def is_release_relevant(path: str) -> bool:
    normalized = path.replace("\\", "/")
    return any(
        normalized.startswith(candidate)
        if candidate.endswith("/")
        else normalized == candidate
        for candidate in RELEASE_RELEVANT_PATHS
    )


def changed_files(commit_hash: str) -> list[str]:
    output = git(
        ["diff-tree", "--root", "--no-commit-id", "--name-only", "-r", commit_hash]
    )
    return [line.strip() for line in output.splitlines() if line.strip()]


def release_relevant_messages(last_tag: str) -> list[str]:
    commit_range = f"{last_tag}..HEAD" if last_tag else "HEAD"
    hashes = [
        line.strip() for line in git(["log", commit_range, "--format=%H"]).splitlines()
    ]
    messages: list[str] = []
    for commit_hash in filter(None, hashes):
        message = git(["log", "-1", "--format=%B", commit_hash])
        subject = message.splitlines()[0] if message.splitlines() else ""
        if subject.startswith("chore: release v"):
            continue
        if any(is_release_relevant(path) for path in changed_files(commit_hash)):
            messages.append(message.strip())
    return messages


def parse_conventional_commit(message: str) -> dict[str, str | bool] | None:
    lines = message.splitlines()
    if not lines:
        return None
    match = COMMIT_RE.match(lines[0])
    if not match:
        return None
    body = "\n".join(lines[1:])
    return {
        "type": match.group(1),
        "scope": match.group(2) or "",
        "breaking": bool(match.group(3)) or "BREAKING CHANGE:" in body,
        "message": match.group(4),
    }


def determine_bump(commits: list[dict[str, str | bool]]) -> str:
    bump = "none"
    for commit in commits:
        if commit["breaking"]:
            return "major"
        if commit["type"] == "feat":
            bump = "minor"
        if commit["type"] in {"fix", "perf"} and bump == "none":
            bump = "patch"
    return bump


def parse_semver(version: str) -> tuple[int, int, int]:
    match = re.match(r"^(\d+)\.(\d+)\.(\d+)$", version)
    if not match:
        raise RuntimeError(f"Unsupported version format: {version}")
    return tuple(int(part) for part in match.groups())


def increment_version(version: str, bump: str) -> str:
    major, minor, patch = parse_semver(version)
    if bump == "major":
        return f"{major + 1}.0.0"
    if bump == "minor":
        return f"{major}.{minor + 1}.0"
    if bump == "patch":
        return f"{major}.{minor}.{patch + 1}"
    return version


def assert_release_files_clean() -> None:
    dirty = git(["status", "--porcelain", "--", *map(str, RELEASE_FILES)])
    if dirty and os.environ.get("ALLOW_DIRTY_RELEASE") != "1":
        raise RuntimeError(
            "Release files have uncommitted changes:\n"
            f"{dirty}\n"
            "Commit or stash them first, or set ALLOW_DIRTY_RELEASE=1.",
        )


def update_pyproject(version: str) -> None:
    path = Path("pyproject.toml")
    content = path.read_text()
    updated = re.sub(
        r'(?m)^version = "[^"]+"$', f'version = "{version}"', content, count=1
    )
    path.write_text(updated)


def update_uv_lock(version: str) -> None:
    path = Path("uv.lock")
    content = path.read_text()
    updated = re.sub(
        rf'(\[\[package\]\]\nname = "{re.escape(PACKAGE_NAME)}"\nversion = )"[^"]+"',
        rf'\1"{version}"',
        content,
        count=1,
    )
    if updated == content:
        raise RuntimeError("Could not update package version in uv.lock")
    path.write_text(updated)


def capitalize(value: str) -> str:
    return value[:1].upper() + value[1:] if value else value


def changelog_groups(
    commits: list[dict[str, str | bool]],
) -> list[tuple[str, list[str]]]:
    titles = {
        "feat": "Features",
        "fix": "Bug Fixes",
        "perf": "Performance",
        "refactor": "Refactoring",
        "docs": "Documentation",
        "ci": "CI/CD",
        "test": "Testing",
        "style": "Styling",
        "chore": "Miscellaneous",
    }
    groups: dict[str, list[str]] = {key: [] for key in titles}
    for commit in commits:
        commit_type = str(commit["type"])
        if commit_type not in groups:
            continue
        scope = f"**{commit['scope']}**: " if commit["scope"] else ""
        breaking = " **BREAKING**" if commit["breaking"] else ""
        groups[commit_type].append(
            f"- {scope}{capitalize(str(commit['message']))}{breaking}"
        )
    return [(titles[key], groups[key]) for key in titles if groups[key]]


def update_changelog(version: str, commits: list[dict[str, str | bool]]) -> None:
    path = Path("CHANGELOG.md")
    today = dt.date.today().isoformat()
    lines = [f"## [{version}] - {today}", ""]
    for title, items in changelog_groups(commits):
        lines.extend([f"### {title}", *items, ""])
    section = "\n".join(lines).rstrip() + "\n"

    if not path.exists():
        path.write_text(
            "# Changelog\n\n"
            "All notable changes to this project will be documented in this file.\n\n"
            f"{section}",
        )
        return

    content = path.read_text()
    if re.search(rf"(?m)^## \[{re.escape(version)}\]", content):
        return
    first_release = re.search(r"(?m)^## \[", content)
    if not first_release:
        path.write_text(f"{content.rstrip()}\n\n{section}")
        return
    insert_at = first_release.start()
    path.write_text(
        f"{content[:insert_at].rstrip()}\n\n{section}\n{content[insert_at:]}"
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--ci", action="store_true")
    parser.add_argument("--push", action="store_true")
    args = parser.parse_args()

    if args.ci:
        run("git", ["config", "user.name", "github-actions[bot]"], dry_run=args.dry_run)
        run(
            "git",
            [
                "config",
                "user.email",
                "41898282+github-actions[bot]@users.noreply.github.com",
            ],
            dry_run=args.dry_run,
        )

    current_version = get_current_version()
    last_tag = get_last_release_tag()
    commits = [
        parsed
        for message in release_relevant_messages(last_tag)
        if (parsed := parse_conventional_commit(message))
    ]
    bump = determine_bump(commits)
    if bump == "none":
        print("No release-worthy Conventional Commits found.")
        return 0

    next_version = increment_version(current_version, bump)
    next_tag = f"v{next_version}"
    try:
        git(["rev-parse", "-q", "--verify", f"refs/tags/{next_tag}"])
    except subprocess.CalledProcessError:
        pass
    else:
        raise RuntimeError(f"Tag {next_tag} already exists.")

    print(f"{current_version} -> {next_version} ({bump})")
    if args.dry_run:
        return 0

    assert_release_files_clean()
    update_pyproject(next_version)
    update_uv_lock(next_version)
    update_changelog(next_version, commits)

    run("git", ["add", *map(str, RELEASE_FILES)], dry_run=False)
    run("git", ["commit", "-m", f"chore: release {next_tag}"], dry_run=False)
    run("git", ["tag", "-a", next_tag, "-m", f"Release {next_tag}"], dry_run=False)
    if args.push:
        branch = git(["branch", "--show-current"])
        run("git", ["push", "origin", f"HEAD:{branch}", "--follow-tags"], dry_run=False)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"release failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
