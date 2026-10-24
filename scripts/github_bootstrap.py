#!/usr/bin/env python
"""Идемпотентная настройка GitHub-репозитория из .env (PLAN.md T7.1).

Только stdlib (без python-dotenv). Читает .env сам, вызывает `gh` с `GH_TOKEN`
в окружении процесса (токен не печатается). Делает, пропуская уже сделанное:

1. создаёт репозиторий (если нет), добавляет remote origin, пушит `main` и `impl/v1`;
2. описание, topics (asyncio, pipeline, dataflow, seda), отключает wiki/projects;
3. environment `pypi` с обязательным ревьюером = владелец (ручное подтверждение публикации);
4. защита `main`: обязательный статус `check`, запрет force-push.

Запуск: `uv run poe github` (или `python scripts/github_bootstrap.py`).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

TOPICS = ("asyncio", "pipeline", "dataflow", "seda")
ROOT = Path(__file__).resolve().parent.parent


def read_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def gh(
    args: list[str], env: dict[str, str], *, check: bool = True
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["gh", *args],
        env=env,
        check=check,
        capture_output=True,
        text=True,
    )


def git(args: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=ROOT,
        check=check,
        capture_output=True,
        text=True,
    )


def repo_exists(slug: str, env: dict[str, str]) -> bool:
    return gh(["repo", "view", slug], env, check=False).returncode == 0


def ensure_repo(slug: str, visibility: str, env: dict[str, str]) -> None:
    if repo_exists(slug, env):
        print(f"репозиторий {slug} уже существует")
        return
    print(f"создаю репозиторий {slug} ({visibility})")
    gh(["repo", "create", slug, f"--{visibility}"], env)


def ensure_remote_and_push(slug: str) -> None:
    url = f"https://github.com/{slug}.git"
    if git(["remote", "get-url", "origin"], check=False).returncode != 0:
        git(["remote", "add", "origin", url])
    for branch in ("main", "impl/v1"):
        if git(["rev-parse", "--verify", branch], check=False).returncode == 0:
            print(f"push {branch}")
            git(["push", "-u", "origin", branch])


def configure_repo(slug: str, env: dict[str, str]) -> None:
    args = [
        "repo",
        "edit",
        slug,
        "--description",
        "Декларативный конвейер стадий поверх asyncio",
        "--enable-wiki=false",
        "--enable-projects=false",
    ]
    for topic in TOPICS:
        args += ["--add-topic", topic]
    gh(args, env)


def gh_api_put(path: str, body: dict[str, object], env: dict[str, str]) -> None:
    """Вызвать `gh api -X PUT <path>` с телом JSON из stdin."""
    subprocess.run(
        ["gh", "api", "-X", "PUT", path, "--input", "-"],
        env=env,
        input=json.dumps(body),
        check=True,
        capture_output=True,
        text=True,
    )


def ensure_pypi_environment(owner: str, slug: str, env: dict[str, str]) -> None:
    owner_id = gh(["api", f"users/{owner}", "--jq", ".id"], env).stdout.strip()
    body: dict[str, object] = {"reviewers": [{"type": "User", "id": int(owner_id)}]}
    gh_api_put(f"repos/{slug}/environments/pypi", body, env)
    print("environment pypi настроен (обязательный ревьюер — владелец)")


def protect_main(slug: str, env: dict[str, str]) -> None:
    body: dict[str, object] = {
        "required_status_checks": {"strict": True, "contexts": ["check"]},
        "enforce_admins": True,
        "required_pull_request_reviews": None,
        "restrictions": None,
        "allow_force_pushes": False,
        "allow_deletions": False,
    }
    gh_api_put(f"repos/{slug}/branches/main/protection", body, env)
    print("защита main настроена (обязательный статус check, запрет force-push)")


def main() -> int:
    cfg = read_env(ROOT / ".env")
    token = cfg.get("GH_TOKEN") or os.environ.get("GH_TOKEN", "")
    owner = cfg.get("GITHUB_OWNER", "")
    repo = cfg.get("GITHUB_REPO", "")
    visibility = cfg.get("GITHUB_VISIBILITY", "public")
    if not (token and owner and repo):
        print("не заданы GH_TOKEN/GITHUB_OWNER/GITHUB_REPO в .env", file=sys.stderr)
        return 2

    slug = f"{owner}/{repo}"
    env = {**os.environ, "GH_TOKEN": token}

    ensure_repo(slug, visibility, env)
    ensure_remote_and_push(slug)
    configure_repo(slug, env)
    ensure_pypi_environment(owner, slug, env)
    protect_main(slug, env)
    print(f"готово: {slug}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
