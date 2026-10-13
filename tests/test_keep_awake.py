"""SPEC-0010: защита от сна Mac, пока работает сбор."""

from __future__ import annotations

import os
import subprocess
import sys
import time
from dataclasses import replace
from typing import Any

import pytest

from cluster_source.config import AppConfig


class FakePopen:
    calls: list[list[str]] = []

    def __init__(self, args: list[str]) -> None:
        FakePopen.calls.append(args)
        self.pid = 4242


@pytest.fixture(autouse=True)
def _reset() -> None:
    FakePopen.calls = []


@pytest.fixture
def main() -> Any:
    """``main`` импортирует SDK — импорт внутри теста, как в остальных тестах."""
    from cluster_source import main as module

    return module


def test_caffeinate_bound_to_own_pid(
    share_config: AppConfig, monkeypatch: pytest.MonkeyPatch, main: Any
) -> None:
    """SPEC-0010 A-1 (G-1): на macOS — caffeinate -i -w <PID процесса>."""
    monkeypatch.setattr(main.sys, "platform", "darwin")
    monkeypatch.setattr(main.shutil, "which", lambda _: "/usr/bin/caffeinate")
    monkeypatch.setattr(main.subprocess, "Popen", FakePopen)
    assert main.prevent_idle_sleep(share_config) is not None
    assert FakePopen.calls == [["/usr/bin/caffeinate", "-i", "-w", str(os.getpid())]]


@pytest.mark.parametrize(
    ("platform", "keep_awake", "binary"),
    [
        ("linux", True, "/usr/bin/caffeinate"),
        ("darwin", False, "/usr/bin/caffeinate"),
        ("darwin", True, None),
    ],  # fmt: skip
)
def test_no_caffeinate_when_not_applicable(
    share_config: AppConfig,
    monkeypatch: pytest.MonkeyPatch,
    platform: str,
    keep_awake: bool,
    binary: str | None,
    main: Any,
) -> None:
    """SPEC-0010 A-2: другая ОС, keep_awake: false или нет caffeinate — ничего не запускается."""
    monkeypatch.setattr(main.sys, "platform", platform)
    monkeypatch.setattr(main.shutil, "which", lambda _: binary)
    monkeypatch.setattr(main.subprocess, "Popen", FakePopen)
    config = replace(share_config, keep_awake=keep_awake)
    assert main.prevent_idle_sleep(config) is None
    assert FakePopen.calls == []


@pytest.mark.skipif(sys.platform != "darwin", reason="только macOS")
def test_real_caffeinate_exits_with_parent() -> None:
    """SPEC-0010 A-3: запрет держится, пока жив процесс, и снимается после его выхода."""
    parent = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(1.5)"])
    child = subprocess.Popen(["caffeinate", "-i", "-w", str(parent.pid)])
    try:
        time.sleep(0.3)
        assert child.poll() is None  # родитель жив — запрет держится
        parent.wait(timeout=5)
        assert child.wait(timeout=5) == 0  # родитель вышел — caffeinate тоже
    finally:
        for proc in (parent, child):
            if proc.poll() is None:
                proc.kill()


def test_keep_awake_parsed_from_yaml(tmp_path: Any) -> None:
    path = tmp_path / "config.yaml"
    path.write_text("TOKEN: t\ndata_dir: d\nkeep_awake: false\n", encoding="utf-8")
    assert AppConfig.from_yaml(path).keep_awake is False
    path.write_text("TOKEN: t\ndata_dir: d\n", encoding="utf-8")
    assert AppConfig.from_yaml(path).keep_awake is True
