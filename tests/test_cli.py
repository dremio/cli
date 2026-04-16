#
# Copyright (C) 2017-2026 Dremio Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Tests for CLI --version and --help flags."""

from __future__ import annotations

from typer.testing import CliRunner

from drs import __version__
from drs.cli import app

runner = CliRunner()


def test_version_flag() -> None:
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert f"dremio-cli {__version__}" in result.output


def test_help_includes_version() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert f"(version {__version__})" in result.output


def test_help_short_flag() -> None:
    result = runner.invoke(app, ["-h"])
    assert result.exit_code == 0
    assert f"(version {__version__})" in result.output
