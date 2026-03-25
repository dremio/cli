# Contributing to dremio-cli

Thank you for your interest in contributing to dremio-cli! This document explains how to get started.

Please read our [Code of Conduct](CODE_OF_CONDUCT.md) before contributing.

## Development Setup

### Prerequisites

- Python 3.11 or later
- [uv](https://docs.astral.sh/uv/) package manager

### Getting Started

```bash
git clone https://github.com/dremio/cli.git
cd cli
uv sync
uv run dremio --help
```

### Pre-commit Hooks

Install pre-commit hooks to automatically run linting and license header checks on each commit:

```bash
uv run pre-commit install
```

## Running Tests

```bash
uv run pytest tests/ -v
```

## Linting and Formatting

This project uses [ruff](https://docs.astral.sh/ruff/) for linting and formatting.

Check for issues:

```bash
uv run ruff check .
uv run ruff format --check .
```

Auto-fix:

```bash
uv run ruff check --fix .
uv run ruff format .
```

## Pull Request Process

1. Fork the repository and create a feature branch from `main`.
2. Make your changes, ensuring tests pass and linting is clean.
3. Push your branch and open a pull request against `main`.
4. CI will automatically run lint and test checks.
5. A maintainer will review your pull request.

## License Headers

All Python source files must include the Apache 2.0 license header. This is enforced by a pre-commit hook. New files should start with:

```python
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
```

## Reporting Issues

Use [GitHub Issues](https://github.com/dremio/cli/issues) to report bugs or request features. Please include steps to reproduce the issue.

## Releasing (Maintainers)

Releases are automated via GitHub Actions:

1. Ensure `main` is green (CI passes).
2. Create a [GitHub release](https://github.com/dremio/cli/releases/new) with a tag following semver (e.g., `v2.1.0`).
3. The release workflow automatically builds, publishes to PyPI, and updates the Claude Code plugin version.
4. Verify at `https://pypi.org/project/dremio-cli/`.

Version is derived from the git tag via [hatch-vcs](https://github.com/ofek/hatch-vcs) — no manual version bumping is needed.
