# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-03-18

### Added
- **12 command groups** with consistent CRUD verbs: `query`, `folder`, `schema`, `wiki`, `tag`, `reflection`, `job`, `engine`, `user`, `role`, `grant`, `project`
- **Top-level commands**: `search`, `describe`
- **3 output formats**: JSON (default), CSV, pretty table
- **Field filtering** via `--fields` for reduced output
- **Retry with exponential backoff** on timeouts and 429/502/503/504
- **Config resolution**: CLI flags > env vars > config file
- **Command introspection** via `dremio describe <command>` for agent self-discovery
- **Input validation** for SQL injection, path traversal, and malformed IDs
- **120 unit tests** with full HTTP mocking
