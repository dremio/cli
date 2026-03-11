#!/usr/bin/env python3
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
"""Validate drs client.py coverage against Dremio OpenAPI specs.

Parses the OpenAPI YAML specs from the Dremio monorepo and compares them
against the methods implemented in client.py. Reports:
  - Endpoints covered by drs
  - Endpoints available in specs but not covered
  - drs methods that don't map to any spec (potential drift)

Usage:
    python scripts/validate_api_coverage.py [--dremio-repo /path/to/dremio]

Requires: pyyaml
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml


# -- drs client coverage: method -> (HTTP_METHOD, url_pattern) --
# This is the source of truth for what drs implements.
# Kept here (not parsed from client.py) so it's explicit and reviewable.

DRS_COVERAGE = {
    # SQL / Jobs (v0 project-scoped)
    "submit_sql":        ("POST", "/v0/projects/{pid}/sql"),
    "get_job_status":    ("GET",  "/v0/projects/{pid}/job/{jobId}"),
    "get_job_results":   ("GET",  "/v0/projects/{pid}/job/{jobId}/results"),
    "cancel_job":        ("POST", "/v0/projects/{pid}/job/{jobId}/cancel"),

    # Catalog (v0 project-scoped)
    "get_catalog_entity":  ("GET",  "/v0/projects/{pid}/catalog/{id}"),
    "get_catalog_by_path": ("GET",  "/v0/projects/{pid}/catalog/by-path/{path}"),
    "search":              ("POST", "/v0/projects/{pid}/search"),
    "get_lineage":         ("GET",  "/v0/projects/{pid}/catalog/{id}/graph"),
    "get_wiki":            ("GET",  "/v0/projects/{pid}/catalog/{id}/collaboration/wiki"),
    "get_tags":            ("GET",  "/v0/projects/{pid}/catalog/{id}/collaboration/tag"),

    # Reflections (v0 project-scoped)
    "get_reflection":     ("GET",    "/v0/projects/{pid}/reflection/{id}"),
    "refresh_reflection": ("POST",   "/v0/projects/{pid}/reflection/{id}/refresh"),
    "delete_reflection":  ("DELETE", "/v0/projects/{pid}/reflection/{id}"),

    # Users & Roles (v1)
    "list_users":        ("GET", "/v1/users"),
    "get_user_by_name":  ("GET", "/v1/users/name/{userName}"),
    "list_roles":        ("GET", "/v1/roles"),
    "get_grants":        ("GET", "/v1/{scope}/{scopeId}/grants/{type}/{id}"),
}


# -- Spec files to scan, grouped by API version --

SPEC_GROUPS = {
    "cloud_v0": {
        "base": "/v0",
        "files": [
            "openapi/cloud/src/main/openapi/dc_job_API.yaml",
            "openapi/cloud/src/main/openapi/sources_apis.yaml",
            "openapi/cloud/src/main/openapi/projectsAPI.yaml",
            "openapi/cloud/src/main/openapi/engines.yaml",
        ],
    },
    "cloud_v1": {
        "base": "/v1",
        "files": [
            "openapi/cloud/src/main/openapi/users.yaml",
            "openapi/cloud/src/main/openapi/roles.yaml",
            "openapi/cloud/src/main/openapi/rbac_rest_apis.yaml",
        ],
    },
    "enterprise_v3": {
        "base": "/api/v3",
        "files": [
            "openapi/enterprise/src/main/openapi/catalog.yaml",
            "openapi/enterprise/src/main/openapi/search.yaml",
            "openapi/enterprise/src/main/openapi/reflection.yaml",
            "openapi/enterprise/src/main/openapi/user.yaml",
            "openapi/enterprise/src/main/openapi/functions.yaml",
            "openapi/enterprise/src/main/openapi/scripts.yaml",
        ],
    },
}


def load_spec_endpoints(dremio_repo: Path) -> dict[str, list[tuple[str, str]]]:
    """Parse OpenAPI specs and extract all endpoint paths.

    Returns: {group_name: [(HTTP_METHOD, full_path), ...]}
    """
    all_endpoints: dict[str, list[tuple[str, str]]] = {}

    for group_name, group_info in SPEC_GROUPS.items():
        base = group_info["base"]
        endpoints: list[tuple[str, str]] = []

        for spec_file in group_info["files"]:
            spec_path = dremio_repo / spec_file
            if not spec_path.exists():
                print(f"  WARN: Spec not found: {spec_path}", file=sys.stderr)
                continue

            try:
                with open(spec_path) as f:
                    spec = yaml.safe_load(f)
            except Exception as e:
                print(f"  WARN: Failed to parse {spec_path}: {e}", file=sys.stderr)
                continue

            if not spec or "paths" not in spec:
                continue

            for path, methods in spec["paths"].items():
                full_path = f"{base}{path}" if not path.startswith(base) else path
                for method in methods:
                    if method.upper() in {"GET", "POST", "PUT", "DELETE", "PATCH"}:
                        endpoints.append((method.upper(), full_path))

        all_endpoints[group_name] = endpoints

    return all_endpoints


def normalize_path(path: str) -> str:
    """Normalize path for comparison (collapse path params to {param})."""
    import re
    return re.sub(r"\{[^}]+\}", "{*}", path)


def compare(drs_coverage: dict, spec_endpoints: dict) -> tuple[list, list, list]:
    """Compare drs coverage against spec endpoints.

    Returns: (covered, uncovered_in_spec, unmapped_in_drs)
    """
    # Normalize drs paths
    drs_normalized = {}
    for method_name, (http_method, path) in drs_coverage.items():
        key = (http_method, normalize_path(path))
        drs_normalized[key] = method_name

    # Normalize spec paths
    spec_normalized: dict[tuple[str, str], list[str]] = {}
    for group, endpoints in spec_endpoints.items():
        for http_method, path in endpoints:
            key = (http_method, normalize_path(path))
            spec_normalized.setdefault(key, []).append(f"{group}: {http_method} {path}")

    # Compare
    covered = []
    for key, method_name in drs_normalized.items():
        if key in spec_normalized:
            covered.append((method_name, key, spec_normalized[key]))

    uncovered = []
    for key, sources in spec_normalized.items():
        if key not in drs_normalized:
            uncovered.append((key[0], key[1], sources))

    unmapped = []
    for key, method_name in drs_normalized.items():
        if key not in spec_normalized:
            unmapped.append((method_name, key[0], key[1]))

    return covered, uncovered, unmapped


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate drs API coverage against Dremio OpenAPI specs")
    parser.add_argument(
        "--dremio-repo",
        type=Path,
        default=Path.home() / "dremio-repo" / "dremio",
        help="Path to the dremio monorepo (default: ~/dremio-repo/dremio)",
    )
    args = parser.parse_args()

    if not args.dremio_repo.exists():
        print(f"Error: Dremio repo not found at {args.dremio_repo}", file=sys.stderr)
        sys.exit(1)

    print(f"Scanning OpenAPI specs in {args.dremio_repo}/openapi/...")
    spec_endpoints = load_spec_endpoints(args.dremio_repo)

    total_spec = sum(len(eps) for eps in spec_endpoints.values())
    print(f"Found {total_spec} endpoints across {len(SPEC_GROUPS)} spec groups\n")

    covered, uncovered, unmapped = compare(DRS_COVERAGE, spec_endpoints)

    # Report: Covered
    print(f"=== COVERED by drs ({len(covered)}/{total_spec} spec endpoints) ===")
    for method_name, key, sources in sorted(covered, key=lambda x: x[1]):
        print(f"  {method_name:25s} -> {key[0]:6s} {key[1]}")

    # Report: Not covered
    print(f"\n=== NOT COVERED by drs ({len(uncovered)} spec endpoints available) ===")
    for http_method, path, sources in sorted(uncovered, key=lambda x: x[1]):
        print(f"  {http_method:6s} {path}")
        for src in sources:
            print(f"         from: {src}")

    # Report: drs methods not in spec
    if unmapped:
        print(f"\n=== DRS METHODS NOT IN SPEC ({len(unmapped)}) ===")
        for method_name, http_method, path in sorted(unmapped, key=lambda x: x[2]):
            print(f"  {method_name:25s} -> {http_method:6s} {path}")
        print("  (These may use undocumented endpoints or SQL-based implementations)")

    # Summary
    print(f"\n=== SUMMARY ===")
    print(f"  Spec endpoints found:     {total_spec}")
    print(f"  Covered by drs:           {len(covered)}")
    print(f"  Available but not covered: {len(uncovered)}")
    print(f"  drs methods not in spec:  {len(unmapped)}")

    coverage_pct = (len(covered) / total_spec * 100) if total_spec > 0 else 0
    print(f"  Coverage: {coverage_pct:.0f}%")


if __name__ == "__main__":
    main()
