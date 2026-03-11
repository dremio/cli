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
"""Parse dremio/js-sdk TypeScript source to extract API endpoints.

Generates api_registry.json — a machine-readable map of every Dremio API
endpoint the js-sdk calls, grouped by resource and SKU (oss/enterprise/cloud).

Usage:
    # Clone js-sdk first (or point to existing checkout)
    git clone https://github.com/dremio/js-sdk.git /tmp/js-sdk

    # Generate registry
    python scripts/parse_jssdk.py --sdk-path /tmp/js-sdk

    # Compare against drs client.py coverage
    python scripts/parse_jssdk.py --sdk-path /tmp/js-sdk --compare
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass, field, asdict
from pathlib import Path


# -- URL pattern mapping from getResourceConfig.ts --
# These are the actual URL builders in the js-sdk.
URL_PATTERNS = {
    "sonarV2Request": "/ui/projects/{projectId}/{path}",
    "sonarV3Request": "/v0/projects/{projectId}/{path}",
    "sonarV4Request": "/v1/projects/{projectId}/{path}",
    "v2Request": "/ui/{path}",
    "v3Request": "/v0/{path}",
    "v4Request": "/v1/{path}",
}

# Regex to match request calls — various patterns in js-sdk:
#   .sonarV3Request("path", ...)
#   .sonarV3Request(`path/${id}`, ...)
#   config.sonarV3Request(...)
#   this.#config.v3Request(...)
REQUEST_PATTERN = re.compile(
    r"""\.(sonarV[234]Request|v[234]Request)\(\s*"""
    r"""(?:["'`])([^"'`]*?)["'`]""",
    re.MULTILINE,
)

# Regex to extract HTTP method from RequestInit: method: "POST"
METHOD_PATTERN = re.compile(r"""method:\s*["'](\w+)["']""")


@dataclass
class Endpoint:
    """A single API endpoint extracted from the js-sdk."""
    resource: str          # e.g., "jobs", "catalog", "roles"
    sku: str               # "oss", "enterprise", "cloud"
    file: str              # relative file path
    request_fn: str        # e.g., "sonarV3Request"
    path_template: str     # e.g., "job/${id}"
    http_method: str       # GET, POST, PUT, DELETE
    full_url_pattern: str  # resolved URL pattern
    method_context: str    # surrounding function/method name


@dataclass
class APIRegistry:
    """Complete registry of all endpoints in the js-sdk."""
    source: str = "dremio/js-sdk"
    endpoints: list[Endpoint] = field(default_factory=list)

    @property
    def by_resource(self) -> dict[str, list[Endpoint]]:
        groups: dict[str, list[Endpoint]] = {}
        for ep in self.endpoints:
            groups.setdefault(ep.resource, []).append(ep)
        return groups

    @property
    def by_sku(self) -> dict[str, list[Endpoint]]:
        groups: dict[str, list[Endpoint]] = {}
        for ep in self.endpoints:
            groups.setdefault(ep.sku, []).append(ep)
        return groups


def infer_http_method(surrounding_code: str) -> str:
    """Infer HTTP method from surrounding code context."""
    match = METHOD_PATTERN.search(surrounding_code)
    if match:
        return match.group(1).upper()
    # If body is present but no method, it's likely POST
    if "body:" in surrounding_code or "body =" in surrounding_code:
        return "POST"
    return "GET"


def infer_resource_name(file_path: Path) -> str:
    """Infer resource name from file path.

    src/oss/jobs/JobsResource.ts -> jobs
    src/cloud/catalog/EnterpriseCatalogResource.ts -> catalog
    src/enterprise/roles/RolesResource.ts -> roles
    """
    parts = file_path.parts
    # Find the directory after oss/enterprise/cloud
    for i, part in enumerate(parts):
        if part in ("oss", "enterprise", "cloud") and i + 1 < len(parts):
            next_part = parts[i + 1]
            if next_part.endswith(".ts"):
                # File directly in sku dir
                return next_part.replace(".ts", "").lower()
            return next_part.lower()
    return file_path.stem.lower().replace("resource", "")


def infer_sku(file_path: Path) -> str:
    """Infer SKU from file path."""
    path_str = str(file_path)
    if "/cloud/" in path_str:
        return "cloud"
    if "/enterprise/" in path_str:
        return "enterprise"
    if "/oss/" in path_str:
        return "oss"
    if "/common/" in path_str:
        return "common"
    return "unknown"


def infer_method_context(content: str, match_pos: int) -> str:
    """Find the enclosing function/method name for a match position."""
    # Look backwards for method/function declaration
    before = content[:match_pos]
    # Match class method patterns: methodName( or methodName =
    patterns = [
        re.compile(r'(\w+)\s*\([^)]*\)\s*\{[^}]*$', re.DOTALL),
        re.compile(r'(\w+)\s*=\s*(?:\([^)]*\)\s*=>|function)', re.DOTALL),
        re.compile(r'(?:async\s+)?(\w+)\s*\(', re.DOTALL),
    ]
    # Take last 500 chars to find enclosing method
    snippet = before[-500:]
    best_match = None
    best_pos = -1
    for pattern in patterns:
        for m in pattern.finditer(snippet):
            if m.start() > best_pos:
                best_pos = m.start()
                best_match = m.group(1)
    return best_match or "unknown"


def normalize_path_template(path: str) -> str:
    """Normalize TypeScript template literals to consistent format.

    `job/${id}` -> job/{id}
    `catalog/by-path/${path.map(...).join("/")}` -> catalog/by-path/{path}
    """
    # Replace ${...} with {param}, extracting first identifier
    result = re.sub(r'\$\{([^}]+)\}', lambda m: '{' + re.match(r'(\w+)', m.group(1)).group(1) + '}', path)
    # Clean up any remaining template syntax
    result = result.replace('`', '').replace("'", '').replace('"', '')
    # Strip query parameters (e.g., ?maxChildren=0)
    result = result.split('?')[0]
    return result


def resolve_full_url(request_fn: str, path: str) -> str:
    """Resolve a request function + path to a full URL pattern."""
    pattern = URL_PATTERNS.get(request_fn, "/{path}")
    return pattern.replace("{path}", path)


def parse_file(file_path: Path, sdk_root: Path) -> list[Endpoint]:
    """Parse a single TypeScript file for API endpoint calls."""
    content = file_path.read_text(encoding="utf-8")
    relative = file_path.relative_to(sdk_root)
    resource = infer_resource_name(relative)
    sku = infer_sku(relative)
    endpoints = []

    for match in REQUEST_PATTERN.finditer(content):
        request_fn = match.group(1)
        raw_path = match.group(2)

        # Get surrounding context (200 chars after match) for method inference
        start = max(0, match.start() - 200)
        end = min(len(content), match.end() + 300)
        surrounding = content[start:end]

        http_method = infer_http_method(surrounding)
        normalized_path = normalize_path_template(raw_path)
        full_url = resolve_full_url(request_fn, normalized_path)
        method_ctx = infer_method_context(content, match.start())

        endpoints.append(Endpoint(
            resource=resource,
            sku=sku,
            file=str(relative),
            request_fn=request_fn,
            path_template=normalized_path,
            http_method=http_method,
            full_url_pattern=full_url,
            method_context=method_ctx,
        ))

    return endpoints


def parse_sdk(sdk_path: Path) -> APIRegistry:
    """Parse the entire js-sdk source tree."""
    src = sdk_path / "src"
    if not src.exists():
        print(f"Error: {src} not found. Is --sdk-path pointing to the js-sdk root?", file=sys.stderr)
        sys.exit(1)

    registry = APIRegistry()
    ts_files = sorted(src.rglob("*.ts"))
    for f in ts_files:
        # Skip test files, type declarations
        if "test" in f.name.lower() or f.name.endswith(".d.ts"):
            continue
        endpoints = parse_file(f, sdk_path)
        registry.endpoints.extend(endpoints)

    return registry


# -- drs client.py comparison --

DRS_ENDPOINTS = {
    "POST /v0/projects/{pid}/sql": "submit_sql",
    "GET /v0/projects/{pid}/job/{id}": "get_job_status",
    "GET /v0/projects/{pid}/job/{id}/results": "get_job_results",
    "POST /v0/projects/{pid}/job/{id}/cancel": "cancel_job",
    "GET /v0/projects/{pid}/catalog": "get_catalog_entity (root)",
    "GET /v0/projects/{pid}/catalog/{id}": "get_catalog_entity",
    "GET /v0/projects/{pid}/catalog/by-path/{path}": "get_catalog_by_path",
    "POST /v0/projects/{pid}/search": "search",
    "GET /v0/projects/{pid}/catalog/{id}/graph": "get_lineage",
    "GET /v0/projects/{pid}/catalog/{id}/collaboration/wiki": "get_wiki",
    "GET /v0/projects/{pid}/catalog/{id}/collaboration/tag": "get_tags",
    "GET /v0/projects/{pid}/reflection/{id}": "get_reflection",
    "POST /v0/projects/{pid}/reflection/{id}/refresh": "refresh_reflection",
    "DELETE /v0/projects/{pid}/reflection/{id}": "delete_reflection",
    "GET /v0/projects/{pid}/user/{id}": "list_users / get_user_by_name",
    "GET /v0/projects/{pid}/user/by-name/{name}": "get_user_by_name",
    "GET /v0/role": "list_roles",
    "GET /v0/role/{id}": "list_roles (detail)",
}


def compare_coverage(registry: APIRegistry) -> dict:
    """Compare js-sdk endpoints against drs client.py coverage."""
    # Deduplicate js-sdk endpoints by (method, url_pattern)
    sdk_endpoints: dict[str, list[Endpoint]] = {}
    for ep in registry.endpoints:
        key = f"{ep.http_method} {ep.full_url_pattern}"
        sdk_endpoints.setdefault(key, []).append(ep)

    covered = []
    uncovered = []
    drs_only = []

    # Normalize drs endpoints for comparison
    drs_normalized = {}
    for drs_key, method_name in DRS_ENDPOINTS.items():
        # Normalize {pid} -> {projectId}
        normalized = drs_key.replace("{pid}", "{projectId}")
        drs_normalized[normalized] = method_name

    for sdk_key, eps in sorted(sdk_endpoints.items()):
        # Try to match against drs
        # Normalize sdk key for comparison
        matched = False
        for drs_key, drs_method in drs_normalized.items():
            if _endpoints_match(sdk_key, drs_key):
                covered.append({
                    "sdk_endpoint": sdk_key,
                    "drs_method": drs_method,
                    "resource": eps[0].resource,
                    "sku": eps[0].sku,
                })
                matched = True
                break
        if not matched:
            uncovered.append({
                "endpoint": sdk_key,
                "resource": eps[0].resource,
                "sku": eps[0].sku,
                "file": eps[0].file,
                "method": eps[0].method_context,
            })

    # Find drs endpoints not in sdk
    sdk_keys_normalized = set()
    for sdk_key in sdk_endpoints:
        sdk_keys_normalized.add(sdk_key)

    for drs_key, drs_method in drs_normalized.items():
        matched = any(_endpoints_match(sk, drs_key) for sk in sdk_keys_normalized)
        if not matched:
            drs_only.append({"endpoint": drs_key, "drs_method": drs_method})

    return {
        "summary": {
            "sdk_total": len(sdk_endpoints),
            "drs_covered": len(covered),
            "sdk_uncovered_by_drs": len(uncovered),
            "drs_only": len(drs_only),
        },
        "covered": covered,
        "uncovered": uncovered,
        "drs_only": drs_only,
    }


def _endpoints_match(a: str, b: str) -> bool:
    """Fuzzy-match two endpoint patterns, ignoring parameter names."""
    def normalize(s: str) -> str:
        # Replace any {param} with {_}
        return re.sub(r'\{[^}]+\}', '{_}', s)
    return normalize(a) == normalize(b)


def main():
    parser = argparse.ArgumentParser(description="Parse dremio/js-sdk to extract API endpoints")
    parser.add_argument("--sdk-path", type=Path, required=True, help="Path to js-sdk checkout")
    parser.add_argument("--compare", action="store_true", help="Compare against drs client.py")
    parser.add_argument("--output", type=Path, default=None, help="Output file (default: stdout)")
    args = parser.parse_args()

    registry = parse_sdk(args.sdk_path)

    if args.compare:
        result = compare_coverage(registry)
        output = json.dumps(result, indent=2)
    else:
        # Build registry JSON
        # Deduplicate by (method, url) and group by resource
        seen: set[tuple[str, str]] = set()
        deduped: list[dict] = []
        for ep in registry.endpoints:
            key = (ep.http_method, ep.full_url_pattern)
            if key not in seen:
                seen.add(key)
                deduped.append(asdict(ep))

        by_resource: dict[str, list[dict]] = {}
        for ep in deduped:
            by_resource.setdefault(ep["resource"], []).append(ep)

        output_data = {
            "source": "dremio/js-sdk",
            "generated_by": "scripts/parse_jssdk.py",
            "url_patterns": URL_PATTERNS,
            "total_endpoints": len(deduped),
            "resources": {
                name: {
                    "endpoint_count": len(eps),
                    "skus": sorted(set(e["sku"] for e in eps)),
                    "endpoints": [
                        {
                            "method": e["http_method"],
                            "url": e["full_url_pattern"],
                            "path": e["path_template"],
                            "request_fn": e["request_fn"],
                            "sdk_method": e["method_context"],
                            "file": e["file"],
                        }
                        for e in eps
                    ],
                }
                for name, eps in sorted(by_resource.items())
            },
        }
        output = json.dumps(output_data, indent=2)

    # Print summary to stderr
    print(f"\njs-sdk API Registry", file=sys.stderr)
    print(f"{'=' * 50}", file=sys.stderr)
    print(f"Total endpoints: {len(registry.endpoints)}", file=sys.stderr)
    by_sku = registry.by_sku
    for sku in sorted(by_sku):
        print(f"  {sku}: {len(by_sku[sku])} calls", file=sys.stderr)
    by_res = registry.by_resource
    print(f"\nResources ({len(by_res)}):", file=sys.stderr)
    for name in sorted(by_res):
        eps = by_res[name]
        methods = set(f"{e.http_method} {e.path_template}" for e in eps)
        print(f"  {name}: {len(methods)} unique endpoints", file=sys.stderr)

    if args.compare:
        result = json.loads(output)
        s = result["summary"]
        print(f"\nCoverage Comparison", file=sys.stderr)
        print(f"  js-sdk endpoints:     {s['sdk_total']}", file=sys.stderr)
        print(f"  Covered by drs:       {s['drs_covered']}", file=sys.stderr)
        print(f"  Not in drs:           {s['sdk_uncovered_by_drs']}", file=sys.stderr)
        print(f"  drs-only (SQL-based): {s['drs_only']}", file=sys.stderr)

    if args.output:
        args.output.write_text(output)
        print(f"\nWrote {args.output}", file=sys.stderr)
    else:
        print(output)


if __name__ == "__main__":
    main()
