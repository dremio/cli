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
"""Regression coverage for the eMudhra trust-store incident (DX-128380)."""

from __future__ import annotations

import hashlib
import re
import tomllib
from importlib.metadata import version
from pathlib import Path

import certifi
from cryptography import x509
from cryptography.hazmat.primitives import serialization

COMPROMISED_SPKI_SHA256 = {
    "bd153ed7b0434f6886b17bce8bbe84ed340c7132d702a8f4fa318f756ecbd6f3",
    "c619e4780ed8e0abeda24ad732567de4383a7ea941338f6d1a7d1b49ac792fe9",
    "8d417db2dd8bf5e3084d1e3f196d583849d81bdd4c00c70b9d39369e96b8c782",
}
MINIMUM_CERTIFI_VERSION = (2026, 10, 19)
REPO_ROOT = Path(__file__).resolve().parents[1]


def test_certifi_dependency_and_lock_require_fixed_version() -> None:
    project = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())
    assert "certifi>=2026.10.19" in project["project"]["dependencies"]

    lock = tomllib.loads((REPO_ROOT / "uv.lock").read_text())
    certifi_package = next(package for package in lock["package"] if package["name"] == "certifi")
    assert tuple(map(int, certifi_package["version"].split("."))) >= MINIMUM_CERTIFI_VERSION
    assert tuple(map(int, version("certifi").split("."))) >= MINIMUM_CERTIFI_VERSION


def test_certifi_bundle_excludes_all_compromised_public_keys() -> None:
    """Check SPKI hashes, not certificate hashes, to cover cross-signed roots too."""
    bundle = Path(certifi.where()).read_bytes()
    certificates = re.findall(rb"-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----", bundle, re.DOTALL)
    assert certificates, "certifi CA bundle contains no certificates"
    assert bundle.count(b"-----BEGIN CERTIFICATE-----") == len(certificates)
    assert bundle.count(b"-----END CERTIFICATE-----") == len(certificates)

    found = set()
    for pem in certificates:
        certificate = x509.load_pem_x509_certificate(pem)
        public_key = certificate.public_key().public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        fingerprint = hashlib.sha256(public_key).hexdigest()
        if fingerprint in COMPROMISED_SPKI_SHA256:
            found.add(fingerprint)

    assert not found, f"certifi CA bundle contains compromised SPKI fingerprints: {sorted(found)}"
