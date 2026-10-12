#!/usr/bin/env python3
"""Verify that one MobKit release is publicly readable from every registry."""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from typing import Any, Callable, NamedTuple
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urljoin
from urllib.request import Request, urlopen


CRATE_NAME = "meerkat-mobkit"
EXTENSION_CRATE_NAME = "mobkit-extension-state"
PYPI_NAME = "meerkat-mobkit"
NPM_NAME = "@rkat/mobkit-sdk"


class RegistryVerificationError(RuntimeError):
    """One or more public registries do not expose the requested release."""


class PublishedArtifact(NamedTuple):
    """One registry artifact plus the digest that registry attests."""

    label: str
    url: str
    digest_algorithm: str
    expected_digest: str


def _read_json(url: str, *, timeout: float = 30.0) -> dict[str, Any]:
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "meerkat-mobkit-release-verifier",
        },
    )
    with urlopen(request, timeout=timeout) as response:
        payload = json.load(response)
    if not isinstance(payload, dict):
        raise RegistryVerificationError(f"{url} returned a non-object JSON payload")
    return payload


def _download_artifact(url: str, *, timeout: float = 60.0) -> bytes:
    request = Request(
        url,
        headers={"User-Agent": "meerkat-mobkit-release-verifier"},
    )
    with urlopen(request, timeout=timeout) as response:
        payload = response.read()
    if not payload:
        raise RegistryVerificationError(f"{url} returned an empty artifact")
    return payload


def _required_text(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RegistryVerificationError(f"registry response is missing {field}")
    return value.strip()


def _verify_crates_payload(
    payload: dict[str, Any], version: str, crate_name: str = CRATE_NAME
) -> list[PublishedArtifact]:
    published = payload.get("version")
    if not isinstance(published, dict):
        raise RegistryVerificationError("crates.io response is missing version metadata")
    if published.get("crate") != crate_name or published.get("num") != version:
        raise RegistryVerificationError(
            "crates.io returned unexpected package/version metadata"
        )
    if published.get("yanked") is not False:
        raise RegistryVerificationError("crates.io release is yanked or lacks yank state")
    download_path = _required_text(published.get("dl_path"), field="crate download path")
    checksum = _required_text(published.get("checksum"), field="crate checksum")
    return [
        PublishedArtifact(
            label="crate archive",
            url=urljoin("https://crates.io", download_path),
            digest_algorithm="sha256",
            expected_digest=checksum,
        )
    ]


def _verify_pypi_payload(
    payload: dict[str, Any], version: str
) -> list[PublishedArtifact]:
    info = payload.get("info")
    if not isinstance(info, dict) or info.get("version") != version:
        raise RegistryVerificationError("PyPI returned an unexpected package version")

    files = payload.get("urls")
    if not isinstance(files, list):
        raise RegistryVerificationError("PyPI response is missing release artifacts")
    expected_types = {"bdist_wheel", "sdist"}
    relevant = [
        item
        for item in files
        if isinstance(item, dict) and item.get("packagetype") in expected_types
    ]
    if any(item.get("yanked") is not False for item in relevant):
        raise RegistryVerificationError(
            "PyPI wheel or source distribution is yanked or lacks yank state"
        )
    package_types = {item.get("packagetype") for item in relevant}
    missing = expected_types - package_types
    if missing:
        raise RegistryVerificationError(
            "PyPI release is missing expected artifact types: "
            + ", ".join(sorted(missing))
        )
    artifacts: list[PublishedArtifact] = []
    for item in relevant:
        digests = item.get("digests")
        if not isinstance(digests, dict):
            raise RegistryVerificationError("PyPI artifact is missing digests")
        artifacts.append(
            PublishedArtifact(
                label=_required_text(item.get("filename"), field="PyPI filename"),
                url=_required_text(item.get("url"), field="PyPI artifact URL"),
                digest_algorithm="sha256",
                expected_digest=_required_text(
                    digests.get("sha256"), field="PyPI SHA-256 digest"
                ),
            )
        )
    return artifacts


def _verify_npm_payload(
    payload: dict[str, Any], version: str
) -> list[PublishedArtifact]:
    if payload.get("name") != NPM_NAME or payload.get("version") != version:
        raise RegistryVerificationError("npm returned unexpected package/version metadata")
    dist = payload.get("dist")
    if not isinstance(dist, dict):
        raise RegistryVerificationError("npm response is missing distribution metadata")
    return [
        PublishedArtifact(
            label="npm tarball",
            url=_required_text(dist.get("tarball"), field="npm tarball URL"),
            digest_algorithm="sha1",
            expected_digest=_required_text(dist.get("shasum"), field="npm SHA-1 digest"),
        )
    ]


def _registry_checks(
    version: str,
) -> tuple[
    tuple[
        str,
        str,
        Callable[[dict[str, Any], str], list[PublishedArtifact]],
    ],
    ...,
]:
    encoded_crate = quote(CRATE_NAME, safe="")
    encoded_extension = quote(EXTENSION_CRATE_NAME, safe="")
    encoded_pypi = quote(PYPI_NAME, safe="")
    encoded_npm = quote(NPM_NAME, safe="")
    encoded_version = quote(version, safe="")
    return (
        (
            "crates.io",
            f"https://crates.io/api/v1/crates/{encoded_crate}/{encoded_version}",
            _verify_crates_payload,
        ),
        (
            "crates.io extension-state",
            f"https://crates.io/api/v1/crates/{encoded_extension}/{encoded_version}",
            lambda payload, version: _verify_crates_payload(payload, version, EXTENSION_CRATE_NAME),
        ),
        (
            "PyPI",
            f"https://pypi.org/pypi/{encoded_pypi}/{encoded_version}/json",
            _verify_pypi_payload,
        ),
        (
            "npm",
            f"https://registry.npmjs.org/{encoded_npm}/{encoded_version}",
            _verify_npm_payload,
        ),
    )


def verify_once(
    version: str,
    *,
    reader: Callable[[str], dict[str, Any]] = _read_json,
    downloader: Callable[[str], bytes] = _download_artifact,
) -> None:
    failures: list[str] = []
    for registry, url, validator in _registry_checks(version):
        try:
            artifacts = validator(reader(url), version)
            for artifact in artifacts:
                payload = downloader(artifact.url)
                actual_digest = hashlib.new(
                    artifact.digest_algorithm, payload
                ).hexdigest()
                if actual_digest.lower() != artifact.expected_digest.lower():
                    raise RegistryVerificationError(
                        f"{artifact.label} checksum mismatch: "
                        f"expected {artifact.expected_digest}, got {actual_digest}"
                    )
        except (
            RegistryVerificationError,
            HTTPError,
            URLError,
            TimeoutError,
            OSError,
            ValueError,
        ) as error:
            failures.append(f"{registry}: {error}")
    if failures:
        raise RegistryVerificationError("; ".join(failures))


def verify_with_retries(
    version: str,
    *,
    attempts: int = 30,
    delay_seconds: float = 10.0,
    reader: Callable[[str], dict[str, Any]] = _read_json,
    downloader: Callable[[str], bytes] = _download_artifact,
    sleeper: Callable[[float], None] = time.sleep,
) -> None:
    if attempts < 1:
        raise ValueError("attempts must be at least 1")
    if delay_seconds < 0:
        raise ValueError("delay_seconds must not be negative")

    last_error: RegistryVerificationError | None = None
    for attempt in range(1, attempts + 1):
        try:
            verify_once(version, reader=reader, downloader=downloader)
        except RegistryVerificationError as error:
            last_error = error
            if attempt == attempts:
                break
            print(
                f"Registry verification attempt {attempt}/{attempts} incomplete: {error}"
            )
            sleeper(delay_seconds)
        else:
            print(
                f"Verified exact registry metadata and checksummed artifacts for "
                f"{CRATE_NAME} and {EXTENSION_CRATE_NAME} {version} on crates.io, {PYPI_NAME} {version} "
                f"wheel+sdist on PyPI, and {NPM_NAME} {version} on npm"
            )
            return

    assert last_error is not None
    raise RegistryVerificationError(
        f"release {version} was not fully public after {attempts} attempts: {last_error}"
    ) from last_error


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--attempts", type=int, default=30)
    parser.add_argument("--delay-seconds", type=float, default=10.0)
    args = parser.parse_args()

    version = args.version.strip().removeprefix("v")
    if not version:
        parser.error("--version must not be empty")
    try:
        verify_with_retries(
            version,
            attempts=args.attempts,
            delay_seconds=args.delay_seconds,
        )
    except (RegistryVerificationError, ValueError) as error:
        parser.exit(1, f"registry verification failed: {error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
