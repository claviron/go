#!/usr/bin/env python3
"""Prepare one portable native library into prebuilds. Nothing here publishes."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib

ROOT = Path(__file__).resolve().parents[1]
# Classifier: Rust triple, library, target CPU, ELF machine, accepted ELF OS ABIs.
TARGETS = {
    "linux-x86_64": ("x86_64-unknown-linux-gnu", "liborbit_go.so", "x86-64", 62, (0, 3)),
    "linux-aarch64": ("aarch64-unknown-linux-gnu", "liborbit_go.so", "generic", 183, (0, 3)),
    "macos-aarch64": ("aarch64-apple-darwin", "liborbit_go.dylib", "generic", None, ()),
    "freebsd-x86_64": ("x86_64-unknown-freebsd", "liborbit_go.so", "x86-64", 62, (9,)),
    "freebsd-aarch64": ("aarch64-unknown-freebsd", "liborbit_go.so", "generic", 183, (9,)),
}
TARGET_ALIASES = {
    "linux-x64-gnu": "linux-x86_64",
    "linux-arm64-gnu": "linux-aarch64",
    "darwin-arm64": "macos-aarch64",
    "freebsd-x64": "freebsd-x86_64",
    "freebsd-arm64": "freebsd-aarch64",
}


def run(command, cwd=ROOT, env=None):
    subprocess.run(list(map(str, command)), cwd=cwd, env=env, check=True)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def file_sha(path):
    return sha(Path(path).read_bytes())


def atomic(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_bytes(data)
    temporary.replace(path)


def write_json(path, value):
    atomic(path, (json.dumps(value, indent=2, sort_keys=True) + "\n").encode())


def selected_target(value=None):
    value = TARGET_ALIASES.get(value, value)
    if value is None:
        os_name = {"Linux": "linux", "Darwin": "macos", "FreeBSD": "freebsd"}.get(platform.system())
        cpu = {"x86_64": "x86_64", "amd64": "x86_64", "aarch64": "aarch64", "arm64": "aarch64"}.get(
            platform.machine().lower())
        value = f"{os_name}-{cpu}"
    if value in TARGETS:
        return value
    for classifier, spec in TARGETS.items():
        if value == spec[0]:
            return classifier
    raise ValueError(f"Unsupported release target: {value}")


def source_files(root):
    listed = subprocess.check_output(["git", "ls-files", "-z"], cwd=root)
    return sorted(root / name.decode() for name in listed.split(b"\0") if name)


def context(root):
    """What a library is built from: the version and every tracked file."""
    version = tomllib.loads((root / "native/Cargo.toml").read_text())["package"]["version"]
    digest = hashlib.sha256()
    for path in source_files(root):
        data = path.read_bytes()
        digest.update(str(path.relative_to(root)).encode() + b"\0" + str(len(data)).encode() + b"\0" + data)
    return {"version": version, "source": digest.hexdigest()}


def copy_sources(root, destination):
    for path in source_files(root):
        target = destination / path.relative_to(root)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, target)


def ensure_lock(root=ROOT):
    """Resolve once, outside the checkout so a parent's Cargo patches cannot reach it."""
    lock = root / "prebuilds/Cargo.lock"
    if not lock.is_file():
        with tempfile.TemporaryDirectory(prefix="orbit-go-resolve-") as temporary:
            source = Path(temporary).resolve()
            copy_sources(root, source)
            run(["cargo", "generate-lockfile"], cwd=source)
            atomic(lock, (source / "Cargo.lock").read_bytes())
    return lock


def validate_binary(classifier, data):
    _, _, _, machine, osabis = TARGETS[classifier]
    if classifier == "macos-aarch64":
        valid = (len(data) >= 32 and int.from_bytes(data[:4], "little") == 0xfeedfacf
                 and int.from_bytes(data[4:8], "little") == 0x100000c
                 and int.from_bytes(data[12:16], "little") == 6)
    else:
        valid = (len(data) >= 64 and data[:6] == b"\x7fELF\x02\x01" and data[7] in osabis
                 and int.from_bytes(data[16:18], "little") == 3
                 and int.from_bytes(data[18:20], "little") == machine)
    if not valid:
        raise ValueError(f"Native artifact is not a shared library for {classifier}")


def stage_native(root, classifier, data, receipt):
    validate_binary(classifier, data)
    folder = root / "prebuilds" / classifier
    atomic(folder / TARGETS[classifier][1], data)
    write_json(folder / "release.json", receipt)


def read_native(root, classifier, expected=None, directory=None):
    expected = expected or context(root)
    directory = directory or root / "prebuilds" / classifier
    triple, library, cpu, _, _ = TARGETS[classifier]
    receipt = json.loads((directory / "release.json").read_text())
    data = (directory / library).read_bytes()
    validate_binary(classifier, data)
    lock = root / "prebuilds/Cargo.lock"
    required = {**expected, "format": 1, "classifier": classifier, "target": triple,
                "profile": "release", "cpu": cpu, "sha256": sha(data), "lock": file_sha(lock)}
    if any(receipt.get(key) != value for key, value in required.items()):
        raise ValueError(f"{classifier}: native receipt differs from current sources, dependency snapshot or binary")
    return data, receipt


def validate_local_dependencies(metadata, source):
    source = source.resolve()
    for package in metadata["packages"]:
        if package["source"] is None and not Path(package["manifest_path"]).resolve().is_relative_to(source):
            name = package.get("name", "unknown package")
            raise ValueError(f"Release dependency resolves to an external local checkout: {name}")


def prepare(root=ROOT, target=None):
    classifier = selected_target(target)
    triple, library, cpu, _, _ = TARGETS[classifier]
    expected = context(root)
    lock = ensure_lock(root)
    lock_hash = file_sha(lock)
    # Stage outside the checkout so parent .cargo/config.toml cannot inject local patches.
    with tempfile.TemporaryDirectory(prefix="orbit-go-native-") as temporary:
        source = Path(temporary).resolve()
        copy_sources(root, source)
        shutil.copyfile(lock, source / "Cargo.lock")
        env = os.environ.copy()
        output = Path(env.get("CARGO_TARGET_DIR", root / "target/prebuilds")).resolve()
        env["CARGO_TARGET_DIR"] = str(output)
        # A stable remap avoids invalidating every dependency for each random staging directory.
        cargo_home = Path(env.get("CARGO_HOME", Path.home() / ".cargo")).resolve()
        env["CARGO_ENCODED_RUSTFLAGS"] = (
            f"-C\x1ftarget-cpu={cpu}"
            f"\x1f--remap-path-prefix={Path.home()}=/build-home"
            f"\x1f--remap-path-prefix={cargo_home}=/cargo"
            f"\x1f--remap-path-prefix={Path(tempfile.gettempdir()).resolve()}=/build")
        metadata = subprocess.check_output(["cargo", "metadata", "--locked", "--all-features", "--format-version", "1"],
                                           cwd=source, env=env, text=True)
        validate_local_dependencies(json.loads(metadata), source)
        run(["cargo", "build", "--package", "orbit-go-native", "--locked", "--release", "--target", triple],
            cwd=source, env=env)
        data = (output / triple / "release" / library).read_bytes()
    if expected != context(root) or lock_hash != file_sha(lock):
        raise ValueError("Release inputs changed during native preparation")
    receipt = {**expected, "format": 1, "classifier": classifier, "target": triple,
               "profile": "release", "cpu": cpu, "sha256": sha(data), "lock": lock_hash}
    stage_native(root, classifier, data, receipt)
    print(f"Prepared prebuilds/{classifier}/{library}; no publication performed.")
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepare", action="store_true", required=True,
                        help="build one portable native library into prebuilds")
    parser.add_argument("--target", help="classifier or Rust triple; defaults to this host")
    prepare(ROOT, parser.parse_args().target)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, OSError, KeyError, subprocess.CalledProcessError) as error:
        print(f"Release preparation failed: {error}", file=sys.stderr)
        sys.exit(1)
