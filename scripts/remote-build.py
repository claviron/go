#!/usr/bin/env python3
"""Build native artifacts in isolated remote clones; collect verified prebuilds only."""
import argparse
import base64
import concurrent.futures
import importlib.util
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import zlib

ROOT = Path(__file__).resolve().parents[1]


def release_module(root):
    spec = importlib.util.spec_from_file_location("claviron_go_release", root / "scripts/release.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def command(arguments, **kwargs):
    subprocess.run(list(map(str, arguments)), check=True, **kwargs)


def hosts(config, selected, release):
    values = config.get("hosts")
    if not isinstance(values, dict) or not values:
        raise ValueError("Build configuration requires a hosts object")
    if set(selected) - set(values):
        raise ValueError("Unknown selected host")
    result = []
    for name, value in values.items():
        if selected and name not in selected:
            continue
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", name):
            raise ValueError("Invalid host label")
        if not isinstance(value, dict):
            raise ValueError("Each build host must be an object")
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.@:\[\]-]*", value.get("ssh", "")):
            raise ValueError("Invalid SSH destination")
        for key in ("repository", "cache"):
            if key in value and (not isinstance(value[key], str) or not value[key].startswith("/") or any(c in value[key] for c in "\n\r\0")):
                raise ValueError(f"{key} must be an absolute remote path")
        if "repository" not in value:
            raise ValueError("Remote repository is required")
        if not re.fullmatch(r"[A-Za-z0-9_./-]+", value.get("python", "python3")) or value.get("python", "python3").startswith("-"):
            raise ValueError("python must name an executable, without shell arguments")
        if "jobs" in value and (type(value["jobs"]) is not int or value["jobs"] < 1):
            raise ValueError("jobs must be a positive integer")
        if "nice" in value and (type(value["nice"]) is not int or not -20 <= value["nice"] <= 19):
            raise ValueError("nice must be between -20 and 19")
        result.append({**value, "name": name, "target": release.selected_target(value["target"])})
    if len({value["target"] for value in result}) != len(result):
        raise ValueError("Select only one builder per target")
    return result


def transfer(root, target, source, release, expected):
    library = release.TARGETS[target][1]
    (root / "prebuilds").mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".collect-", dir=root / "prebuilds") as temporary:
        temporary = Path(temporary)
        command(["rsync", "-rt", "--protect-args", "-e", "ssh -o ConnectionAttempts=3", "--include=/" + library,
                 "--include=/release.json", "--exclude=*", "--", source.rstrip("/") + "/", str(temporary) + "/"])
        data, receipt = release.read_native(root, target, expected, temporary)
        if release.context(root) != expected:
            raise ValueError("Release sources changed during collection")
        release.stage_native(root, target, data, receipt)


def build_all(root, configuration, selected=(), dry_run=False):
    release = release_module(root)
    builders = hosts(configuration, selected, release)
    expected = release.context(root)
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    if not re.fullmatch(r"[a-f0-9]{40}", revision):
        raise ValueError("Cannot determine committed source revision")
    for host in builders:
        print(f"{host['name']}: {host['ssh']} -> {host['target']}, revision {revision[:8]}", flush=True)
    if dry_run:
        return []
    command(["git", "cat-file", "-e", revision + ":scripts/remote-build.py"], cwd=root)
    lock = release.ensure_lock(root)
    compressed = base64.b64encode(zlib.compress(lock.read_bytes())).decode()
    parent = root / "target/build-all"
    parent.mkdir(parents=True, exist_ok=True)
    record = Path(tempfile.mkdtemp(prefix="run-", dir=parent))
    def one(host):
        request = {**host, "revision": revision, "context": expected, "dependencyLock": compressed,
                   "lock": release.file_sha(lock)}
        encoded = base64.b64encode(json.dumps(request).encode()).decode()
        git = "git -C " + shlex.quote(host["repository"])
        bootstrap = ("set -eu; set -o pipefail; " + git + " cat-file -e " + shlex.quote(revision + "^{commit}")
                     + " 2>/dev/null || " + git + " fetch --no-tags origin " + shlex.quote(revision) + "; "
                     + git + " show " + shlex.quote(revision + ":scripts/remote-build.py") + " | "
                     + shlex.quote(host.get("python", "python3")) + " - --launch " + shlex.quote(encoded))
        logfile = record / (host["name"] + ".log")
        try:
            with logfile.open("w") as log:
                with subprocess.Popen(["ssh", "-T", "-o", "BatchMode=yes", "-o", "ConnectionAttempts=3", "--", host["ssh"],
                    "bash -l -c " + shlex.quote(bootstrap)], cwd=root, stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True) as child:
                    result = None
                    for line in child.stdout:
                        log.write(line)
                        log.flush()
                        if line.startswith("ORBIT_GO_BUILD_STARTED "):
                            started = json.loads(line.split(" ", 1)[1])
                            print(f"{host['name']}: building; remote log {started['job']}/build.log", flush=True)
                        if line.startswith("ORBIT_GO_BUILD_RESULT "):
                            result = json.loads(line.split(" ", 1)[1])
                    if child.wait() or not result or not result.get("success"):
                        raise ValueError((result or {}).get("error", "Remote build failed; inspect the build log"))
                directory = result.get("directory", "")
                if result.get("revision") != revision or result.get("context") != expected or result.get("target") != host["target"]:
                    raise ValueError("Remote result identity does not match the request")
                if not directory.startswith("/") or any(c in directory for c in "\n\r\0"):
                    raise ValueError("Invalid remote artifact path")
                transfer(root, host["target"], host["ssh"] + ":" + directory, release, expected)
            return {"host": host["name"], "target": host["target"], "success": True}
        except (OSError, ValueError, subprocess.CalledProcessError) as error:
            print(f"{host['name']}: failed; inspect {logfile}", file=sys.stderr)
            return {"host": host["name"], "target": host["target"], "success": False, "error": str(error)}
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(builders)) as pool:
        results = list(pool.map(one, builders))
    release.write_json(record / "results.json", {"revision": revision, "context": expected, "results": results})
    print(f"Results: {record / 'results.json'}")
    return results


def worker(job):
    request = json.loads((job / "request.json").read_text())
    source = job / "source"
    release = release_module(source)
    lock = Path(request["cache"]) / ("prepare-" + request["target"] + ".lock")
    owned = False
    try:
        lock.mkdir()
        owned = True
        release.write_json(lock / "owner.json", {"pid": os.getpid(), "job": str(job)})
        if release.context(source) != request["context"] or release.file_sha(source / "prebuilds/Cargo.lock") != request["lock"]:
            raise ValueError("Committed source or dependency snapshot differs from release request")
        env = os.environ.copy()
        env["CARGO_TARGET_DIR"] = str(Path(request["cache"]) / "target")
        if "jobs" in request:
            env["CARGO_BUILD_JOBS"] = str(request["jobs"])
        arguments = [sys.executable, "scripts/release.py", "--prepare", "--target", request["target"]]
        if "nice" in request:
            arguments = ["nice", "-n", str(request["nice"]), *arguments]
        command(arguments, cwd=source, env=env)
        release.read_native(source, request["target"], request["context"])
        result = {"success": True, "context": request["context"], "revision": request["revision"],
                  "target": request["target"], "directory": str(source / "prebuilds" / request["target"])}
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        result = {"success": False, "error": str(error), "target": request["target"]}
    finally:
        if owned:
            shutil.rmtree(lock)
    release.write_json(job / "result.json", result)
    return 0 if result["success"] else 1


def launch(request):
    cache = Path(request.get("cache", Path.home() / ".cache/claviron-go-builder"))
    (cache / "jobs").mkdir(parents=True, exist_ok=True)
    job = Path(tempfile.mkdtemp(prefix=request["revision"][:8] + "-" + request["target"] + "-", dir=cache / "jobs"))
    source = job / "source"
    command(["git", "clone", "--quiet", "--no-hardlinks", "--no-checkout", request["repository"], source])
    command(["git", "-C", source, "checkout", "--quiet", "--detach", request["revision"]])
    (source / "prebuilds").mkdir()
    (source / "prebuilds/Cargo.lock").write_bytes(zlib.decompress(base64.b64decode(request["dependencyLock"])))
    request["cache"] = str(cache)
    (job / "request.json").write_text(json.dumps(request))
    (job / "request.json").chmod(0o600)
    with (job / "build.log").open("w") as log:
        child = subprocess.Popen([sys.executable, str(source / "scripts/remote-build.py"), "--run", str(job)],
            stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    print("ORBIT_GO_BUILD_STARTED " + json.dumps({"job": str(job), "pid": child.pid}), flush=True)
    code = child.wait()
    result = json.loads((job / "result.json").read_text())
    print("ORBIT_GO_BUILD_RESULT " + json.dumps(result), flush=True)
    return code


def synchronize(root, config):
    release = release_module(root)
    expected = release.context(root)
    targets = config.get("targets")
    if not isinstance(targets, dict) or not targets:
        raise ValueError("Sync configuration requires targets")
    complete = True
    for target, value in targets.items():
        try:
            target = release.selected_target(target)
            source = value["source"]
            if not isinstance(source, str) or not source or source.startswith("-") or any(c in source for c in "\n\r\0"):
                raise ValueError("Invalid artifact source")
            transfer(root, target, source, release, expected)
            print(f"{target}: collected and verified")
        except (OSError, ValueError, KeyError, subprocess.CalledProcessError) as error:
            complete = False
            print(f"{target}: {error}", file=sys.stderr)
    return complete


def main():
    if len(sys.argv) >= 3 and sys.argv[1] == "--launch":
        return launch(json.loads(base64.b64decode(sys.argv[2])))
    if len(sys.argv) >= 3 and sys.argv[1] == "--run":
        return worker(Path(sys.argv[2]))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hosts", type=Path, default=ROOT / ".hosts")
    parser.add_argument("--host", action="append", default=[])
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--sync", type=Path, help="collect artifacts using an ignored sync configuration; never builds")
    args = parser.parse_args()
    if args.sync:
        return 0 if synchronize(ROOT, json.loads(args.sync.read_text())) else 1
    results = build_all(ROOT, json.loads(args.hosts.read_text()), args.host, args.dry_run)
    return 1 if any(not result["success"] for result in results) else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, OSError, KeyError, subprocess.CalledProcessError) as error:
        print(f"Native build failed: {error}", file=sys.stderr)
        sys.exit(1)
