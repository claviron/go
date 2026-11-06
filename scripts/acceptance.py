#!/usr/bin/env python3
"""Run the real-Link Go fixture, then clear its fleet after every process exits."""
import argparse
import os
from pathlib import Path
import secrets
import subprocess
import sys

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--go', default=os.environ.get('GO', 'go'))
parser.add_argument('--claviron-source', type=Path)
args = parser.parse_args()
root = Path(__file__).resolve().parent.parent
target = Path(os.environ.get('CARGO_TARGET_DIR', root / 'target'))
if not target.is_absolute():
    target = root / target
fixture = target / 'goapp'
command = [sys.executable, str(root / 'scripts/build.py'), '--go', args.go]
if args.claviron_source:
    command += ['--claviron-source', str(args.claviron_source.resolve())]
command += ['--', 'build', '-race', '-o', str(fixture), './harness/goapp']
subprocess.run(command, cwd=root, check=True)
example = target / 'plain-example'
subprocess.run(command[:-2] + [str(example), './examples/plain'], cwd=root, check=True)
fleet = 'g' + secrets.token_hex(4)[:7]
result = subprocess.run([str(target / 'debug/orbit-go-acceptance'), str(fixture), fleet, str(example)], cwd=root)
if result.returncode:
    print(f'Acceptance failed: retain fleet {fleet} for inspection; stop all owners before cleanup.', file=sys.stderr)
    sys.exit(result.returncode)
# Both Go children were explicitly joined and the Rust caller has exited.
# No process owned by this harness retains any mapping at this point.
subprocess.run(['orbit', 'clear', fleet, '--all', '--yes'], check=True)
print('All acceptance processes exited; isolated fleet cleaned.')
