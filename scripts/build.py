#!/usr/bin/env python3
"""Build the native library and run Go commands with explicit native linking."""
import argparse
import json
import shlex
import os
from pathlib import Path
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--go', default=os.environ.get('GO', 'go'), help='Go executable')
parser.add_argument('--claviron-source', type=Path, help='Unreleased Claviron source override')
parser.add_argument('--release', action='store_true', help='Use the release native profile')
parser.add_argument('command', nargs=argparse.REMAINDER, help='Go command (default: test ./...)')
args = parser.parse_args()
root = Path(__file__).resolve().parent.parent
command = ['cargo']
if args.claviron_source:
    command += ['--config', 'patch.crates-io.claviron.path=' + json.dumps(str(args.claviron_source.resolve()))]
command += ['build', '--workspace', '--all-features', '--all-targets']
if args.release:
    command.append('--release')
subprocess.run(command, cwd=root, check=True)
target = Path(os.environ.get('CARGO_TARGET_DIR', root / 'target'))
if not target.is_absolute():
    target = root / target
library = target / ('release' if args.release else 'debug')
env = os.environ.copy()
env['CGO_ENABLED'] = '1'
env['CGO_LDFLAGS'] = env.get('CGO_LDFLAGS', '') + ' ' + shlex.join([f'-L{library}', f'-Wl,-rpath,{library}'])
command = args.command or ['test', './...']
if command[0] == '--':
    command = command[1:]
subprocess.run([args.go, *command], cwd=root, env=env, check=True)
