"""Fail-closed container ownership and host lease primitives (stdlib only).

Integration: lease = OwnershipLease(); lease.require() before accepting work and
at every long-running task boundary. LeaseExpired is a RuntimeError. Never cache
validity: each call reads the atomic lease and the operation fence. Unconfigured
standalone development is optional; systemd sets FN_ENHANCE_REQUIRE_LEASE=1.

lifecycle.json schema (ROOT/state/lifecycle.json):
  {"version": 1, "container_name": "litepan", "compose_labels": {
    "com.docker.compose.project": "litepan",
    "com.docker.compose.service": "litepan",
    "com.docker.compose.project.working_dir": "/absolute/compose/directory",
    "com.docker.compose.project.config_files": "/absolute/docker-compose.yml"
  }, "modules": ["resolution_metadata.py", "plugin_lifecycle.py", "plugin_manager.py"]}
All supplied labels are exact constraints; the four above are mandatory. Compose
files are existing, regular, non-symlink paths, never rewritten by these tools.
"""
import contextlib
import json
import math
import os
from pathlib import Path
import re
import stat
import subprocess
import tempfile
import time

DEFAULT_ROOT = Path('/vol1/1000/docker/appdata/fn-enhance')
DEFAULT_LEASE = Path('/run/litepan-fn-enhance/lease.json')
WORKER = 'fn-enhance.service'
MANAGER = 'fn-enhance-manager.service'
TTL = 5.0
POLL = 1.0
PROJECT = 'com.docker.compose.project'
SERVICE = 'com.docker.compose.service'
FILES = 'com.docker.compose.project.config_files'
WORKDIR = 'com.docker.compose.project.working_dir'


class LifecycleError(RuntimeError):
    pass


class LeaseExpired(RuntimeError):
    """The current process has no fresh ownership lease; stop its work."""


def checked_path(path, kind=None, missing=False):
    """Reject symlinks, junctions, non-regular objects and hard-linked files."""
    path = Path(os.path.abspath(os.fspath(path)))
    for part in reversed((path,) + tuple(path.parents)):
        try:
            info = part.lstat()
        except FileNotFoundError:
            if missing:
                continue
            raise LifecycleError('Missing path: ' + str(part))
        if stat.S_ISLNK(info.st_mode) or getattr(info, 'st_file_attributes', 0) & 0x400:
            raise LifecycleError('Symlink/reparse path rejected: ' + str(part))
        if part != path and not stat.S_ISDIR(info.st_mode):
            raise LifecycleError('Non-directory ancestor: ' + str(part))
        if part == path:
            if kind == 'file' and (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1):
                raise LifecycleError('Expected single-link regular file: ' + str(part))
            if kind == 'dir' and not stat.S_ISDIR(info.st_mode):
                raise LifecycleError('Expected directory: ' + str(part))
    return path


def read_json(path, maximum=65536):
    path = checked_path(path, 'file')
    fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > maximum:
            raise LifecycleError('Invalid or oversized JSON: ' + str(path))
        raw = stream.read(maximum + 1)
    if len(raw) > maximum:
        raise LifecycleError('JSON size limit exceeded')
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise LifecycleError('Duplicate JSON key: ' + key)
            result[key] = value
        return result
    return json.loads(raw, object_pairs_hook=unique)


def sync_directory(path):
    if os.name != 'nt':
        fd = os.open(path, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def atomic_json(path, value):
    path = checked_path(path, 'file', missing=True)
    checked_path(path.parent, 'dir')
    fd, temporary = tempfile.mkstemp(prefix='.' + path.name + '-', dir=path.parent)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as stream:
            json.dump(value, stream, allow_nan=False, separators=(',', ':'))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        sync_directory(path.parent)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def boot_id():
    # An old monotonic timestamp must never become valid after reboot.
    with open('/proc/sys/kernel/random/boot_id', encoding='ascii') as stream:
        return stream.read(128).strip()


class FileLock:
    """Exclusive flock on Linux; Windows byte lock permits standalone unit tests."""
    def __init__(self, path, timeout=30):
        self.path, self.timeout, self.fd = Path(path), timeout, None

    def __enter__(self):
        checked_path(self.path, 'file', missing=True)
        self.fd = os.open(self.path, os.O_RDWR | os.O_CREAT | getattr(os, 'O_NOFOLLOW', 0), 0o600)
        info = os.fstat(self.fd)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            os.close(self.fd)
            self.fd = None
            raise LifecycleError('Invalid lock file')
        deadline = time.monotonic() + self.timeout
        while True:
            try:
                if os.name == 'nt':
                    import msvcrt
                    os.lseek(self.fd, 0, 0)
                    msvcrt.locking(self.fd, msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return self
            except OSError:
                if time.monotonic() >= deadline:
                    os.close(self.fd)
                    self.fd = None
                    raise LifecycleError('Lock acquisition timed out: ' + str(self.path))
                time.sleep(0.05)

    def __exit__(self, *args):
        if self.fd is not None:
            try:
                if os.name == 'nt':
                    import msvcrt
                    os.lseek(self.fd, 0, 0)
                    msvcrt.locking(self.fd, msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(self.fd, fcntl.LOCK_UN)
            finally:
                os.close(self.fd)
                self.fd = None


class OwnershipLease:
    def __init__(self, path=None, required=None):
        self.path = Path(path if path is not None else os.environ.get('FN_ENHANCE_LEASE_PATH', DEFAULT_LEASE))
        setting = os.environ.get('FN_ENHANCE_REQUIRE_LEASE', '').strip().lower()
        self.required = (setting not in ('', '0', 'false', 'no', 'off')) if required is None else bool(required)

    def snapshot(self):
        """Return a newly read valid lease dict, otherwise None (including fence)."""
        try:
            if os.path.lexists(self.path.parent / 'fence.json'):
                return None
            data = read_json(self.path)
            if not isinstance(data, dict) or data.get('version') != 1:
                return None
            stamp, expires = data.get('timestamp'), data.get('expires_at')
            if any(type(v) not in (int, float) or not math.isfinite(v) for v in (stamp, expires)):
                return None
            now = time.monotonic()
            if not (0 <= stamp <= now < expires and 0 < expires - stamp <= TTL):
                return None
            if not re.fullmatch('[0-9a-f]{64}', data.get('container_id', '')):
                return None
            if not isinstance(data.get('started_at'), str) or not data['started_at']:
                return None
            if data.get('boot_id') != boot_id():
                return None
            # Check fence again: a concurrent administrative operation wins.
            if os.path.lexists(self.path.parent / 'fence.json'):
                return None
            return dict(data)
        except (OSError, ValueError, TypeError, LifecycleError):
            return None

    def valid(self):
        return not self.required or self.snapshot() is not None

    def require(self):
        data = self.snapshot()
        if self.required and data is None:
            raise LeaseExpired('LitePan ownership lease expired, fenced, or unavailable')
        return data


class LeaseStore:
    def __init__(self, path=DEFAULT_LEASE):
        self.path = Path(path)
        checked_path(self.path.parent, 'dir', missing=True)
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        checked_path(self.path.parent, 'dir')
        self.fence_path = self.path.parent / 'fence.json'
        self.lock_path = self.path.parent / 'lease.lock'

    def _revoke(self):
        checked_path(self.path, 'file', missing=True)
        self.path.unlink(missing_ok=True)
        sync_directory(self.path.parent)

    def revoke(self):
        with FileLock(self.lock_path, timeout=0.1):
            self._revoke()

    def fence(self, reason):
        with FileLock(self.lock_path, timeout=0.1):
            atomic_json(self.fence_path, {'reason': reason, 'timestamp': time.monotonic()})
            self._revoke()

    def unfence(self):
        with FileLock(self.lock_path, timeout=0.1):
            checked_path(self.fence_path, 'file', missing=True)
            self.fence_path.unlink(missing_ok=True)
            sync_directory(self.path.parent)

    def grant(self, identity):
        with FileLock(self.lock_path, timeout=0.1):
            if os.path.lexists(self.fence_path):
                self._revoke()
                return False
            stamp = time.monotonic()
            atomic_json(self.path, {'version': 1, 'timestamp': stamp,
                'expires_at': stamp + TTL, 'container_id': identity[0],
                'started_at': identity[1], 'boot_id': boot_id()})
            return True


class Config:
    def __init__(self, root=DEFAULT_ROOT):
        self.root = checked_path(root, 'dir')
        checked_path(self.root / 'state', 'dir')
        data = read_json(self.root / 'state' / 'lifecycle.json')
        if not isinstance(data, dict) or data.get('version') != 1:
            raise LifecycleError('Unsupported lifecycle config version')
        self.name = data.get('container_name')
        if not isinstance(self.name, str) or not re.fullmatch('[a-zA-Z0-9][a-zA-Z0-9_.-]*', self.name):
            raise LifecycleError('Invalid exact container_name')
        self.labels = data.get('compose_labels')
        if not isinstance(self.labels, dict) or any(k not in self.labels for k in (PROJECT, SERVICE, FILES, WORKDIR)):
            raise LifecycleError('Four exact Compose ownership labels are mandatory')
        if any(not isinstance(k, str) or not k.startswith('com.docker.compose.') or
               not isinstance(v, str) or not v or '\x00' in v for k, v in self.labels.items()):
            raise LifecycleError('Invalid Compose label constraints')
        for key in (PROJECT, SERVICE):
            if not re.fullmatch('[a-zA-Z0-9][a-zA-Z0-9_.-]*', self.labels[key]):
                raise LifecycleError('Invalid Compose project/service')
        paths = self.labels[FILES].split(',')
        if any(not p or not Path(p).is_absolute() for p in paths):
            raise LifecycleError('Compose config_files must be exact absolute paths')
        self.compose_files = [checked_path(p, 'file') for p in paths]
        if len(set(self.compose_files)) != len(paths):
            raise LifecycleError('Repeated Compose config file')
        if not Path(self.labels[WORKDIR]).is_absolute():
            raise LifecycleError('Compose working_dir must be absolute')
        self.compose_dir = checked_path(self.labels[WORKDIR], 'dir')
        self.modules = data.get('modules', ['resolution_metadata.py', 'task_runtime.py', 'actor_visibility.py', 'plugin_lifecycle.py', 'plugin_manager.py'])
        if not isinstance(self.modules, list) or any(not isinstance(m, str) or
                not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*\.py', m) or m in
                ('server.py', 'install.py', 'setup.py', 'sitecustomize.py', 'usercustomize.py') for m in self.modules):
            raise LifecycleError('Invalid update module allowlist')


class Runner:
    def run(self, args, timeout=15):
        # No shell, no untrusted cwd/import path, no inherited Compose overrides.
        env = {k: v for k, v in os.environ.items() if not k.startswith(('COMPOSE_', 'DOCKER_'))}
        try:
            result = subprocess.run(args, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, text=True, timeout=timeout, check=False, env=env, cwd='/')
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise LifecycleError('Command failed: ' + repr(args) + ': ' + str(exc)) from exc
        if result.returncode:
            raise LifecycleError('Command failed: ' + repr(args) + ': ' + result.stderr[-2000:])
        if len(result.stdout) > 4 * 1024 * 1024:
            raise LifecycleError('Command output exceeds bound: ' + repr(args))
        return result.stdout


class Docker:
    def __init__(self, config, runner=None):
        self.config, self.runner = config, runner or Runner()

    def observe(self):
        # Successful enumeration distinguishes absence from an unavailable daemon.
        text = self.runner.run(['docker', 'container', 'ls', '--all', '--no-trunc',
            '--filter', 'name=^/' + re.escape(self.config.name) + '$', '--format', '{{.ID}}'], timeout=0.8)
        ids = text.split()
        if not ids:
            return None
        if len(ids) != 1 or not re.fullmatch('[0-9a-f]{64}', ids[0]):
            raise LifecycleError('Ambiguous/invalid exact container observation')
        rows = json.loads(self.runner.run(['docker', 'container', 'inspect', ids[0]], timeout=0.8))
        if not isinstance(rows, list) or len(rows) != 1:
            raise LifecycleError('Invalid Docker inspect response')
        row = rows[0]
        labels = row.get('Config', {}).get('Labels') or {}
        if row.get('Id') != ids[0] or row.get('Name') != '/' + self.config.name or any(
                labels.get(k) != v for k, v in self.config.labels.items()):
            raise LifecycleError('Container name/Compose ownership mismatch')
        state = row.get('State')
        if not isinstance(state, dict):
            raise LifecycleError('Missing container State')
        return row

    @staticmethod
    def identity(row):
        state = row['State']
        if (state.get('Running') is not True or state.get('Paused') is not False or
                state.get('Restarting') is not False or state.get('Dead') is not False or
                state.get('Status') != 'running'):
            return None
        started = state.get('StartedAt')
        if not isinstance(started, str) or not started or started.startswith('0001-'):
            raise LifecycleError('Missing/invalid container StartedAt')
        return row['Id'], started

    def compose(self, *args):
        for file in self.config.compose_files:
            checked_path(file, 'file')
        checked_path(self.config.compose_dir, 'dir')
        return ['docker', 'compose', '--project-name', self.config.labels[PROJECT],
            '--project-directory', str(self.config.compose_dir), *[item for file in
            self.config.compose_files for item in ('-f', str(file))], *args,
            self.config.labels[SERVICE]]

    def verify_compose(self):
        """No deployment changes: validate the resolved single-service target."""
        model = json.loads(self.runner.run(self.compose('config', '--format', 'json'), timeout=15))
        services = model.get('services', {})
        service = services.get(self.config.labels[SERVICE], {})
        if model.get('name') != self.config.labels[PROJECT] or service.get('container_name') != self.config.name:
            raise LifecycleError('Compose model does not confirm exact project/service/container_name')
        if service.get('build') or service.get('develop') or service.get('post_start') or service.get('pre_stop'):
            raise LifecycleError('Build/develop/lifecycle hooks are not accepted for lifecycle operations')
        return service

    def stop_container(self, row):
        if row is None:
            return
        current = self.observe()
        if current is None or current['Id'] != row['Id']:
            raise LifecycleError('Container identity changed before stop')
        self.runner.run(['docker', 'container', 'stop', '--time', '10', row['Id']], timeout=15)
        stopped = self.observe()
        if stopped is not None and (stopped['Id'] != row['Id'] or
                stopped['State'].get('Running') is not False or
                stopped['State'].get('Restarting') is not False):
            raise LifecycleError('Container stop not confirmed or identity replaced')

    def recreate(self):
        self.observe()  # Reject a newly substituted foreign container before Compose mutates it.
        self.verify_compose()
        self.runner.run(self.compose('up', '-d', '--force-recreate', '--no-deps', '--no-build', '--pull', 'never'), timeout=60)


class Units:
    def __init__(self, runner=None):
        self.runner = runner or Runner()

    def stop(self, unit):
        self.runner.run(['systemctl', 'stop', unit], timeout=15)
        # ActiveState + ControlGroup process cleanup is owned by KillMode=control-group.
        state = self.runner.run(['systemctl', 'show', '--property=ActiveState', '--value', unit], timeout=2).strip()
        if state not in ('inactive', 'failed'):
            raise LifecycleError('Unit failed to stop synchronously: ' + unit + ': ' + state)

    def start(self, unit):
        if unit == WORKER:
            # Type=simple will become active promptly; do not stall observations
            # behind a start job. Subsequent polls verify its actual ActiveState.
            self.runner.run(['systemctl', 'start', '--no-block', unit], timeout=0.3)
        else:
            self.runner.run(['systemctl', 'start', unit], timeout=15)

    def active(self, unit):
        return self.runner.run(['systemctl', 'show', '--property=ActiveState',
            '--value', unit], timeout=0.3).strip() == 'active'

    def disable(self, unit):
        self.runner.run(['systemctl', 'disable', unit], timeout=15)


def main(argv=None):
    """Exact-path systemd guard: require before start, revoke before group stop."""
    import argparse
    import sys
    parser = argparse.ArgumentParser(description=main.__doc__)
    parser.add_argument('command', choices=('require', 'revoke'))
    parser.add_argument('--lease-path', type=Path, default=DEFAULT_LEASE)
    args = parser.parse_args(argv)
    try:
        if args.command == 'require':
            OwnershipLease(args.lease_path, required=True).require()
        else:
            LeaseStore(args.lease_path).revoke()
        return 0
    except Exception as exc:
        print('Lifecycle guard failed: ' + str(exc), file=sys.stderr)
        return 1


if __name__ == '__main__':
    import sys
    sys.exit(main())

