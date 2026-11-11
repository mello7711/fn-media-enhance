"""Cooperative batch cancellation, commit fencing and owned subprocess cleanup.

No server, lease or executor is created here. ``ownership_valid`` must read the
current lease without calling back into the runtime. Integrators must put every
transaction AND its commit inside ``ctx.commit_guard()`` (or pass the connection
to that context manager for automatic commit/rollback). A Docker stop or an
external lease change is not an instantaneous write barrier: writes stop after
those conditions are observed at a fence. Workers must check around blocking
operations; arbitrary non-cooperative Python workers cannot be forcibly stopped.

``on_complete`` receives a detached terminal snapshot, outside the fence lock.
Only state == 'done' may advance autofill full_done, including done-with-errors.
The next incremental run should select missing rows, not replay this summary.
"""
import copy
import errno
import html
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import tempfile
import threading
import time
import unicodedata
from urllib.parse import unquote
import uuid
from contextlib import contextmanager

MAX_ROWS = 10000
MAX_ERRORS = 100
MAX_STATE_BYTES = 64 * 1024
MAX_OUTPUT_BYTES = 8 * 1024 * 1024  # Combined retained stdout + stderr.
POLL_SECONDS = .05
TERMINATE_GRACE = .3
REAP_SECONDS = 1.0

ERROR_MESSAGES = {
    'CANCELLED': 'Task cancelled.',
    'SHUTDOWN': 'Runtime is shutting down.',
    'OWNERSHIP_LOST': 'Runtime ownership is no longer valid.',
    'BUSY': 'A batch is already running.',
    'INVALID_INPUT': 'Invalid task input.',
    'DATABASE_BUSY': 'Database is busy; retry this item later.',
    'DATABASE_ERROR': 'Database operation failed.',
    'NOT_FOUND': 'Required file or executable was not found.',
    'PERMISSION_DENIED': 'Required file or executable is not accessible.',
    'DISK_FULL': 'Insufficient storage space.',
    'IO_ERROR': 'File or process operation failed.',
    'PROCESS_TIMEOUT': 'Process exceeded its time limit.',
    'OUTPUT_LIMIT': 'Process output exceeded the permitted size.',
    'PROCESS_FAILED': 'Media process failed.',
    'PROCESS_CLEANUP_FAILED': 'Child process cleanup did not complete.',
    'INVALID_MEDIA': 'Media probe returned invalid data.',
    'NO_VIDEO': 'Media has no recognizable video stream.',
    'INTERNAL_ERROR': 'Task operation failed.',
    'PERSISTENCE_FAILED': 'Terminal task summary could not be saved.',
    'CALLBACK_FAILED': 'Completion callback failed.',
}


class TaskError(Exception):
    """Allowlisted public errors. Caller-provided messages are never echoed."""
    def __init__(self, code='INTERNAL_ERROR', message=None, *, status=None):
        if isinstance(code, str) and re.fullmatch(r'HTTP_[45]\d\d', code):
            status = int(code[5:])
        if (code == 'HTTP_ERROR' or isinstance(code, str) and code.startswith('HTTP_')) and (
                type(status) is int and 400 <= status <= 599):
            self.code = f'HTTP_{status}'
            self.message = f'Media source returned HTTP {status}.'
        else:
            self.code = code if isinstance(code, str) and code in ERROR_MESSAGES else 'INTERNAL_ERROR'
            self.message = ERROR_MESSAGES[self.code]
        super().__init__(self.message)


class TaskCancelled(TaskError):
    def __init__(self, code='CANCELLED'):
        super().__init__(code if code in ('CANCELLED', 'SHUTDOWN', 'OWNERSHIP_LOST') else 'CANCELLED')


_HTTP_STATUS = re.compile(
    rb'(?:\bHTTP\s*(?:error\s*[:=]?\s*|/[0-9.]+\s+|status\s*[:=]?\s*)|'
    rb'\bServer returned\s+)([45][0-9]{2})\b', re.I)


def classify_failure(error=None, *, stderr=None):
    """Inspect error TYPE/errno, or known ffprobe HTTP phrases; never raw text."""
    if isinstance(error, TaskError):
        return TaskCancelled(error.code) if isinstance(error, TaskCancelled) else TaskError(error.code)
    if isinstance(error, (subprocess.TimeoutExpired, TimeoutError)):
        return TaskError('PROCESS_TIMEOUT')
    if isinstance(error, sqlite3.Error):
        number = getattr(error, 'sqlite_errorcode', None)
        if isinstance(number, int) and number & 0xff in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
            return TaskError('DATABASE_BUSY')
        # Older Python/driver exceptions may omit sqlite_errorcode. Only fixed
        # SQLite lock phrases are examined; they are never used as output.
        if isinstance(error, sqlite3.OperationalError) and re.search(
                r'\bdatabase (?:is locked|is busy|table is locked)\b', str(error), re.I):
            return TaskError('DATABASE_BUSY')
        return TaskError('DATABASE_ERROR')
    if isinstance(error, OSError):
        code = {errno.ENOENT: 'NOT_FOUND', errno.EACCES: 'PERMISSION_DENIED',
                errno.EPERM: 'PERMISSION_DENIED', errno.ENOSPC: 'DISK_FULL'}.get(error.errno, 'IO_ERROR')
        return TaskError(code)
    if isinstance(error, (json.JSONDecodeError, UnicodeError)):
        return TaskError('INVALID_MEDIA')
    if isinstance(error, (ValueError, TypeError)):
        return TaskError('INVALID_INPUT')
    if stderr is not None:
        if isinstance(stderr, str):
            stderr = stderr[:MAX_OUTPUT_BYTES].encode('utf-8', 'replace')
        if isinstance(stderr, bytes):
            match = _HTTP_STATUS.search(stderr[:MAX_OUTPUT_BYTES])
            if match:
                return TaskError('HTTP_ERROR', status=int(match[1]))
        return TaskError('PROCESS_FAILED')
    if isinstance(error, subprocess.CalledProcessError):
        return classify_failure(stderr=error.stderr or b'')
    return TaskError('INTERNAL_ERROR')


_URL = re.compile(r'(?:[a-z][a-z0-9+.-]*\s*:\s*/\s*/|//|www\.)[^\s<>\"\']*', re.I)
_CREDENTIAL = re.compile(
    r'\b(?:password|passwd|pwd|secret|token|access[_-]?token|refresh[_-]?token|'
    r'api[_-]?key|authorization|credential|signature|sig|x-amz-signature)\b'
    r'\s*[\"\']?\s*[:=]\s*[^\s,;<>]*', re.I)
_USERINFO = re.compile(r'[^\s/:@]+:[^\s/@]+@[^\s<>]*')


def safe_text(value, *, limit=512, basename=False):
    """Decode, redact known URL/credential forms, bound UTF-8, HTML-escape."""
    if not isinstance(value, str):
        return ''
    # Limit work on hostile names. Redaction happens before any basename split,
    # so a URL's final component cannot become an apparently harmless filename.
    value = value[:4096]
    for _ in range(3):
        decoded = html.unescape(unquote(value))
        if decoded == value:
            break
        value = decoded
    value = ''.join(' ' if ch.isspace() else ch for ch in value
                    if not unicodedata.category(ch).startswith('C') or ch.isspace())
    value = _URL.sub('[redacted]', value)
    value = _USERINFO.sub('[redacted]', value)
    value = _CREDENTIAL.sub('[redacted]', value)
    if basename:
        value = value.replace('\\', '/').rstrip('/').rsplit('/', 1)[-1]
        value = value.split('?', 1)[0].split('#', 1)[0]
        if value in ('.', '..'):
            value = ''
    value = ' '.join(value.split())
    escaped = html.escape(value, quote=True)
    # Do not leave a partial HTML entity when trimming expanded escape text.
    out = []
    used = 0
    for ch in value:
        fragment = html.escape(ch, quote=True)
        size = len(fragment.encode('utf-8'))
        if used + size > limit:
            break
        out.append(fragment)
        used += size
    return ''.join(out) if len(escaped.encode('utf-8')) > limit else escaped


def _idle():
    return {'source': 'manual', 'run_id': None, 'state': 'idle', 'done': 0,
            'total': 0, 'success': 0, 'failed': 0, 'current': None,
            'started_at': None, 'finished_at': None, 'errors': [], 'errors_omitted': 0}


def _identity(row):
    if isinstance(row, str):
        row = {'guid': row}
    if not hasattr(row, 'keys'):
        raise TaskError('INVALID_INPUT')
    guid = row['guid'] if 'guid' in row.keys() else None
    if not isinstance(guid, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', guid):
        raise TaskError('INVALID_INPUT')
    return {'guid': guid,
            'title': safe_text(row['title'] if 'title' in row.keys() else ''),
            'filename': safe_text(row['filename'] if 'filename' in row.keys() else
                                  row['path'] if 'path' in row.keys() else '', limit=256, basename=True),
            'stage': 'queued'}


def _encoded(summary):
    return json.dumps(summary, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode('utf-8')


def _budget(summary):
    while len(summary['errors']) > MAX_ERRORS or len(_encoded(summary)) > MAX_STATE_BYTES:
        if not summary['errors']:
            raise TaskError('PERSISTENCE_FAILED')
        summary['errors'].pop()
        summary['errors_omitted'] += 1


class TaskContext:
    def __init__(self, tracker, run_id, cancel_event, identity):
        self.runtime = tracker.runtime
        self.tracker = tracker
        self.run_id = run_id
        self.cancel_event = cancel_event
        self._identity = identity.copy()

    def _check_locked(self):
        self.runtime._check_locked(self.cancel_event)
        if (self.tracker._summary['run_id'] != self.run_id or
                self.tracker._summary['state'] != 'running'):
            raise TaskCancelled()

    def check(self):
        with self.runtime.fence_lock:
            self._check_locked()

    def stage(self, stage):
        with self.runtime.fence_lock:
            self._check_locked()
            self._identity['stage'] = safe_text(stage, limit=96)
            self.tracker._summary['current'] = self._identity.copy()

    @contextmanager
    def commit_guard(self, connection=None):
        """Fence a full write transaction; optionally own commit and rollback.

        With no connection argument, the caller must commit INSIDE the guard,
        with its connection context nested inside the guard for rollback.
        With a connection argument, do not manually commit in the body: this
        manager checks again immediately before commit, and rolls back on error.
        """
        with self.runtime.fence_lock:
            try:
                self._check_locked()
                yield
                self._check_locked()
                if connection is not None:
                    connection.commit()
            except BaseException:
                if connection is not None:
                    connection.rollback()
                raise


class _Child:
    def __init__(self, process, cancel_event):
        self.process = process
        self.cancel_event = cancel_event
        self.stop_lock = threading.Lock()
        self.output_lock = threading.Lock()
        self.output_limit = threading.Event()
        self.io_error = threading.Event()
        self.output = [bytearray(), bytearray()]
        self.readers = []

    def read(self, pipe, index, limit):
        try:
            while True:
                block = pipe.read1(65536)
                if not block:
                    break
                with self.output_lock:
                    remaining = limit - sum(map(len, self.output))
                    self.output[index].extend(block[:max(0, remaining)])
                    if len(block) > remaining:
                        self.output_limit.set()
                # Drain/discard excess until the controller reaps the child.
        except (OSError, ValueError):
            self.io_error.set()
        finally:
            pipe.close()

    def start_readers(self, limit):
        # communicate(timeout=short_poll) is used ONLY to wait/reap. Its normal
        # PIPE accumulation is unbounded, so dedicated bounded readers own pipes.
        pipes = (self.process.stdout, self.process.stderr)
        self.process.stdout = self.process.stderr = None
        for index, pipe in enumerate(pipes):
            thread = threading.Thread(target=self.read, args=(pipe, index, limit), daemon=True)
            self.readers.append(thread)
            thread.start()

    def stop(self):
        with self.stop_lock:
            deadline = time.monotonic() + REAP_SECONDS
            if self.process.poll() is None:
                try:
                    self.process.terminate()
                except ProcessLookupError:
                    pass
                except OSError:
                    pass
                try:
                    self.process.communicate(timeout=TERMINATE_GRACE)
                except subprocess.TimeoutExpired:
                    try:
                        self.process.kill()
                    except OSError:
                        pass
            try:
                self.process.communicate(timeout=max(.01, deadline - time.monotonic()))
            except (subprocess.TimeoutExpired, OSError):
                return False
            for reader in self.readers:
                reader.join(max(0, deadline - time.monotonic()))
            return self.process.poll() is not None and not any(t.is_alive() for t in self.readers)


class FillTracker:
    """One serial batch, independent of an external transcode executor."""
    def __init__(self, runtime):
        self.runtime = runtime
        self._summary = _idle()
        self._cancel_event = threading.Event()
        self._thread = None

    def start(self, rows, worker, source='manual', on_complete=None):
        if source not in ('manual', 'auto') or not callable(worker) or (
                on_complete is not None and not callable(on_complete)):
            raise TaskError('INVALID_INPUT')
        identities, seen = [], set()
        try:
            for number, row in enumerate(rows):
                if number >= MAX_ROWS:
                    raise TaskError('INVALID_INPUT')
                identity = _identity(row)
                if identity['guid'] not in seen:
                    identities.append(identity)
                    seen.add(identity['guid'])
        except TaskError:
            raise
        except Exception:
            raise TaskError('INVALID_INPUT') from None
        with self.runtime.fence_lock:
            self.runtime._check_locked()
            if self._summary['state'] == 'running':
                raise TaskError('BUSY')
            run_id = uuid.uuid4().hex
            self._cancel_event = threading.Event()
            self._summary = _idle()
            self._summary.update(source=source, run_id=run_id, state='running',
                                 total=len(identities), started_at=time.time())
            thread = threading.Thread(target=self._run,
                                      args=(run_id, identities, worker, self._cancel_event, on_complete),
                                      name='fill-' + run_id[:8], daemon=True)
            self._thread = thread
            self.runtime._threads.add(thread)
            try:
                thread.start()
            except Exception:
                self.runtime._threads.discard(thread)
                self._summary.update(state='cancelled', finished_at=time.time())
                self.runtime._persist_locked(self._summary)
                raise TaskError('INTERNAL_ERROR') from None
        return run_id

    def _run(self, run_id, identities, worker, cancel_event, on_complete):
        cancelled = False
        try:
            for identity in identities:
                ctx = TaskContext(self, run_id, cancel_event, identity)
                try:
                    ctx.stage('queued')
                    worker(identity['guid'], ctx)
                    ctx.check()
                except TaskCancelled:
                    cancelled = True
                    break
                except Exception as error:
                    failure = classify_failure(error)
                    with self.runtime.fence_lock:
                        try:
                            ctx._check_locked()
                        except TaskCancelled:
                            cancelled = True
                            break
                        self._summary['failed'] += 1
                        entry = ctx._identity.copy()
                        entry.update(code=failure.code, message=failure.message)
                        self._summary['errors'].append(entry)
                        _budget(self._summary)
                        self._summary['done'] += 1
                else:
                    with self.runtime.fence_lock:
                        try:
                            ctx._check_locked()
                        except TaskCancelled:
                            cancelled = True
                            break
                        self._summary['success'] += 1
                        self._summary['done'] += 1
                with self.runtime.fence_lock:
                    self._summary['current'] = None
        except BaseException:
            # SystemExit/KeyboardInterrupt in a worker must not strand running
            # state or invoke an apparent successful callback.
            cancelled = True
        finally:
            with self.runtime.fence_lock:
                try:
                    self.runtime._check_locked(cancel_event)
                except TaskCancelled:
                    cancelled = True
                self._summary.update(state='cancelled' if cancelled else 'done',
                                     current=None, finished_at=time.time())
                self.runtime._persist_locked(self._summary)
                terminal = copy.deepcopy(self._summary)
            try:
                if on_complete is not None:
                    on_complete(terminal)
            except Exception:
                with self.runtime.fence_lock:
                    if self._summary['run_id'] == run_id:
                        self._summary['callback_error'] = 'CALLBACK_FAILED'
                        self.runtime._persist_locked(self._summary)
            finally:
                with self.runtime.fence_lock:
                    self.runtime._threads.discard(threading.current_thread())

    def cancel(self, auto_only=False):
        with self.runtime.fence_lock:
            if self._summary['state'] != 'running' or auto_only and self._summary['source'] != 'auto':
                return False
            # The same fence as transaction entry/check/commit. Cancel returns
            # only after any already-fenced transaction has completed.
            self._cancel_event.set()
            children = [child for child in self.runtime._children
                        if child.cancel_event is self._cancel_event]
        for child in children:
            child.stop()
        return True

    def snapshot(self):
        with self.runtime.fence_lock:
            return copy.deepcopy(self._summary)

    def clear_history(self):
        """Forget the persisted terminal summary. Only a finished/cancelled
        run can be cleared; running and idle states return False."""
        with self.runtime.fence_lock:
            if self._summary['state'] not in ('done', 'cancelled'):
                return False
            self._summary = _idle()
            self.runtime._forget_state_locked()
            return True

    def wait(self, timeout=None):
        with self.runtime.fence_lock:
            thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout)
        return thread is None or not thread.is_alive()


class TaskRuntime:
    def __init__(self, state_path, ownership_valid):
        if not callable(ownership_valid):
            raise TaskError('INVALID_INPUT')
        self.state_path = Path(state_path)
        self.ownership_valid = ownership_valid
        self.shutdown_event = threading.Event()
        self.fence_lock = threading.RLock()
        self._children = set()
        self._threads = set()  # Batch threads only; never join an executor worker.
        self._process_calls = set()
        self.fill = FillTracker(self)
        self.tracker = self.fill
        with self.fence_lock:
            self._load_locked()

    def _check_locked(self, cancel_event=None):
        if self.shutdown_event.is_set():
            raise TaskCancelled('SHUTDOWN')
        if cancel_event is not None and cancel_event.is_set():
            raise TaskCancelled()
        try:
            valid = bool(self.ownership_valid())
        except Exception:
            valid = False
        if not valid:
            raise TaskCancelled('OWNERSHIP_LOST')

    def check(self, cancel_event=None):
        with self.fence_lock:
            self._check_locked(cancel_event)

    @property
    def active_children(self):
        with self.fence_lock:
            return len(self._children)

    def start(self, rows, worker, source='manual', on_complete=None):
        return self.fill.start(rows, worker, source, on_complete)

    def cancel(self, auto_only=False):
        return self.fill.cancel(auto_only)

    def snapshot(self):
        return self.fill.snapshot()

    def clear_history(self):
        return self.fill.clear_history()

    def wait(self, timeout=None):
        return self.fill.wait(timeout)

    def _forget_state_locked(self):
        try:
            self.state_path.unlink()
        except OSError:
            pass

    def _persist_locked(self, summary):
        _budget(summary)
        summary.pop('persistence_error', None)
        temporary = None
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            fd, temporary = tempfile.mkstemp(prefix='.' + self.state_path.name + '-',
                                              dir=self.state_path.parent)
            with os.fdopen(fd, 'wb') as output:
                os.chmod(temporary, 0o600)
                output.write(_encoded(summary))
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, self.state_path)
            temporary = None
        except (OSError, ValueError):
            summary['persistence_error'] = 'PERSISTENCE_FAILED'
        finally:
            if temporary is not None:
                try:
                    os.unlink(temporary)
                except OSError:
                    pass

    def _load_locked(self):
        try:
            with self.state_path.open('rb') as saved:
                raw = saved.read(MAX_STATE_BYTES + 1)
            if len(raw) > MAX_STATE_BYTES:
                return
            data = json.loads(raw)
            if not isinstance(data, dict) or data.get('state') not in ('running', 'done', 'cancelled'):
                return
            summary = _idle()
            summary['source'] = data.get('source') if data.get('source') in ('manual', 'auto') else 'manual'
            token = data.get('run_id')
            summary['run_id'] = token if isinstance(token, str) and re.fullmatch('[a-f0-9]{32}', token) else None
            for key in ('total', 'success', 'failed', 'errors_omitted'):
                value = data.get(key, 0)
                summary[key] = min(MAX_ROWS, max(0, value)) if type(value) is int else 0
            summary['success'] = min(summary['success'], summary['total'])
            summary['failed'] = min(summary['failed'], summary['total'] - summary['success'])
            summary['done'] = summary['success'] + summary['failed']
            for key in ('started_at', 'finished_at'):
                value = data.get(key)
                if type(value) in (int, float) and math.isfinite(value) and 0 <= value <= 1e12:
                    summary[key] = value
            entries = data.get('errors', [])
            if isinstance(entries, list):
                for row in entries:
                    try:
                        identity = _identity(row)
                    except TaskError:
                        continue
                    identity['stage'] = safe_text(row.get('stage', ''), limit=96)
                    failure = TaskError(row.get('code'))
                    identity.update(code=failure.code, message=failure.message)
                    summary['errors'].append(identity)
                _budget(summary)
            summary['state'] = 'cancelled' if data['state'] == 'running' else data['state']
            if data['state'] == 'running':
                summary['finished_at'] = time.time()
            self.fill._summary = summary
            # Canonicalize untrusted saved text; interrupted work never resumes.
            self._persist_locked(summary)
        except (OSError, ValueError, TypeError):
            return

    def run_process(self, cmd, *, timeout=45, ctx=None, cancel_event=None,
                    max_output_bytes=MAX_OUTPUT_BYTES, check=True, cwd=None, env=None):
        """Run a command without a shell, polling communicate and bounding pipes.

        Returns subprocess.CompletedProcess with byte stdout/stderr. Nonzero
        exit raises an allowlisted TaskError by default; check=False returns it
        for callers needing custom *safe* classification. Process output/args
        are never copied to summaries. Independent cancel_event is for transcode
        work; it is unrelated to fill cancellation unless ctx is also supplied.
        """
        if not isinstance(cmd, (list, tuple)) or not cmd or not all(isinstance(x, (str, os.PathLike)) for x in cmd):
            raise TaskError('INVALID_INPUT')
        if (type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0 or
                type(max_output_bytes) is not int or not 0 < max_output_bytes <= MAX_OUTPUT_BYTES):
            raise TaskError('INVALID_INPUT')
        if ctx is not None and ctx.runtime is not self:
            raise TaskError('INVALID_INPUT')
        event = ctx.cancel_event if ctx is not None else cancel_event
        thread = threading.current_thread()
        child = None
        call_done = threading.Event()
        call = (thread, call_done)
        registered = False
        started = time.monotonic()
        try:
            with self.fence_lock:
                self._check_locked(event)
                if cancel_event is not None:
                    self._check_locked(cancel_event)
                if ctx is not None:
                    ctx._check_locked()
                process = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE, cwd=cwd, env=env, shell=False)
                child = _Child(process, event)
                self._children.add(child)
                self._process_calls.add(call)
                registered = True
                child.start_readers(max_output_bytes)
            while True:
                with self.fence_lock:
                    self._check_locked(event)
                    if cancel_event is not None:
                        self._check_locked(cancel_event)
                    if ctx is not None:
                        ctx._check_locked()
                if child.output_limit.is_set():
                    raise TaskError('OUTPUT_LIMIT')
                if child.io_error.is_set():
                    raise TaskError('IO_ERROR')
                remaining = timeout - (time.monotonic() - started)
                if remaining <= 0:
                    raise TaskError('PROCESS_TIMEOUT')
                try:
                    process.communicate(timeout=min(POLL_SECONDS, remaining))
                    break
                except subprocess.TimeoutExpired:
                    continue
            for reader in child.readers:
                reader.join(REAP_SECONDS)
            self.check(event)
            if cancel_event is not None:
                self.check(cancel_event)
            if child.output_limit.is_set():
                raise TaskError('OUTPUT_LIMIT')
            if child.io_error.is_set():
                raise TaskError('IO_ERROR')
            if any(reader.is_alive() for reader in child.readers):
                raise TaskError('PROCESS_CLEANUP_FAILED')
            result = subprocess.CompletedProcess(cmd, process.returncode,
                                                 bytes(child.output[0]), bytes(child.output[1]))
            if check and result.returncode:
                raise classify_failure(stderr=result.stderr)
            return result
        except TaskError:
            raise
        except Exception as error:
            raise classify_failure(error) from None
        finally:
            if child is not None:
                clean = child.stop()
                with self.fence_lock:
                    if clean:
                        self._children.discard(child)
            if registered:
                with self.fence_lock:
                    self._process_calls.discard(call)
                    call_done.set()

    def shutdown(self, timeout=None):
        """Fence shutdown, terminate/kill/reap children, join cooperative workers.

        Returns False if an optional join deadline expires or cleanup remains;
        callers must not treat that result as a successful quiescent shutdown.
        Does not shut down an executor owned by the integrating server.
        """
        deadline = None if timeout is None else time.monotonic() + max(0, timeout)
        with self.fence_lock:
            self.shutdown_event.set()
            self.fill._cancel_event.set()
            children = list(self._children)
        # Fan out termination first, rather than one grace interval per child.
        for child in children:
            if child.process.poll() is None:
                try:
                    child.process.terminate()
                except OSError:
                    pass
        for child in children:
            if child.stop():
                with self.fence_lock:
                    self._children.discard(child)
        while True:
            with self.fence_lock:
                threads = [thread for thread in self._threads if thread is not threading.current_thread()]
                calls = [event for owner, event in self._process_calls
                         if owner is not threading.current_thread()]
            if not threads and not calls:
                break
            for thread in threads:
                thread.join(None if deadline is None else max(0, deadline - time.monotonic()))
            for event in calls:
                event.wait(None if deadline is None else max(0, deadline - time.monotonic()))
            if deadline is not None and time.monotonic() >= deadline:
                break
        with self.fence_lock:
            return (not self._children and not any(thread.is_alive() for thread in self._threads
                                                   if thread is not threading.current_thread()) and
                    not any(owner is not threading.current_thread() for owner, _ in self._process_calls))
