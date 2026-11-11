"""Read-only projection of probed STRM resolutions."""
import re
import sqlite3
import threading
import time
from collections import OrderedDict
from pathlib import Path

MAX_IDS = 256
VALID_RESOLUTIONS = ('8k', '4k', '1080p', '1080i', '720p', '720i', '480p', '480i')


def validate_ids(ids):
    if not isinstance(ids, list) or not ids or len(ids) > MAX_IDS:
        raise ValueError('无效的影片查询数量')
    if any(not isinstance(gid, str) or not re.fullmatch('[a-f0-9]{32}', gid) for gid in ids):
        raise ValueError('无效的影片编号')
    return list(dict.fromkeys(ids))


def stream_resolution(row):
    try:
        width, height = int(row['width']), int(row['height'])
    except (ValueError, TypeError):
        return None
    if not 0 < width <= 16384 or not 0 < height <= 16384:
        return None
    if not row['codec_name'] or row['status'] != 1 or row['ext1'] == -1:
        return None
    if width >= 7680:
        return '8k'
    # Dimensions are observed stream facts, never filename/title hints.
    # Derive from width directly: stored resolution_type may predate the
    # corrected 1080p threshold (1440-wide streams were written as 720p).
    if width >= 3840:
        return '4k'
    if width >= 1400:
        return '1080p'
    if width >= 1280:
        return '720p'
    if width >= 640:
        return '480p'
    return None


def lookup_resolutions(db, ids):
    ids = validate_ids(ids)
    placeholders = ','.join('(?)' for _ in ids)
    query = '''
        WITH RECURSIVE requested(guid) AS (VALUES ''' + placeholders + '''),
        descendants(root, guid) AS (
            SELECT requested.guid, item.guid FROM requested
            JOIN item ON item.guid = requested.guid
            UNION
            SELECT descendants.root, child.guid FROM descendants
            JOIN item parent ON parent.guid = descendants.guid
            JOIN item child ON child.parent_guid = parent.guid
            WHERE parent.type IN ('TV', 'Season')
        )
        SELECT descendants.root, streams.codec_name, streams.width, streams.height,
               streams.resolution_type, streams.status, streams.ext1
        FROM descendants
        JOIN item_media media ON media.item_guid = descendants.guid
        JOIN media_stream streams ON streams.media_guid = media.guid
        WHERE lower(media.path) LIKE '%.strm' AND streams.codec_type = 'video'
    '''
    result = {gid: set() for gid in ids}
    for row in db.execute(query, ids):
        resolution = stream_resolution(row)
        if resolution:
            result[row['root']].add(resolution)
    order = {name: i for i, name in enumerate(VALID_RESOLUTIONS)}
    return {gid: sorted(values, key=order.get) for gid, values in result.items()}


class ResolutionMetadata:
    def __init__(self, path, enabled, clock=time.monotonic):
        self.path = Path(path)
        self.enabled = enabled
        self.clock = clock
        self.cache = OrderedDict()
        self.lock = threading.Lock()
        self.generation = 0

    def invalidate(self):
        with self.lock:
            self.cache.clear()
            self.generation += 1

    def query(self, ids):
        ids = validate_ids(ids)
        if not self.enabled():
            return {'enabled': False, 'resolutions': {}}
        now = self.clock()
        result, missing = {}, []
        with self.lock:
            generation = self.generation
            for gid in ids:
                cached = self.cache.get(gid)
                if cached and cached[0] > now:
                    result[gid] = cached[1][:]
                    self.cache.move_to_end(gid)
                else:
                    missing.append(gid)
        if missing:
            db = sqlite3.connect(self.path.resolve().as_uri() + '?mode=ro', uri=True, timeout=.3)
            try:
                db.row_factory = sqlite3.Row
                db.execute('PRAGMA query_only=ON')
                deadline = time.monotonic() + .8
                db.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
                fresh = lookup_resolutions(db, missing)
            finally:
                db.close()
            result.update(fresh)
            with self.lock:
                if generation == self.generation:
                    for gid, values in fresh.items():
                        self.cache[gid] = (self.clock() + (30 if values else 5), values[:])
                        self.cache.move_to_end(gid)
                    while len(self.cache) > 2048:
                        self.cache.popitem(last=False)
        # A concurrent settings change must take effect without waiting for cache expiry.
        if not self.enabled():
            return {'enabled': False, 'resolutions': {}}
        return {'enabled': True, 'resolutions': {gid: result[gid] for gid in ids}}
