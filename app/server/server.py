"""Personal fnOS STRM metadata and on-demand Rockchip HLS extension.

Backend on 0.0.0.0:5230 (the browser on 8005 reaches it directly; no
reverse proxy): writes STRM metadata straight into the trim.media database
and patches the Feiniu frontend JS on disk for actor hiding and the STRM
transcode menu (originals backed up under state/actor-js-backup and
state/player-js-backup). Transcode is A-scheme: the player fetches the HLS
list from GET /v/nas-enhance/transcode and appends the qualities to its
own quality menu; the original direct link stays first and default.
"""
import base64
import concurrent.futures
import hashlib
import hmac
import ipaddress
import json
import math
import mimetypes
import os
from pathlib import Path
import re
import secrets
import shutil
import sqlite3
import subprocess
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import urllib.error
import urllib.request
from urllib.parse import urlsplit, parse_qs

from resolution_metadata import ResolutionMetadata
from actor_visibility import actor_asset, ACTOR_ANCHOR, ACTOR_FILTER
from player_patch import (
    patch_player_js, unpatch_player_js, patch_app_js, unpatch_app_js,
    PLAYER_S2_MARKER, PLAYER_M3U8_MARKER, PLAYER_ANCHORS,
    APP_TQ_MARKER, APP_DISABLED_MARKER, APP_ANCHORS,
)
from plugin_lifecycle import OwnershipLease
from task_runtime import TaskRuntime, TaskError, TaskCancelled, classify_failure
import signal
from contextlib import contextmanager

ROOT = Path(__file__).resolve().parent
_state_env = os.environ.get('FN_ENHANCE_STATE')
if not _state_env and os.environ.get('APP_NAME'):
    _state_env = '/usr/local/apps/@appdata/' + os.environ['APP_NAME'] + '/state'
STATE = Path(_state_env or (ROOT / 'state'))
STATE.mkdir(exist_ok=True, mode=0o700)
CACHE = STATE / 'hls'
CACHE.mkdir(exist_ok=True)
DB = '/usr/local/apps/@appdata/trim.media/database/trimmedia.db'
ASSETS = Path('/usr/local/apps/@appcenter/trim.media/static/assets')
# 配置页（Vue SPA，v1.0.2 同款构建）由 5230 后端在 /enhance/ 直出，不再走 8666 反代。
# 自用插件，不做管理员鉴权：所有 5230 接口直接开放，能打开本页的局域网设备
# 即为管理员；HLS 票据仍 guard 播放内容（SECRET 密钥签名）。
UI_DIST = ROOT.parent / 'ui' / 'dist'
UI_CONTENT_TYPES = {'.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8'}

def ui_file_bytes(rel):
    """取打包内配置页文件；返回 (bytes, content-type)，越界/不存在/类型不符返回 None。"""
    base = UI_DIST.resolve()
    target = (base / rel).resolve()
    if target != base and base not in target.parents: return None
    if not target.is_file() or target.suffix not in UI_CONTENT_TYPES: return None
    data = target.read_bytes()
    return data, UI_CONTENT_TYPES[target.suffix]

SECRET_FILE = STATE / 'secret'
if not SECRET_FILE.exists():
    SECRET_FILE.write_bytes(secrets.token_bytes(32)); SECRET_FILE.chmod(0o600)
SECRET = SECRET_FILE.read_bytes()
CONFIG_FILE = STATE / 'config.json'
AUTOFILL_FILE = STATE / 'autofill.json'
CONFIG_DEFAULTS = {'enabled': True, 'auto_fill': False, 'fill_interval_min': 30, 'default_resolution': '720', 'hide_no_avatar_actors': False}

def validate_config(data, current):
    if not isinstance(data, dict) or set(data) - set(CONFIG_DEFAULTS):
        raise TaskError('INVALID_INPUT')
    candidate = dict(current)
    for key, value in data.items():
        if key in ('enabled', 'auto_fill', 'hide_no_avatar_actors'):
            if type(value) is not bool: raise TaskError('INVALID_INPUT')
        elif key == 'fill_interval_min':
            if type(value) is not int or not 30 <= value <= 1440: raise TaskError('INVALID_INPUT')
        elif key == 'default_resolution':
            if not isinstance(value, str) or value not in ('1080', '720', '480'): raise TaskError('INVALID_INPUT')
        candidate[key] = value
    return candidate

CONFIG = dict(CONFIG_DEFAULTS)
if CONFIG_FILE.exists():
    try: CONFIG = validate_config(json.loads(CONFIG_FILE.read_text(encoding='utf-8')), CONFIG)
    except (OSError, ValueError, TaskError): pass

def autofill_state():
    try:
        data = json.loads(AUTOFILL_FILE.read_text(encoding='utf-8'))
        last = data.get('last', 0)
        if type(last) not in (int, float) or not math.isfinite(last) or last < 0: last = 0
        return {'last': last, 'full_done': data.get('full_done') is True}
    except (OSError, ValueError, TypeError, AttributeError): return {'last': 0, 'full_done': False}

def atomic_state(path, data):
    tmp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    try:
        with os.fdopen(os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w', encoding='utf-8') as stream:
            json.dump(data, stream, ensure_ascii=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)

def autofill_save(x):
    atomic_state(AUTOFILL_FILE,x)
CONFIG_LOCK = threading.RLock()
OWNERSHIP = OwnershipLease()
RUNTIME = TaskRuntime(STATE/'fill-last.json', OWNERSHIP.valid)
RESOLUTION_METADATA = ResolutionMetadata(DB, lambda: bool(CONFIG['auto_fill']))
JOBS = {}
LOCK = threading.RLock()
ENCODER = threading.Semaphore(1)
POOL = concurrent.futures.ThreadPoolExecutor(max_workers=3)
SEG = 4
WINDOW = 6
MAX_CACHE = 2 * 1024**3
HOP = {'connection', 'transfer-encoding', 'content-length', 'content-encoding', 'keep-alive', 'upgrade'}
PRESETS = {'1080': (1920,1080,4000000), '720': (1280,720,2500000), '480': (854,480,1200000)}

def connect(ro=False):
    d=sqlite3.connect('file:'+DB+'?mode=ro',uri=True,timeout=15) if ro else sqlite3.connect(DB,timeout=15)
    d.row_factory=sqlite3.Row
    return d

def media(mid):
    if not re.fullmatch('[a-f0-9]{32}',mid): raise ValueError('无效媒体编号')
    with connect(True) as d:
        row=d.execute('select m.*,i.title from item_media m join item i on i.guid=m.item_guid where m.guid=?',(mid,)).fetchone()
    if not row or not row['path'].endswith('.strm'): raise ValueError('不是 STRM 媒体')
    p=Path(row['path'])
    if not p.is_file() or p.stat().st_size>65536: raise ValueError('STRM 文件不存在或过大')
    url=p.read_text(encoding='utf-8-sig').strip()
    if urlsplit(url).scheme not in ('http','https'): raise ValueError('仅支持 HTTP/HTTPS STRM')
    return dict(row),url

# 夸克专用：STRM 里的 LitePan 链接是 TV302 转码播放地址，ffprobe 只能拿到
# H.264 1440x604 + AAC，无字幕流。原文件在夸克 FUSE 挂载里，含完整流信息
#（HEVC 1920x804、AC3 音轨、PGS 字幕）。先探原文件，失败再回落探 URL。
QUARK_STRM_ROOT='/vol1/1000/video/strm/夸克'
QUARK_PAN_ROOT='/vol1/1000/video/pan/夸克'
QUARK_VIDEO_EXTS=('.mkv','.mp4','.avi','.mov','.ts','.m2ts','.rmvb','.wmv','.flv','.webm','.mpg','.mpeg','.3gp','.m4v')

def quark_original_path(strm_path):
    """STRM 路径 -> 夸克 FUSE 挂载里的原文件路径；找不到返回 None。"""
    try:
        p=Path(strm_path)
        if not str(p).startswith(QUARK_STRM_ROOT+'/'): return None
        rel=p.relative_to(QUARK_STRM_ROOT)
        stem=rel.stem
        # 候选 1：同相对目录；候选 2：中间插一层“影视剧”
        #（strm 里“电影/外语电影”，原文件是“影视剧/电影/外语电影”）
        for parent in (Path(QUARK_PAN_ROOT)/rel.parent, Path(QUARK_PAN_ROOT)/'影视剧'/rel.parent):
            for ext in QUARK_VIDEO_EXTS:
                cand=parent/(stem+ext)
                if cand.is_file(): return str(cand)
        # 候选 3：按文件名在挂载里搜一次（单条 find，15 秒兜底）
        cond=[]
        for ext in QUARK_VIDEO_EXTS:
            cond += ['-name', stem+ext, '-o']
        cond = cond[:-1]
        try:
            r=subprocess.run(['find',QUARK_PAN_ROOT,'-type','f','(']+cond+[')','-print','-quit'],capture_output=True,text=True,timeout=15)
        except Exception:
            return None
        hit=(r.stdout or '').strip().splitlines()
        return hit[0] if hit else None
    except Exception:
        return None

def probe(mid,ctx=None):
    RUNTIME.check()
    if ctx:ctx.stage('validating')
    try:row,url=media(mid)
    except ValueError:raise TaskError('INVALID_MEDIA')
    if ctx:ctx.stage('probing')
    target=quark_original_path(row.get('path','') if isinstance(row,dict) else '') or url
    p=RUNTIME.run_process(['ffprobe','-v','error','-rw_timeout','15000000','-analyzeduration','5000000','-probesize','5000000','-show_streams','-show_format','-of','json',target],timeout=45,ctx=ctx,check=False)
    if p.returncode and target!=url:
        # 探原文件失败，回落探 LitePan URL（通用版行为）
        p=RUNTIME.run_process(['ffprobe','-v','error','-rw_timeout','15000000','-analyzeduration','5000000','-probesize','5000000','-show_streams','-show_format','-of','json',url],timeout=45,ctx=ctx,check=False)
    if p.returncode:raise classify_failure(stderr=p.stderr)
    try:info=json.loads(p.stdout)
    except (ValueError,UnicodeError):raise TaskError('INVALID_MEDIA')
    streams=info.get('streams') if isinstance(info,dict) else None
    if not isinstance(streams,list) or any(not isinstance(s,dict) for s in streams): raise TaskError('INVALID_MEDIA')
    if not any(s.get('codec_type')=='video' and type(s.get('width')) is int and s['width']>0 for s in streams): raise TaskError('NO_VIDEO')
    if not isinstance(info.get('format',{}),dict): raise TaskError('INVALID_MEDIA')
    info.setdefault('format',{})
    if ctx:ctx.check()
    return info

@contextmanager
def fill_transaction(d,ctx):
    if ctx:
        with ctx.commit_guard(d):yield
    else:
        with RUNTIME.fence_lock:
            RUNTIME.check()
            try:
                yield
                RUNTIME.check()
                d.commit()
            except BaseException:
                d.rollback()
                raise

def fill(mid,ctx=None):
    info=probe(mid,ctx)
    if ctx:ctx.stage('backing_up')
    folder=STATE/'backups'/f'{int(time.time())}-{mid}-{uuid.uuid4().hex}'
    folder.mkdir(parents=True,mode=0o700)
    with connect() as d, fill_transaction(d,ctx):
        # Back up just this media's rows. Restoring the whole database would erase unrelated changes.
        d.execute('begin immediate')
        original=[dict(x) for x in d.execute('select * from media_stream where media_guid=?',(mid,))]
        (folder/'streams.json').write_text(json.dumps({'media_guid':mid,'rows':original},ensure_ascii=False),encoding='utf-8')
        cols=[x[1] for x in d.execute('pragma table_info(media_stream)')]
        template=original[0] if original else {k:None for k in cols}
        old_ids={(s['codec_type'],s['index']):s['guid'] for s in original}
        new=[];now=int(time.time()*1000)
        for s in info['streams']:
            typ=s.get('codec_type')
            if typ not in ('video','audio','subtitle'): continue
            if typ=='video' and (s.get('disposition',{}).get('attached_pic') or s.get('codec_name') in ('mjpeg','png','bmp','gif','webp','tiff','pict')): continue
            # Initialize fields independently; audio/subtitle must not inherit video fields.
            v={k: (0 if isinstance(template.get(k), (int,float)) else '') for k in cols}
            for k in ('is_bluray','ext1','status','dv_profile'): v[k]=0
            v.update(guid=old_ids.get((typ,s['index']),uuid.uuid4().hex),media_guid=mid,status=1,create_time=now,update_time=now)
            for k in ('codec_name','codec_type','profile','width','height','coded_width','coded_height','display_aspect_ratio','pix_fmt','color_range','level','color_space','color_transfer','color_primaries','refs','r_frame_rate','avg_frame_rate','time_base','start_pts','start_time','duration_pts','channels','sample_rate','bits_per_raw_sample','channel_layout','index'):
                if k in s and k in cols: v[k]=s[k]
            tags=s.get('tags',{});dis=s.get('disposition',{})
            v.update(title=tags.get('title',''),language=tags.get('language',''),is_default=dis.get('default',0),forced=dis.get('forced',0),duration=int(float(s.get('duration',info['format'].get('duration',0)))),bps=int(s.get('bit_rate',tags.get('BPS',0)) or 0),container_format=info['format'].get('format_name','').split(',')[0],progressive=int(s.get('field_order')=='progressive'))
            pix=s.get('pix_fmt','')
            v['bit_depth']=int(s.get('bits_per_raw_sample',0) or (10 if '10' in pix or 'p010' in pix else 8 if pix else 0))
            if typ=='video':
                # Native stream resolution_type describes playback quality, not the list badge.
                w=s.get('width',0);v['resolution_type']='4k' if w>=3800 else '1080p' if w>=1400 else '720p' if w>=1200 else '480p'
                ct=s.get('color_transfer')
                v['color_range_type']='DolbyVision' if s.get('dv_profile') else 'HDR10' if ct=='smpte2084' else 'HLG' if ct=='arib-std-b67' else 'SDR'
            new.append(v)
        if ctx:ctx.stage('writing')
        RUNTIME.check()
        d.execute('delete from media_stream where media_guid=?',(mid,))
        for v in new:
            keys=list(v);d.execute('insert into media_stream ('+','.join('"'+k+'"' for k in keys)+') values ('+','.join('?' for _ in keys)+')',[v[k] for k in keys])
        # Quality contains playback quality choices; it is not the native list badge source.
        vt=[s for s in info['streams'] if s.get('codec_type')=='video']
        if vt:
            v0=vt[0];w=int(v0.get('width') or 0)
            res='2160' if w>=3800 else '1080' if w>=1900 else '720' if w>=1200 else '480'
            _tags=v0.get('tags') or {}
            entry={'bitrate':int(v0.get('bit_rate') or _tags.get('BPS') or 0),'progressive':True,'resolution':res}
            content=json.dumps([entry],separators=(',',':'))
            now_ms=int(time.time()*1000)
            d.execute("delete from media_property where media_guid=? and category='Quality'",(mid,))
            d.execute("insert into media_property (media_guid,category,content,update_time,create_time) values (?,?,?,?,?)",(mid,'Quality',content,now_ms,now_ms))
        if ctx:ctx.check()
        RUNTIME.check()
    RESOLUTION_METADATA.invalidate()
    return {'streams':len(new),'backup':str(folder),'video':next(s['codec_name'] for s in info['streams'] if s.get('codec_type')=='video')}

def ticket(mid,duration,codec):
    raw=json.dumps({'m':mid,'d':duration,'c':codec,'e':int(time.time())+12*3600},separators=(',',':')).encode()
    b=base64.urlsafe_b64encode(raw).decode().rstrip('=')
    return b+'.'+hmac.new(SECRET,b.encode(),hashlib.sha256).hexdigest()

def unticket(t):
    b,s=t.split('.',1)
    if not hmac.compare_digest(s,hmac.new(SECRET,b.encode(),hashlib.sha256).hexdigest()): raise ValueError('播放凭据无效')
    obj=json.loads(base64.urlsafe_b64decode(b+'='*(-len(b)%4)))
    if obj['e']<time.time(): raise ValueError('播放凭据已过期，请重新打开影片')
    if not 0<float(obj['d'])<7*86400: raise ValueError('无效时长')
    return obj

def transcode_qualities(mid, host):
    """转码菜单接口：读库里已补的 video 流信息，返回 1080/720/480 HLS 地址。

    只读库（fill 已写好才给）；库里没有 video 信息就回不合规，不现场 probe。
    资格：duration>0、codec 在 hevc/h264/vp9/av1、无 DV、无 HDR。高度上限同样：res 超过原高 1.4 倍就跳过。HLS 地址用请求
    Host 拼绝对地址，8005 页面的 hls.js 可直接加载；原直链永远排在第一位、
    只是菜单里的一个可选项，默认不动它。
    """
    try:
        media(mid)
    except ValueError:
        return {'ok': False, 'reason': 'not_strm'}
    if not CONFIG.get('enabled'):
        return {'ok': False, 'reason': 'disabled'}
    with connect(True) as d:
        row = d.execute("select codec_name,height,duration,dv_profile,color_transfer from media_stream where media_guid=? and codec_type='video' and status=1 order by rowid limit 1", (mid,)).fetchone()
    if row is None:
        return {'ok': False, 'reason': 'not_filled'}
    codec = row['codec_name']
    duration = row['duration']
    height = row['height'] or 0
    if not duration or codec not in ('hevc', 'h264', 'vp9', 'av1') or row['dv_profile'] or row['color_transfer'] in ('smpte2084', 'arib-std-b67'):
        return {'ok': False, 'reason': 'ineligible'}
    t = ticket(mid, duration, codec)
    netloc = re.sub(r'[^A-Za-z0-9.\-:\[\]]', '', host or '')
    if not netloc:
        netloc = '127.0.0.1:5230'
    elif ':' not in netloc:
        netloc += ':5230'
    qualities = []
    for res, (_, _, br) in PRESETS.items():
        if int(res) > int(height) * 1.4:
            continue
        qualities.append({'resolution': res, 'bitrate': br, 'progressive': True, 'is_m3u8': True, 'nas_transcode': True, 'url': f'http://{netloc}/v/nas-enhance/hls/{t}/{res}/index.m3u8'})
    return {'ok': True, 'guid': mid, 'qualities': qualities}

def playlist(t,res,obj):
    count=math.ceil(float(obj['d'])/SEG)
    lines=['#EXTM3U','#EXT-X-VERSION:3','#EXT-X-TARGETDURATION:4','#EXT-X-MEDIA-SEQUENCE:0','#EXT-X-PLAYLIST-TYPE:VOD','#EXT-X-INDEPENDENT-SEGMENTS']
    for i in range(count):
        if i and i%WINDOW==0: lines.append('#EXT-X-DISCONTINUITY')
        lines.extend([f'#EXTINF:{min(SEG,float(obj["d"])-i*SEG):.6f},',f'{i}.ts'])
    lines.append('#EXT-X-ENDLIST')
    return '\n'.join(lines)+'\n'

def window_key(obj,res,idx): return hashlib.sha256(f'{obj["m"]}:{res}:{idx//WINDOW}'.encode()).hexdigest()

TRANSCODE_CANCEL=threading.Event()
TRANSCODE_STATS_FILE = STATE / 'transcode-stats.json'
def _load_transcode_stats():
    try:
        data = json.loads(TRANSCODE_STATS_FILE.read_text(encoding='utf-8'))
        windows = data.get('windows', 0)
        return {'windows': windows} if type(windows) is int and windows >= 0 else {'windows': 0}
    except (OSError, ValueError, TypeError, AttributeError):
        return {'windows': 0}
TRANSCODE_STATS = _load_transcode_stats()
TRANSCODE_STATS_LOCK = threading.Lock()

def note_transcode_window():
    """转码累计计数：每成功编完一个 24 秒窗口记 1 次（持久化到 state）。

    只证明转码功能跑过、供配置页横幅显示；计数失败永不影响转码本身。
    """
    try:
        with TRANSCODE_STATS_LOCK:
            TRANSCODE_STATS['windows'] = TRANSCODE_STATS.get('windows', 0) + 1
            atomic_state(TRANSCODE_STATS_FILE, dict(TRANSCODE_STATS))
    except Exception:
        pass
@contextmanager
def encoder_slot(cancel_event):
    while not ENCODER.acquire(timeout=.1):RUNTIME.check(cancel_event)
    try:
        RUNTIME.check(cancel_event)
        yield
    finally:ENCODER.release()

def encode_window(obj,res,idx,folder,job,cancel_event):
    with encoder_slot(cancel_event):
        job['state']='encoding';job['started']=time.time()
        _,url=media(obj['m']);start=(idx//WINDOW)*WINDOW;offset=start*SEG
        width,height,bitrate=PRESETS[res]
        cmd=['ffmpeg','-nostdin','-hide_banner','-loglevel','warning','-rw_timeout','15000000','-ss',str(offset)]
        decoder={'hevc':'hevc_rkmpp','h264':'h264_rkmpp','vp9':'vp9_rkmpp','av1':'av1_rkmpp'}.get(obj['c'])
        if not decoder: raise ValueError('该视频编码尚未启用硬件转码')
        cmd+=['-hwaccel','rkmpp','-hwaccel_output_format','drm_prime','-c:v',decoder,'-i',url,'-t',str(min(SEG*WINDOW,float(obj['d'])-offset)),'-map','0:v:0','-map','0:a:0?','-sn','-vf',f'scale_rkrga=w={width}:h={height}:format=nv12:force_original_aspect_ratio=decrease:force_divisible_by=2','-c:v','h264_rkmpp','-b:v',str(bitrate),'-maxrate',str(bitrate),'-bufsize',str(bitrate*2),'-force_key_frames','expr:gte(t,n_forced*4)','-c:a','aac','-ac','2','-b:a','128k','-output_ts_offset',str(offset),'-f','hls','-hls_time',str(SEG),'-hls_list_size','0','-hls_flags','independent_segments+temp_file','-start_number',str(start),'-hls_segment_filename',str(folder/'%d.ts'),'-y',str(folder/'local.m3u8')]
        p=RUNTIME.run_process(cmd,timeout=110,cancel_event=cancel_event,check=False)
        if p.returncode:raise classify_failure(stderr=p.stderr)
        note_transcode_window()
        job['state']='done'

def request_segment(obj,res,idx):
    with LOCK: cancel_event=TRANSCODE_CANCEL
    RUNTIME.check(cancel_event)
    if idx<0 or idx>=math.ceil(float(obj['d'])/SEG): raise ValueError('分片超出范围')
    key=window_key(obj,res,idx);folder=CACHE/key;target=folder/f'{idx}.ts'
    with LOCK:
        job=JOBS.get(key)
        if target.exists():
            os.utime(folder,None);return target
        if not job or job.get('state') in ('error','cancelled'):
            if sum(x.get('state') in ('queued','encoding') for x in JOBS.values())>=3: raise ValueError('转码繁忙，请稍后再试')
            folder.mkdir(exist_ok=True);job={'state':'queued','media':obj['m'],'resolution':res,'window':idx//WINDOW,'created':time.time()};JOBS[key]=job
            def run():
                try: encode_window(obj,res,idx,folder,job,cancel_event)
                except TaskCancelled:job.update(state='cancelled',error='任务已停止')
                except Exception as e:job.update(state='error',error=classify_failure(e).message)
            try: POOL.submit(run)
            except RuntimeError:
                job.update(state='cancelled',error='任务已停止')
                raise TaskCancelled('SHUTDOWN') from None
    deadline=time.monotonic()+100
    while time.monotonic()<deadline:
        RUNTIME.check(cancel_event)
        if job.get('state')=='cancelled':raise TaskCancelled('CANCELLED')
        if target.exists(): return target
        if job.get('state')=='error': raise ValueError(job['error'])
        if job.get('state')=='done': raise ValueError('目标分片没有生成')
        time.sleep(.15)
    raise ValueError('等待转码超时')

def libraries():
    with connect(True) as d:
        rows=d.execute("select m.guid,i.title,m.path,s.codec_name,s.width,s.height,s.duration,s.ext1 from item_media m join item i on i.guid=m.item_guid left join media_stream s on s.media_guid=m.guid and s.codec_type='video' where m.path like '%.strm' order by i.title").fetchall()
    # 一个媒体可能有多条 video 流（如 mjpeg 封面流也写成了 video），左连接会把同一
    # 条目重复算多次；总数必须按媒体去重，否则“共 4 部”对不上实际的 3 条 STRM。
    seen={}
    for r in rows:
        if r['guid'] not in seen:
            seen[r['guid']]={'guid':r['guid'],'title':r['title'],'filename':Path(r['path']).name,'codec':r['codec_name'],'width':r['width'],'height':r['height'],'duration':r['duration'],'complete':bool(r['codec_name'] and r['width'] and r['ext1']!=-1)}
    return list(seen.values())

def disk_strm_scan():
    """扫描磁盘上的 .strm（按 mediadb_config 的 dir_list），与库内条目对照。

    库里只有 3 条 STRM，但磁盘上有 13270 个——飞牛媒体库根本没把它们扫进去，
    插件读库再准也只能看到 3 条。fill() 写 media_stream 需要 item_media 的 guid，
    不在库里的 STRM 无法直接写库；先把“磁盘 vs 入库”的缺口如实暴露给 UI，
    让用户知道补全覆盖不到的原因是入库缺失（飞牛扫描问题），不是插件没跑。
    返回 {'disk_total': N, 'in_library': M, 'not_in_library': N-M, 'dirs': [...]}。
    """
    try:
        with connect(True) as d:
            dirs=set()
            for (raw,) in d.execute("select dir_list from mediadb_config"):
                try:
                    for p in json.loads(raw):
                        if isinstance(p,str) and p: dirs.add(p)
                except (ValueError, TypeError): pass
            in_lib={r[0] for r in d.execute("select path from item_media where path like '%.strm'")}
        # 除已配置的库目录外，再兜底扫 /vol1/1000/video/strm/ 根目录：
        # 13267 个 STRM 在 strm/光/（115 挂载）下，但没有任何库配置了 光/ 目录，
        # 只扫配置目录会漏掉它们，误报成“磁盘只有 3 个”。
        for extra in ('/vol1/1000/video/strm', '/vol1/1000/video/local'):
            dirs.add(extra)
        total=0; seen=set()
        for base in sorted(dirs):
            p=Path(base)
            if not p.is_dir(): continue
            for fp in p.rglob('*.strm'):
                try:
                    s=str(fp)
                except Exception: continue
                if s in seen: continue
                seen.add(s); total+=1
                if total>200000: break
            if total>200000: break
        not_in=len([s for s in seen if s not in in_lib])
        return {'disk_total':total,'in_library':len(in_lib),'not_in_library':not_in,'dirs':sorted(dirs)}
    except Exception:
        return {'disk_total':None,'in_library':None,'not_in_library':None,'dirs':[]}

def fill_error_cn(code):
    """失败码 → 中文原因；HTTP_xxx 返回 HTTP 状态说明。

    映射表放函数内做局部常量：测试以 AST 隔离加载 server.py，只取函数/类
    定义，模块级常量在隔离命名空间里取不到。
    """
    error_cn = {
        'INVALID_MEDIA': '探测不到有效的媒体信息',
        'NO_VIDEO': '没有可识别的视频流',
        'PROCESS_TIMEOUT': '探测超时（45 秒）',
        'PROCESS_FAILED': 'ffprobe 执行失败',
        'DATABASE_BUSY': '数据库正忙，稍后重试',
        'DATABASE_ERROR': '写数据库失败',
        'NOT_FOUND': 'STRM 文件或 ffprobe 找不到',
        'PERMISSION_DENIED': '文件无权访问',
        'DISK_FULL': '磁盘空间不足',
        'IO_ERROR': '文件或进程操作失败',
        'BUSY': '已有补全批次在运行',
        'CANCELLED': '任务被取消',
        'INTERNAL_ERROR': '内部错误',
    }
    if isinstance(code, str):
        m = re.fullmatch(r'HTTP_([45]\d\d)', code)
        if m:
            return '媒体源返回 HTTP ' + m.group(1)
        if code in error_cn:
            return error_cn[code]
    return str(code) if code else '未知原因'

def fill_progress_snapshot():
    """补全进度快照（/api/fn-enhance/status 的 fill_progress 和 /fill-progress 页面共用）。

    total/completed/pending 是库内 STRM 的计数（已补全 = 库里已写入视频流信息）；
    pending_items 是未补清单（最多 200 条），每条若在最近一次补全
    运行里失败过，附上 last_error（失败码 + 中文原因）；run 是最近一次运行的原始
    快照（state/done/total/success/failed/current/errors）。库读不到时计数为 None。
    """
    snap = RUNTIME.snapshot()
    with CONFIG_LOCK:
        config = dict(CONFIG)
        st = autofill_state()
    try:
        rows = libraries()
    except Exception:
        rows = None
    try:
        disk = disk_strm_scan()
    except Exception:
        disk = {'disk_total': None, 'in_library': None, 'not_in_library': None, 'dirs': []}
    interval = config['fill_interval_min']
    last = st['last']
    base = {'run': snap, 'interval_min': interval, 'auto_fill': bool(config['auto_fill']),
            'last_run_at': int(last * 1000) if last else None, 'full_done': st['full_done'],
            'next_run_at': int((last + interval * 60) * 1000) if last and config['auto_fill'] else None,
            'disk': disk}
    if rows is None:
        base.update(total=None, completed=None, pending=None, pending_items=[], pending_omitted=0, total_strm=None)
        return base
    pending_rows = [r for r in rows if not r['complete']]
    errors = {}
    for e in snap.get('errors', []) or []:
        if isinstance(e, dict) and e.get('guid'):
            errors[e['guid']] = {'code': e.get('code'), 'reason': fill_error_cn(e.get('code'))}
    pending_list_limit = 200  # 未补清单单次最多列出的条数（局部常量，见 fill_error_cn 的注释）
    items = []
    for r in pending_rows[:pending_list_limit]:
        item = {'guid': r['guid'], 'title': r['title'], 'filename': r['filename']}
        err = errors.get(r['guid'])
        if err:
            item['last_error'] = err
        items.append(item)
    base.update(total=len(rows), completed=len(rows) - len(pending_rows), pending=len(pending_rows),
                pending_items=items, pending_omitted=max(0, len(pending_rows) - pending_list_limit), total_strm=len(rows))
    return base

def render_fill_progress_page(prog):
    """补全进度页（服务端直出 HTML，30 秒自动刷新；只读展示，不做任何写操作）。"""
    import html
    def esc(v):
        return html.escape('' if v is None else str(v), quote=True)
    def num(v):
        return '—' if v is None else str(v)
    def fmt(ms):
        if not ms:
            return '—'
        return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ms / 1000))
    head = ('<!DOCTYPE html><html lang="zh-CN"><head><meta charset="utf-8">'
            '<meta http-equiv="refresh" content="30">'
            '<meta name="viewport" content="width=device-width, initial-scale=1">'
            '<title>STRM 补全进度</title>'
            '<style>body{font-family:sans-serif;max-width:900px;margin:2em auto;padding:0 1em}'
            'table{border-collapse:collapse;width:100%}th,td{border:1px solid #ccc;padding:4px 8px;text-align:left}'
            'th{background:#f0f0f0}.ok{color:green}.bad{color:red}</style></head><body>'
            '<h1>STRM 补全进度</h1><p>页面每 30 秒自动刷新；只读展示。开始补全仍走补全接口。</p>')
    foot = '</body></html>'
    if not isinstance(prog, dict):
        return head + '<p>补全状态暂时读不到（媒体库读取失败），请稍后刷新。</p>' + foot
    run = prog.get('run') or {}
    disk = prog.get('disk') or {}
    state = run.get('state') or 'idle'
    cur = run.get('current') or {}
    if cur:
        current = esc(cur.get('title') or cur.get('filename') or cur.get('guid')) + '（' + esc(cur.get('filename') or '') + '，' + esc(cur.get('stage') or '') + '）'
    else:
        current = '空闲（没有正在补的条目）'
    started = fmt(run.get('started_at') * 1000 if run.get('started_at') else None)
    finished = fmt(run.get('finished_at') * 1000 if run.get('finished_at') else None)
    items = prog.get('pending_items') or []
    err_rows = ''
    for e in run.get('errors', []) or []:
        if not isinstance(e, dict):
            continue
        err_rows += ('<tr><td>' + esc(e.get('title') or '') + '</td><td>' + esc(e.get('filename') or '') + '</td>'
                     '<td>' + esc(e.get('code') or '') + '</td><td>' + esc(fill_error_cn(e.get('code'))) + '</td></tr>')
    pend_rows = ''
    for it in items:
        err = it.get('last_error') or {}
        reason = (esc(err.get('code') or '') + '：' + esc(err.get('reason') or '')) if err else '尚未尝试'
        pend_rows += ('<tr><td>' + esc(it.get('title') or '') + '</td><td>' + esc(it.get('filename') or '') + '</td>'
                      '<td>' + reason + '</td></tr>')
    omitted = prog.get('pending_omitted') or 0
    return (head
        + '<h2>概要</h2><table>'
        + '<tr><th>库内 STRM 总数</th><td>' + num(prog.get('total')) + '</td></tr>'
        + '<tr><th>已补全</th><td class="ok">' + num(prog.get('completed')) + '</td></tr>'
        + '<tr><th>未补全</th><td class="bad">' + num(prog.get('pending')) + '</td></tr>'
        + '<tr><th>磁盘 STRM 总数 / 已入库</th><td>' + num(disk.get('disk_total')) + ' / ' + num(disk.get('in_library')) + '（只有入库的才能补全）</td></tr>'
        + '<tr><th>补全间隔</th><td>每 ' + num(prog.get('interval_min')) + ' 分钟（自动补全' + ('开启' if prog.get('auto_fill') else '关闭') + '）</td></tr>'
        + '<tr><th>全部完成</th><td>' + ('是' if prog.get('full_done') else '否') + '</td></tr>'
        + '<tr><th>上次补全时间</th><td>' + fmt(prog.get('last_run_at')) + '</td></tr>'
        + '<tr><th>下次计划补全</th><td>' + fmt(prog.get('next_run_at')) + '</td></tr>'
        + '</table>'
        + '<h2>本次运行</h2><table>'
        + '<tr><th>状态</th><td>' + esc(state) + '（来源：' + esc(run.get('source') or '') + '）</td></tr>'
        + '<tr><th>正在补全</th><td>' + current + '</td></tr>'
        + '<tr><th>已处理 / 成功 / 失败</th><td>' + num(run.get('done')) + ' / ' + num(run.get('success')) + ' / ' + num(run.get('failed')) + '</td></tr>'
        + '<tr><th>开始 / 结束</th><td>' + started + ' / ' + finished + '</td></tr>'
        + '</table>'
        + '<h2>本次失败（' + str(len(run.get('errors') or [])) + ' 条' + ('，另有 ' + str(run.get('errors_omitted') or 0) + ' 条未列出' if run.get('errors_omitted') else '') + '）</h2>'
        + ('<table><tr><th>标题</th><th>文件</th><th>失败码</th><th>原因</th></tr>' + err_rows + '</table>' if err_rows else '<p>本次运行没有失败。</p>')
        + '<h2>未补清单（' + num(prog.get('pending')) + ' 条' + ('，以下仅列前 ' + str(len(items)) + ' 条，另有 ' + str(omitted) + ' 条未列出' if omitted else '') + '）</h2>'
        + ('<table><tr><th>标题</th><th>文件</th><th>上次失败原因</th></tr>' + pend_rows + '</table>' if pend_rows else '<p>没有未补的条目。</p>')
        + '<p>已补全 = 库里已写入视频流信息。未补原因只保留最近一次运行的失败记录；从未尝试过的条目显示“尚未尝试”。</p>'
        + foot)

AUTO_PENDING = False

def batch_fill(ids=None, source='manual', on_complete=None):
    RUNTIME.check()
    rows = list({r['guid']: r for r in libraries()}.values())
    allowed = {r['guid']: r for r in rows}
    if ids is None:
        selected = [r for r in rows if not r['complete']]
    else:
        if not isinstance(ids, list) or len(ids) > 10000 or any(not isinstance(x, str) or x not in allowed for x in ids):
            raise TaskError('INVALID_INPUT')
        selected = [allowed[x] for x in dict.fromkeys(ids)]
    if len(selected) > 10000: raise TaskError('INVALID_INPUT')
    return RUNTIME.start(selected, fill, source=source, on_complete=on_complete)

# ---- enhance-proxy lifecycle: 8666 entry managed by this plugin (no boot service) ----
PROXY_BIN = Path(__file__).resolve().parent / 'enhance-proxy'
PROXY_ENV = {
    'FN_ENHANCE_STATE': str(STATE),
    'FN_ENHANCE_UPSTREAM': 'http://127.0.0.1:5230',
    'FNOS_UPSTREAM': 'http://127.0.0.1:8005',
    'LISTEN': ':8666',
    'FN_ENHANCE_ASSET': 'bf29a647181e25c03987ab8cc8f81a1a-DB2rIeMx.js',
    'FN_ENHANCE_PLAYER_ASSET': '74c95604043427f0bee1d0e16bfa53af-DZn1lJtt.js',
    'APP_SOCK': '/usr/local/apps/@appcenter/fn-media-enhance-quark/app.sock',
    'APP_NAME': 'fn-media-enhance-quark',
}
PROXY_PROCESS = None
PROXY_LOG = None
PROXY_LOCK = threading.Lock()

def _proxy_alive():
    return PROXY_PROCESS is not None and PROXY_PROCESS.poll() is None

def stop_proxy():
    global PROXY_PROCESS
    with PROXY_LOCK:
        p = PROXY_PROCESS
        PROXY_PROCESS = None
    if p is not None and p.poll() is None:
        try: p.terminate()
        except Exception: pass
        try: p.wait(timeout=5)
        except Exception:
            try: p.kill()
            except Exception: pass

def sync_proxy():
    global PROXY_PROCESS, PROXY_LOG
    if CONFIG.get('enabled'):
        if _proxy_alive(): return
        if not PROXY_BIN.exists():
            print('enhance-proxy missing, skip', flush=True); return
        env = dict(os.environ); env.update(PROXY_ENV)
        try: env['FN_ENHANCE_TOKEN'] = Path('/usr/local/apps/@appdata/fn-media-enhance-quark/token').read_text(encoding='utf-8').strip()
        except Exception: env['FN_ENHANCE_TOKEN'] = secrets.token_hex(24)
        with PROXY_LOCK:
            if _proxy_alive(): return
            try:
                try: Path(PROXY_ENV['APP_SOCK']).unlink()
                except Exception: pass
                if PROXY_LOG is None:
                    try: PROXY_LOG = open(str(STATE.parent / 'enhance.log'), 'ab')
                    except Exception: PROXY_LOG = subprocess.DEVNULL
                PROXY_PROCESS = subprocess.Popen([str(PROXY_BIN)], cwd=str(PROXY_BIN.parent), env=env, stdout=PROXY_LOG, stderr=subprocess.STDOUT, start_new_session=True)
                print('enhance-proxy spawned pid=' + str(PROXY_PROCESS.pid) + ' on :8666', flush=True)
            except Exception as e:
                print('enhance-proxy spawn failed: ' + str(e)[:120], flush=True)
    else:
        if _proxy_alive():
            stop_proxy(); print('enhance-proxy stopped (disabled)', flush=True)

def update_config(data):
    with CONFIG_LOCK:
        candidate = validate_config(data, CONFIG)
        atomic_state(CONFIG_FILE, candidate)  # Validate and persist everything before publishing.
        CONFIG.clear()
        CONFIG.update(candidate)
        if not candidate['auto_fill']:
            RUNTIME.cancel(auto_only=True)
        try:
            ensure_actor_patch()
            ensure_player_patch()
        except Exception:
            pass
        try:
            sync_proxy()
        except Exception:
            pass
        return dict(CONFIG)

def schedule_fill():
    global AUTO_PENDING
    with CONFIG_LOCK:
        if not CONFIG['auto_fill'] or AUTO_PENDING: return
        RUNTIME.check()
        if RUNTIME.snapshot()['state'] == 'running': return
        st = autofill_state()
        interval = CONFIG['fill_interval_min'] * 60
        if time.time() - st['last'] < interval: return
        rows = list({r['guid']: r for r in libraries()}.values())
        ids = [r['guid'] for r in rows if not st['full_done'] or not r['complete']]
        if not ids:
            autofill_save({'last': time.time(), 'full_done': st['full_done']})
            return
        AUTO_PENDING = True  # Keep the reservation until the terminal callback finishes.
        def completed(result):
            global AUTO_PENDING
            with CONFIG_LOCK:
                try:
                    autofill_save({'last': time.time(), 'full_done': st['full_done'] or result['state'] == 'done'})
                finally:
                    AUTO_PENDING = False
        try:
            batch_fill(ids, source='auto', on_complete=completed)
        except Exception:
            AUTO_PENDING = False
            autofill_save({'last': time.time(), 'full_done': st['full_done']})
            raise

ACTOR_PATCH = {'file': None, 'patched': False, 'available': False, 'detail': ''}

def ensure_actor_patch():
    """磁盘补丁：把演员过滤 + STRM 转码（S2/getM3u8Url）写回飞牛前端播放器 JS。

    一份文件、一次原子写入、一份原始备份。想要的内容直接从当前文件
    算：要的打上、不要的按原样还原（补丁都是一对一的字符串替换，正反都
    可逆），所以藏演员开/关、插件开/关任意组合都不会互相抹掉对方。
    失败永不致命——返回 ACTOR_PATCH 状态快照，绝不抛错。浏览器按
    Last-Modified 启发式缓存该 JS，改后最坏约一天旧缓存，需硬刷新一次。
    """
    marker = 'e.profilePath!==null&&typeof e.profilePath==='
    state = globals().get('ACTOR_PATCH')
    if not isinstance(state, dict):
        state = {'file': None, 'patched': False, 'available': False, 'detail': ''}
        globals()['ACTOR_PATCH'] = state
    try:
        anchor = globals().get('ACTOR_ANCHOR')
        patch_fn = globals().get('actor_asset')
        actor_filter = globals().get('ACTOR_FILTER', '')
        player_patch_fn = globals().get('patch_player_js')
        player_unpatch_fn = globals().get('unpatch_player_js')
        anchors = globals().get('PLAYER_ANCHORS') or ()
        if not isinstance(anchor, str) or not anchor or not callable(patch_fn):
            state.update(file=None, patched=False, available=False, detail='actor patch unavailable')
            return dict(state)
        assets = globals().get('ASSETS')
        if assets is None:
            assets = Path('/usr/local/apps/@appcenter/trim.media/static/assets')
        assets = Path(assets)
        if not assets.is_dir():
            state.update(file=None, patched=False, available=False, detail='assets dir missing')
            return dict(state)
        target = None
        content = None
        for p in sorted(assets.glob('*.js')):
            try:
                text = p.read_text(encoding='utf-8')
            except (OSError, UnicodeError):
                continue
            if text.count(anchor) == 1 or marker in text or any(a and a in text for a in anchors):
                target = p
                content = text
                break
        if target is None:
            state.update(file=None, patched=False, available=False, detail='actor anchor not found')
            return dict(state)
        with CONFIG_LOCK:
            want_actor = bool(CONFIG.get('hide_no_avatar_actors'))
            want_player = bool(CONFIG.get('enabled'))
        desired = content
        if want_actor:
            if marker not in desired:
                new = patch_fn(desired.encode('utf-8'))
                if new is None:
                    state.update(file=target.name, patched=False, available=True, detail='actor patch failed')
                    return dict(state)
                desired = new.decode('utf-8')
        elif marker in desired and actor_filter:
            # 正向替换的原样还原：filter 文本是 anchor 的一部分替换。
            desired = desired.replace(anchor[:-3] + actor_filter + 'let', anchor, 1)
        if callable(player_patch_fn) and callable(player_unpatch_fn):
            if want_player:
                new = player_patch_fn(desired.encode('utf-8'))
                if new is not None:
                    desired = new.decode('utf-8')
            else:
                desired = player_unpatch_fn(desired.encode('utf-8')).decode('utf-8')
        if desired == content:
            state.update(file=target.name, patched=marker in desired, available=True, detail='already ok')
            return dict(state)
        backup_dir = STATE / 'actor-js-backup'
        backup_dir.mkdir(parents=True, exist_ok=True)
        backup = backup_dir / (target.name + '.orig')
        if not backup.exists():
            backup.write_bytes(content.encode('utf-8'))
        tmp = target.with_name(target.name + '.patch-tmp-' + uuid.uuid4().hex)
        tmp.write_bytes(desired.encode('utf-8'))
        try:
            os.chmod(tmp, target.stat().st_mode)
        except OSError:
            pass
        os.replace(tmp, target)
        state.update(file=target.name, patched=marker in desired, available=True, detail='patched on disk')
        return dict(state)
    except Exception as e:
        try:
            state.update(file=state.get('file'), patched=False, available=False, detail=('error: ' + str(e))[:120])
        except Exception:
            pass
        return dict(state)

PLAYER_PATCH = {'file': None, 'patched': False, 'available': False, 'detail': ''}

def ensure_player_patch():
    """磁盘补丁：把 STRM 转码菜单注入（菜单禁用 + IIFE/OSC/刷新）写回飞牛前端 app JS。

    与 ensure_actor_patch 分工：这里只碰含转码锚点的 app.js 文件，两份文件
    各自独立备份、互不干扰。插件关掉就按原样还原。失败永不致命，返回
    PLAYER_PATCH 状态快照，绝不抛错。
    """
    state = globals().get('PLAYER_PATCH')
    if not isinstance(state, dict):
        state = {'file': None, 'patched': False, 'available': False, 'detail': ''}
        globals()['PLAYER_PATCH'] = state
    try:
        patch_fn = globals().get('patch_app_js')
        unpatch_fn = globals().get('unpatch_app_js')
        tq_marker = globals().get('APP_TQ_MARKER', '')
        dis_marker = globals().get('APP_DISABLED_MARKER', '')
        anchors = globals().get('APP_ANCHORS') or ()
        # 播放器 bundle（74c956…）不能误认：它被 ensure_actor_patch 打上 S2 补丁后，
        # 里面会出现 APP_DISABLED_MARKER 的子串（e.is_m3u8&&!e.nas_transcode），
        # 按字母排序又排在 app bundle 前面；不排除它，扫描会盯错文件、报假的 already ok。
        player_anchors = globals().get('PLAYER_ANCHORS') or ()
        if not callable(patch_fn) or not callable(unpatch_fn):
            state.update(file=None, patched=False, available=False, detail='player patch unavailable')
            return dict(state)
        assets = globals().get('ASSETS')
        if assets is None:
            assets = Path('/usr/local/apps/@appcenter/trim.media/static/assets')
        assets = Path(assets)
        if not assets.is_dir():
            state.update(file=None, patched=False, available=False, detail='assets dir missing')
            return dict(state)
        target = None
        content = None
        for p in sorted(assets.glob('*.js')):
            try:
                text = p.read_text(encoding='utf-8')
            except (OSError, UnicodeError):
                continue
            if any(a and a in text for a in player_anchors):
                continue
            if any(a and a in text for a in anchors):
                target = p
                content = text
                break
        if target is None:
            state.update(file=None, patched=False, available=False, detail='player anchor not found')
            return dict(state)
        with CONFIG_LOCK:
            want = bool(CONFIG.get('enabled'))
        desired = content
        if want:
            new = patch_fn(desired.encode('utf-8'))
            if new is not None:
                desired = new.decode('utf-8')
        else:
            desired = unpatch_fn(desired.encode('utf-8')).decode('utf-8')
        patched = bool(tq_marker and tq_marker in desired) or bool(dis_marker and dis_marker in desired)
        if desired == content:
            state.update(file=target.name, patched=patched, available=True, detail='already ok')
            return dict(state)
        backup_dir = STATE / 'player-js-backup'
        backup_dir.mkdir(parents=True, exist_ok=True)
        backup = backup_dir / (target.name + '.orig')
        if not backup.exists():
            backup.write_bytes(content.encode('utf-8'))
        tmp = target.with_name(target.name + '.patch-tmp-' + uuid.uuid4().hex)
        tmp.write_bytes(desired.encode('utf-8'))
        try:
            os.chmod(tmp, target.stat().st_mode)
        except OSError:
            pass
        os.replace(tmp, target)
        state.update(file=target.name, patched=patched, available=True, detail='patched on disk')
        return dict(state)
    except Exception as e:
        try:
            state.update(file=state.get('file'), patched=False, available=False, detail=('error: ' + str(e))[:120])
        except Exception:
            pass
        return dict(state)

class Handler(BaseHTTPRequestHandler):
    protocol_version='HTTP/1.1'
    def log_message(self,*args): pass  # Never log capability URLs or cookies.
    def body(self):
        size=int(self.headers.get('Content-Length',0))
        if size>1024*1024:raise ValueError('请求过大')
        return self.rfile.read(size)
    def send(self,status,data,ctype='application/json; charset=utf-8',cors=False):
        if isinstance(data,(dict,list)): data=json.dumps(data,ensure_ascii=False).encode()
        if isinstance(data,str):data=data.encode()
        self.send_response(status);self.send_header('Content-Type',ctype);self.send_header('Content-Length',str(len(data)));self.send_header('Cache-Control','no-store')
        if cors:self.send_header('Access-Control-Allow-Origin','*')
        self.end_headers()
        if self.command!='HEAD':self.wfile.write(data)
    def task_error(self, error):
        failure = classify_failure(error)
        status = 409 if failure.code in ('BUSY', 'CANCELLED') else 503 if failure.code in ('SHUTDOWN', 'OWNERSHIP_LOST') else 400 if failure.code in ('INVALID_INPUT', 'INVALID_MEDIA') else 502
        self.send(status, {'error': failure.message, 'code': failure.code})
    def do_HEAD(self):self.do_GET()
    def do_OPTIONS(self):
        # hls.js（8005 页）带 Play-Link/Authorization 自定义头拉 HLS 会先预检；
        # 凭据在 URL 票据里，不靠 Cookie，* 放行即可。
        try:
            p=urlsplit(self.path).path
            p=p.removeprefix('/app/fn-media-enhance-quark')
            if p.startswith('/v/nas-enhance/'):
                self.send_response(204)
                self.send_header('Access-Control-Allow-Origin','*')
                self.send_header('Access-Control-Allow-Methods','GET, OPTIONS')
                self.send_header('Access-Control-Allow-Headers','*')
                self.send_header('Access-Control-Max-Age','86400')
                self.send_header('Content-Length','0')
                self.end_headers()
                return
            self.send(404,{'error':'未找到接口'})
        except (BrokenPipeError,ConnectionResetError):
            pass
        except Exception:
            pass
    def do_GET(self):
        try:self.handle_get()
        except (BrokenPipeError,ConnectionResetError):pass
        except Exception as error:self.task_error(error)
    def handle_get(self):
        p=urlsplit(self.path).path
        p=p.removeprefix('/app/fn-media-enhance-quark')
        if p=='/v/api/v1/stream':
            # 8666 反代拦截的播放接口（GET 形态）：透传 trim 后端
            try:
                q=urlsplit(self.path).query
                req=urllib.request.Request('http://127.0.0.1:8005/v/api/v1/stream'+(('?'+q) if q else ''))
                for k,v in self.headers.items():
                    if k.lower() in HOP|{'host','accept-encoding'}: continue
                    req.add_header(k,v)
                req.add_unredirected_header('Host', self.headers.get('Host','127.0.0.1'))
                with urllib.request.urlopen(req, timeout=20) as r:
                    data=r.read(); ct=r.headers.get('Content-Type') or 'application/json'
                return self.send(200,data,ct,cors=True)
            except urllib.error.HTTPError as e:
                try: return self.send(e.code,e.read(),e.headers.get('Content-Type') or 'application/json',cors=True)
                except Exception: return self.send(e.code,{'error':'stream upstream error'})
            except Exception:
                return self.send(502,{'error':'stream proxy failed'})
        if p.startswith('/v/assets/'):
            # 8666 反代回源：供给补丁版 trim 前端 bundle（通用版同款机制）
            name=p.rsplit('/',1)[1]
            if name in (PROXY_ENV['FN_ENHANCE_ASSET'],PROXY_ENV['FN_ENHANCE_PLAYER_ASSET']):
                try:
                    fp=ASSETS/name
                    if fp.is_file(): return self.send(200,fp.read_bytes(),mimetypes.guess_type(str(fp))[0] or 'application/javascript',cors=True)
                except Exception: pass
        if '/hls/' in p:
            m=re.fullmatch(r'/v/nas-enhance/hls/([^/]+)/(1080|720|480)/(index\.m3u8|\d+\.ts)',p)
            if not m:raise ValueError('无效播放地址')
            t,res,name=m.groups();obj=unticket(t)
            RUNTIME.check(TRANSCODE_CANCEL)
            if name=='index.m3u8':return self.send(200,playlist(t,res,obj),'application/vnd.apple.mpegurl',cors=True)
            f=request_segment(obj,res,int(name[:-3]));return self.send(200,f.read_bytes(),'video/mp2t',cors=True)
        if p=='/v/nas-enhance/transcode':
            # 浏览器直接调：只返回票据保护的 HLS 地址。
            guid=parse_qs(urlsplit(self.path).query).get('guid',[''])[0]
            return self.send(200,transcode_qualities(guid,self.headers.get('Host','')),cors=True)
        if p=='/fill-progress':
            # 补全进度页：服务端直出 HTML，不需要鉴权；后端绑 0.0.0.0:5230，
            # 页面只读展示、不改任何状态。
            try: prog=fill_progress_snapshot()
            except Exception: prog=None
            return self.send(200,render_fill_progress_page(prog),'text/html; charset=utf-8')
        if p=='/enhance' or p=='/enhance/':
            # 配置页：打包的 Vue SPA 直出，不做鉴权。
            got=ui_file_bytes('index.html')
            if got is None:return self.send(404,{'error':'管理页面未找到'})
            data,ctype=got;return self.send(200,data,ctype)
        if p.startswith('/enhance/assets/'):
            got=ui_file_bytes(p.removeprefix('/enhance/'))
            if got is None:
                # 8666 反代回源补丁版 trim 前端资产：UI dist 没有则回落 trim 静态目录
                try:
                    fp=ASSETS/p.rsplit('/',1)[1]
                    if fp.is_file(): got=(fp.read_bytes(), mimetypes.guess_type(str(fp))[0] or 'application/javascript')
                except Exception: got=None
            if got is None:return self.send(404,{'error':'未找到接口'})
            data,ctype=got;return self.send(200,data,ctype)
        if p.startswith('/api/fn-enhance/'):
            suffix=p.removeprefix('/api/fn-enhance/')
            if suffix=='status':
                with LOCK:jobs=[{k:v for k,v in j.items() if k in ('state','media','resolution','window','created','started','error')} for j in JOBS.values()][-20:]
                try: prog=fill_progress_snapshot()
                except Exception: prog=None
                with CONFIG_LOCK:
                    config = dict(CONFIG)
                    st = autofill_state()
                return self.send(200,{'config':config,'jobs':jobs,'fill':RUNTIME.snapshot(),'presets':list(PRESETS),'actor_patch':dict(globals().get('ACTOR_PATCH') or {'file': None, 'patched': False, 'available': False, 'detail': ''}),
                    'player_patch':dict(globals().get('PLAYER_PATCH') or {'file': None, 'patched': False, 'available': False, 'detail': ''}),
                    'lifecycle':{'managed':OWNERSHIP.required,'active':OWNERSHIP.valid(),'active_children':RUNTIME.active_children,'shutting_down':RUNTIME.shutdown_event.is_set()},
                    'capabilities':{'hide_no_avatar_actors':bool((globals().get('ACTOR_PATCH') or {}).get('patched')),'transcode_menu':bool((globals().get('PLAYER_PATCH') or {}).get('patched'))},
                    'pending':(prog['pending'] if prog else None),'total_strm':(prog['total'] if prog else None),
                    'transcode_total':TRANSCODE_STATS.get('windows', 0),
                    'transcode_active':sum(1 for j in JOBS.values() if j.get('state') in ('queued','encoding')),
                    'disk_strm':(prog['disk'] if prog else {'disk_total':None,'in_library':None,'not_in_library':None,'dirs':[]}),
                    'fill_progress':prog,
                    'autofill':{'last':int(st['last']*1000),'full_done':st['full_done'],'interval':config['fill_interval_min']}})
            if suffix=='library':return self.send(200,libraries())
        self.send(404,{'error':'未找到接口'})
    def do_POST(self):
        try:
            p=urlsplit(self.path).path
            p=p.removeprefix('/app/fn-media-enhance-quark')
            if p=='/internal/fn-enhance/resolutions':
                if not ipaddress.ip_address(self.client_address[0]).is_loopback:
                    raise PermissionError('internal only')
                try:
                    length=int(self.headers.get('Content-Length','-1'))
                    if not 0<length<=32768 or self.headers.get('Transfer-Encoding'):
                        raise ValueError('bad length')
                    self.connection.settimeout(2)
                    data=json.loads(self.rfile.read(length))
                    if not isinstance(data,dict) or set(data)!={'ids'}:
                        raise ValueError('bad query')
                    result=RESOLUTION_METADATA.query(data['ids'])
                except (ValueError, TypeError, KeyError):
                    return self.send(400,{'error':'invalid query'})
                return self.send(200,result)
            if p=='/v/api/v1/stream':
                # 8666 反代拦截的播放接口：透传 trim 后端并注入转码画质（照抄通用版契约）
                try:
                    raw=self.body()
                    req=urllib.request.Request('http://127.0.0.1:8005/v/api/v1/stream', data=raw, method='POST')
                    for k,v in self.headers.items():
                        if k.lower() in HOP|{'host','accept-encoding'}: continue
                        req.add_header(k,v)
                    req.add_unredirected_header('Host', self.headers.get('Host','127.0.0.1'))
                    try:
                        r=urllib.request.urlopen(req, timeout=45); status=r.status; rh=r.headers; rbody=r.read()
                    except urllib.error.HTTPError as e:
                        status=e.code; rh=e.headers; rbody=e.read()
                    if 300<=status<400:
                        self.send_response(status)
                        for key,value in rh.items():
                            if key.lower() not in HOP: self.send_header(key,value)
                        self.send_header('Content-Length',str(len(rbody)))
                        self.end_headers()
                        if self.command!='HEAD': self.wfile.write(rbody)
                        return
                    try:
                        data=json.loads(rbody); obj=data.get('data')
                        if status<300 and data.get('code')==0 and obj and CONFIG.get('enabled'):
                            mid=json.loads(raw).get('media_guid','')
                            try:
                                media(mid); v=obj.get('video_stream',{}); duration=v.get('duration',0); codec=v.get('codec_name')
                                if duration and codec in ('hevc','h264','vp9','av1') and not v.get('dv_profile') and v.get('color_transfer') not in ('smpte2084','arib-std-b67'):
                                    t=ticket(mid,duration,codec); obj['nas_transcode']=True
                                    q=obj.get('direct_link_qualities',[])[:1]
                                    for res,(_,_,br) in PRESETS.items():
                                        if int(res)>int(v.get('height',0))*1.4: continue
                                        q.append({'resolution':res,'bitrate':br,'progressive':True,'is_m3u8':True,'nas_transcode':True,'url':f'/v/nas-enhance/hls/{t}/{res}/index.m3u8'})
                                    obj['direct_link_qualities']=q
                            except ValueError: pass
                        rbody=json.dumps(data,ensure_ascii=False).encode()
                    except Exception: pass
                    ct=(rh.get('Content-Type') if rh else None) or 'application/json'
                    return self.send(status,rbody,ct,cors=True)
                except Exception:
                    return self.send(502,{'error':'stream proxy failed'})
            body=self.body()
            data=json.loads(body or b'{}')
            if not isinstance(data,dict): raise TaskError('INVALID_INPUT')
            if p=='/api/fn-enhance/fill':
                if set(data)-{'ids'}: raise TaskError('INVALID_INPUT')
                batch_fill(data.get('ids'))
                return self.send(202,{'started':True})
            if p=='/api/fn-enhance/fill/stop':
                if set(data)-{'auto_only'} or type(data.get('auto_only',False)) is not bool: raise TaskError('INVALID_INPUT')
                cancelled=RUNTIME.cancel(auto_only=data.get('auto_only',False))
                return self.send(200,{'stopped':cancelled,'fill':RUNTIME.snapshot()})
            if p=='/api/fn-enhance/fill/clear':
                if data: raise TaskError('INVALID_INPUT')
                cleared=RUNTIME.clear_history()
                return self.send(200,{'cleared':cleared,'fill':RUNTIME.snapshot()})
            if p=='/api/fn-enhance/config':
                return self.send(200,update_config(data))
            if p=='/api/fn-enhance/stop':
                # Cancel currently owned transcodes without poisoning subsequent playback.
                RUNTIME.check()
                with LOCK:
                    previous=TRANSCODE_CANCEL
                    previous.set()
                    for job in JOBS.values():
                        if job.get('state')=='queued':job.update(state='cancelled',error='任务已停止')
                    globals()['TRANSCODE_CANCEL']=threading.Event()
                return self.send(200,{'stopped':True})
            self.send(404,{'error':'未找到接口'})
        except (BrokenPipeError,ConnectionResetError):pass
        except Exception as error:self.task_error(error)
STOP_EVENT = threading.Event()

def prune_orphans():
    """飞牛删除条目不级联清 media_property；备份目录同理。清理只删"指向不存在条目"的行/目录。"""
    with connect(True) as d:
        guids={r[0] for r in d.execute('select guid from item_media')}
        orphans=[r[0] for r in d.execute('select distinct media_guid from media_property') if r[0] not in guids]
    if orphans:
        conn=connect()
        with conn, fill_transaction(conn,None):
            conn.executemany('delete from media_property where media_guid=?',[(x,) for x in orphans])
    bk=STATE/'backups'
    if bk.is_dir():
        for p in list(bk.iterdir()):
            # 只清补全备份（<时间戳>-<guid>）；update-* 等其他目录绝不动。
            if p.is_dir() and re.fullmatch(r'\d{9,11}-[0-9a-f]{32}',p.name) and p.name.rsplit('-',1)[-1] not in guids:
                if STOP_EVENT.is_set(): return
                shutil.rmtree(p,ignore_errors=True)

def maintain():
    while not STOP_EVENT.wait(60):
        try:
            RUNTIME.check()
            sync_proxy()
            dirs=sorted([p for p in CACHE.iterdir() if p.is_dir()],key=lambda p:p.stat().st_mtime)
            total=sum(f.stat().st_size for p in dirs for f in p.iterdir() if f.is_file())
            for p in dirs:
                if STOP_EVENT.is_set(): return
                with LOCK: job=JOBS.get(p.name,{})
                if job.get('state') in ('queued','encoding'):continue
                if time.time()-p.stat().st_mtime>3600 or total>MAX_CACHE:
                    size=sum(f.stat().st_size for f in p.iterdir() if f.is_file());shutil.rmtree(p);total-=size
                    with LOCK:JOBS.pop(p.name,None)
            # Keep the most recent 100 per-media backups, then drop backups of removed items.
            bk=STATE/'backups'
            if bk.is_dir():
                bdirs=sorted([p for p in bk.iterdir() if p.is_dir()],key=lambda p:p.stat().st_mtime,reverse=True)
                for p in bdirs[100:]:
                    if STOP_EVENT.is_set(): return
                    shutil.rmtree(p,ignore_errors=True)
                prune_orphans()
        except TaskCancelled:
            STOP_EVENT.set()
            return
        except Exception:pass
        try:
            if not STOP_EVENT.is_set(): schedule_fill()
        except TaskCancelled:
            STOP_EVENT.set()
            return
        except Exception:pass

def run_server():
    http = serving = maintenance = None
    previous_signals = {}
    serving_failed = threading.Event()
    try:
        RUNTIME.check()  # A required lease must be valid before binding a socket.
        for signum in (signal.SIGTERM, signal.SIGINT):
            previous_signals[signum] = signal.signal(signum, lambda *_: STOP_EVENT.set())
        try:
            ensure_actor_patch()
            ensure_player_patch()
        except Exception as e:
            print('frontend patch skipped: ' + str(e)[:120], flush=True)
        http = ThreadingHTTPServer(('0.0.0.0',5230),Handler)
        def serve():
            try: http.serve_forever(poll_interval=.1)
            except Exception: serving_failed.set()
            finally: STOP_EVENT.set()
        serving = threading.Thread(target=serve, name='fn-enhance-http')
        serving.start()
        maintenance = threading.Thread(target=maintain, name='fn-enhance-maintain')
        maintenance.start()
        try:
            sync_proxy()
        except Exception as e:
            print('enhance-proxy start skipped: ' + str(e)[:120], flush=True)
        print('fn-enhance ready; 0.0.0.0:5230; actor_patch='+json.dumps(ACTOR_PATCH,ensure_ascii=False)+'; player_patch='+json.dumps(PLAYER_PATCH,ensure_ascii=False),flush=True)
        while not STOP_EVENT.wait(.25):
            if not OWNERSHIP.valid():
                STOP_EVENT.set()
                break
    finally:
        stop_proxy()
        STOP_EVENT.set()
        TRANSCODE_CANCEL.set()
        with LOCK:
            for job in JOBS.values():
                if job.get('state')=='queued': job.update(state='cancelled',error='任务已停止')
        try:
            # Cancel queued executor work; runtime reaps the already-owned children.
            POOL.shutdown(wait=False,cancel_futures=True)
            if not RUNTIME.shutdown(): raise TaskError('PROCESS_CLEANUP_FAILED')
            RUNTIME.wait()
        finally:
            try:
                POOL.shutdown(wait=True,cancel_futures=True)
            finally:
                if http is not None:
                    if serving is not None and serving.is_alive(): http.shutdown()
                    if serving is not None: serving.join()
                    http.server_close()
                if maintenance is not None: maintenance.join()
                for signum, previous in previous_signals.items(): signal.signal(signum,previous)
    if serving_failed.is_set(): raise TaskError('INTERNAL_ERROR')

if __name__=='__main__':
    run_server()
