"""Disk patches for the Feiniu frontend player JS (A-scheme STRM transcode).

The browser talks straight to 8005; there is no 8666 proxy any more. The
plugin backend (5230) only answers two things the browser can reach: the
transcode list (GET /v/nas-enhance/transcode) and the HLS itself. These
patches teach the 8005 player to fetch that list and put the transcode
qualities in its own quality menu, next to the original direct link:

player bundle (74c956...js)
  S2   - the quality matcher skips every is_m3u8 entry; let it match a
         nas_transcode entry (so a chosen transcode quality actually wins
         over the original on reload).
  M3U8 - getM3u8Url wraps the quality URL into the Feiniu /wp/m3u8 worker,
         which this trim.media version does not implement (501 "Not
         Implemented, please update."). Return a nas_transcode HLS URL
         directly so hls.js loads it from the plugin instead.

app bundle (bf29a6...js)
  MENU    - the quality menu disables every is_m3u8 entry; let a
            nas_transcode entry stay enabled.
  IIFE    - after the stream response arrives, fetch the transcode list
            from the plugin (5 s of patience is NOT enforced; a dead plugin
            fails fast on LAN) and append the qualities to
            direct_link_qualities, caching them in window.__fnEnhanceTQ.
            The fetch never blocks playback: any failure is swallowed and
            the menu simply has no transcode entries.
  OSC     - onStreamDataChange rebuilds mediaData.quality from the stream
            data; re-append the cached transcode qualities first.
  REFRESH - the 30 s direct-link refresh re-fetches and replaces
            streamData; re-append the cached qualities there too.

Every patch is one exact string replacement, so applying twice is a no-op
(the needle is gone after the first pass) and reverting is the exact
inverse replacement. The original direct link stays the first quality, so
the default never flips to HLS.
"""

# The fetch JavaScript injected into the play-start stream request (app.js).
# It runs inside an async IIFE where `e` is the stream response
# ({code, data}) and `h` is the media info; both are in scope there.
APP_IIFE_FETCH = (
    "try{let g=h?.media_guid;if(g){let u=`${window.location.protocol}//"
    "${window.location.hostname}:5230/v/nas-enhance/transcode?guid="
    "${encodeURIComponent(g)}`,r=await fetch(u);if(r.ok){let d=await r.json();"
    "if(d&&d.ok&&Array.isArray(d.qualities)&&d.qualities.length){"
    "window.__fnEnhanceTQ=window.__fnEnhanceTQ||{};"
    "window.__fnEnhanceTQ[g]={qualities:d.qualities};"
    "let q=e.data.direct_link_qualities;if(Array.isArray(q))"
    "for(const tq of d.qualities){if(!q.some(x=>x&&x.nas_transcode&&"
    "x.resolution===tq.resolution))q.push(tq)}}}}}catch(_){}"
)

# Re-append cached transcode qualities; used after onStreamDataChange
# rebuilds the quality list (here `e` is the stream data, `t` the media
# info state, `n` the rebuilt quality list).
APP_OSC_APPEND = (
    "try{let q=window.__fnEnhanceTQ?.[t?.infoData?.media_guid];"
    "if(q&&Array.isArray(q.qualities)){for(const a of "
    "[e.direct_link_qualities,n]){if(Array.isArray(a))"
    "for(const tq of q.qualities){if(!a.some(x=>x&&x.nas_transcode&&"
    "x.resolution===tq.resolution))a.push(tq)}}}}catch(_){}"
)

# Same re-append for the 30 s direct-link refresh (`e` is the freshly
# fetched stream data, `t` the media info state).
APP_REFRESH_APPEND = (
    "try{let q=window.__fnEnhanceTQ?.[t?.infoData?.media_guid];"
    "if(q&&Array.isArray(q.qualities)&&Array.isArray(e.direct_link_qualities))"
    "for(const tq of q.qualities){if(!e.direct_link_qualities.some(x=>x&&"
    "x.nas_transcode&&x.resolution===tq.resolution))"
    "e.direct_link_qualities.push(tq)}}catch(_){}"
)

PLAYER_S2_NEEDLE = "let r=e=>e.is_m3u8?!1:e.resolution===t&&e.bitrate===n;"
PLAYER_S2_REPLACEMENT = "let r=e=>e.is_m3u8&&!e.nas_transcode?!1:e.resolution===t&&e.bitrate===n;"
PLAYER_S2_MARKER = "e.nas_transcode?!1"

PLAYER_M3U8_NEEDLE = (
    "getM3u8Url(e){let t=this.attachM3u8Params(e),"
    "n=new URLSearchParams({originalUrl:t});"
    "return`${this.m3u8Url}?${n.toString()}`}"
)
PLAYER_M3U8_REPLACEMENT = (
    "getM3u8Url(e){if(typeof e===`string`&&e.includes(`/v/nas-enhance/hls/`))"
    "return e;let t=this.attachM3u8Params(e),"
    "n=new URLSearchParams({originalUrl:t});"
    "return`${this.m3u8Url}?${n.toString()}`}"
)
PLAYER_M3U8_MARKER = "e.includes(`/v/nas-enhance/hls/`)"

PLAYER_PATCHES = (
    (PLAYER_S2_NEEDLE, PLAYER_S2_REPLACEMENT),
    (PLAYER_M3U8_NEEDLE, PLAYER_M3U8_REPLACEMENT),
)

APP_DISABLED_NEEDLE = "disabled:e.is_m3u8},e.bitrate+e.url"
APP_DISABLED_REPLACEMENT = "disabled:e.is_m3u8&&!e.nas_transcode},e.bitrate+e.url"
APP_DISABLED_MARKER = "e.is_m3u8&&!e.nas_transcode"

APP_IIFE_NEEDLE = (
    "if(e?.code!==0||!e?.data){i(e?.code===0||!e?sn.Unknown:e),_=!0;return}"
    "return e.data}"
)
APP_IIFE_REPLACEMENT = APP_IIFE_NEEDLE.replace(
    "return e.data}", APP_IIFE_FETCH + "return e.data}"
)

APP_OSC_NEEDLE = (
    "let n=ea(e,d(e)),r={...t.defaultMediaConfig,"
    "duration:e?.video_stream?.duration};"
)
APP_OSC_REPLACEMENT = APP_OSC_NEEDLE + APP_OSC_APPEND

APP_REFRESH_NEEDLE = (
    "let r=pe(t);r.streamData={...t.streamData,"
    "direct_link_qualities:e.direct_link_qualities,"
    "direct_link_audio_streams:e.direct_link_audio_streams},n(r)"
)
APP_REFRESH_REPLACEMENT = (
    "let r=pe(t);" + APP_REFRESH_APPEND +
    "r.streamData={...t.streamData,"
    "direct_link_qualities:e.direct_link_qualities,"
    "direct_link_audio_streams:e.direct_link_audio_streams},n(r)"
)

APP_VISIBLE_NEEDLE = "t?null:(0,Q.jsx)(`div`,{className:`h-full`,children:(0,Q.jsx)(Tf,{popperChildren:(0,Q.jsx)(Im,{})"
APP_VISIBLE_REPLACEMENT = "t&&!e?.streamData?.nas_transcode?null:(0,Q.jsx)(`div`,{className:`h-full`,children:(0,Q.jsx)(Tf,{popperChildren:(0,Q.jsx)(Im,{})"
APP_VISIBLE_MARKER = "nas_transcode?null"

APP_PATCHES = (
    (APP_DISABLED_NEEDLE, APP_DISABLED_REPLACEMENT),
    (APP_IIFE_NEEDLE, APP_IIFE_REPLACEMENT),
    (APP_OSC_NEEDLE, APP_OSC_REPLACEMENT),
    (APP_REFRESH_NEEDLE, APP_REFRESH_REPLACEMENT),
    (APP_VISIBLE_NEEDLE, APP_VISIBLE_REPLACEMENT),
)
APP_TQ_MARKER = "__fnEnhanceTQ"

# Needles/markers used to recognise each bundle on disk (patched or not).
PLAYER_ANCHORS = (PLAYER_S2_NEEDLE, PLAYER_M3U8_NEEDLE, PLAYER_S2_MARKER, PLAYER_M3U8_MARKER)
APP_ANCHORS = (
    APP_DISABLED_NEEDLE, APP_IIFE_NEEDLE, APP_OSC_NEEDLE, APP_REFRESH_NEEDLE,
    APP_TQ_MARKER, APP_DISABLED_MARKER, APP_VISIBLE_NEEDLE, APP_VISIBLE_MARKER,
)


def _text(source):
    return source.decode('utf-8') if isinstance(source, bytes) else source


def _apply(text, patches):
    out, found = text, False
    for needle, repl in patches:
        if repl in out:
            continue  # Already applied (append-style replacements keep the needle).
        if needle in out:
            out = out.replace(needle, repl, 1)
            found = True
    return out, found


def _revert(text, patches):
    out, found = text, False
    for needle, repl in patches:
        if repl in out:
            out = out.replace(repl, needle, 1)
            found = True
    return out, found


def patch_player_js(source):
    """Apply the S2 + getM3u8Url patches; None when the bundle has no anchor."""
    try:
        out, found = _apply(_text(source), PLAYER_PATCHES)
    except UnicodeDecodeError:
        return None
    return out.encode('utf-8') if found else None


def patch_app_js(source):
    """Apply the menu + fetch + re-append patches; None when no anchor."""
    try:
        out, found = _apply(_text(source), APP_PATCHES)
    except UnicodeDecodeError:
        return None
    return out.encode('utf-8') if found else None


def unpatch_player_js(source):
    """Exact inverse of patch_player_js (replacement -> needle)."""
    out, _ = _revert(_text(source), PLAYER_PATCHES)
    return out.encode('utf-8')


def unpatch_app_js(source):
    """Exact inverse of patch_app_js (replacement -> needle)."""
    out, _ = _revert(_text(source), APP_PATCHES)
    return out.encode('utf-8')
