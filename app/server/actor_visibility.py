"""Patch only the native cast renderer; leave API records and pagination intact."""
ACTOR_ANCHOR = 'UH=({parentClassName:e,routeType:t,persons:n=[],titleVisible:r=!0})=>{let'
# 隐藏无头像演员：只保留 profilePath 为非空字符串的 Actor。
# 旧版过滤器含 `!hasOwnProperty(profilePath)`，会把“根本没有 profilePath 属性”
# 的演员也保留下来——那正是没头像的演员，导致隐藏失效。现改为：只有
# profilePath 是非空字符串才保留，其余（缺失/null/非字符串/空串）一律隐藏。
ACTOR_FILTER = '''n=n.filter(e=>!e||e.job!==`Actor`||(e.profilePath!==null&&typeof e.profilePath===`string`&&e.profilePath.trim()!==``));'''


def actor_asset(source):
    text = source.decode('utf-8') if isinstance(source, bytes) else source
    if text.count(ACTOR_ANCHOR) != 1:
        return None
    return text.replace(ACTOR_ANCHOR, ACTOR_ANCHOR[:-3] + ACTOR_FILTER + 'let', 1).encode('utf-8')
