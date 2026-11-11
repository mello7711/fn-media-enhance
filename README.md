# 飞牛影视增强

飞牛影视（trim.media）的第三方增强插件，面向 **STRM 片库**（夸克网盘、LitePan 直链等）。补全 STRM 元数据、给首页海报加分辨率角标、在播放器里提供 rkmpp 硬件转码画质菜单，并隐藏无头像演员。

> 反代入口 `:8666`，原版 `:8005` / `:5666` 入口保持原样、不受影响。

> **平台要求**：ARM（Rockchip **RK3566 / RK3588** 等带 rkmpp 的 SoC）。硬件转码（1080/720/480）依赖 Rockchip MPP；非 Rockchip 的 ARM 设备上转码档不会出现，其余功能（补全/角标/演员隐藏）不受影响。

## 功能

| 功能 | 说明 | 生效入口 |
|---|---|---|
| STRM 信息补全 | 用 ffprobe 探测 STRM（优先 FUSE 原文件，回落到直链），把视频/音频/字幕流写入飞牛数据库；跳过封面图流（attached_pic / mjpeg / png 等），不会产生重复按钮 | 8005 与 8666 都生效（写库） |
| 首页分辨率角标 | 反代注入列表接口响应，海报右下角显示 `1080p` / `4K` | 8666 |
| 播放器画质菜单 | 播放 STRM 时提供 原画 / 1080p / 720p / 480p（rkmpp 硬件转码，HLS 按需切片） | 8666 |
| 演员隐藏 | 前端补丁，隐藏无头像演员 | 8666（及磁盘补丁生效的入口） |
| 自动补全 | 每 30 分钟扫描一次，新入库 STRM 自动补全 | 后台 |

## 架构

- **sidecar（`:5230`）**：Python 后端。写库补全、HLS 转码（ffmpeg + Rockchip rkmpp）、前端磁盘补丁（幂等，禁用自动还原）、反代生命周期管理。
- **反代（`:8666`）**：Go 反代（源自 Ponphil/LitePan）。反代飞牛影视，向列表接口注入分辨率、向插件回源补丁版前端 bundle、透传播放接口并注入转码画质。
- **反代生命周期由插件自管**：配置页总开关 `enabled` 关闭时杀掉反代，开启时拉起；崩溃 60 秒巡检自愈；插件退出连带清理。无独立 systemd 服务。

## 安装

1. 应用中心 → 手动安装 → 选择 `fn-media-enhance-generic-1.0.3.fpk`（ARM 平台）。
2. 应用中心启用「飞牛影视增强」。
3. 配置页：`http://<NAS>:5230/enhance/`（补全开关、间隔、默认分辨率、演员隐藏）。
4. 影视入口使用 `http://<NAS>:8666/v`（角标与画质菜单在此入口生效）。

## 已知限制

- **字幕**：网页播放器只支持文本字幕（SRT/VTT/ASS）。PGS 等位图字幕无法在网页端渲染；本机 ffmpeg 的 libass 不支持 PGS 烧入，原生转码接口（`/wp/m3u8`）在 trim 0.9.8 上为 501 未实现。需要字幕请给片源配外挂文本字幕。
- **转码档范围**：仅 `hevc / h264 / vp9 / av1` 且非 HDR/杜比视界的片源会出现转码档；其余编码只有原画。
- **直链要求**：STRM 内必须是 http(s) 直链且支持 Range 请求（转码 seek 依赖）。
- 首次切换某个转码档需等待 8~13 秒（服务端编码第一个 24 秒窗口），之后边播边预编。

## v1.0.3 修复

- 8666 黑屏：反代回源补丁版前端资产的路由补齐（`/v/assets/<bundle>`）。
- 播放失败「未知错误 / 无效的签名」：`/v/api/v1/stream` 透传，全量转发请求头并保留原始 Host。
- 画质菜单不显示：可见性补丁并入 `player_patch.py`（`t?null:` → `t&&!e?.streamData?.nas_transcode?null:`），新安装自动生效、禁用自动还原。
- 转码出片失败：`scale_rkrga` 显式 `format=nv12`（修复 `-38 Function not implemented`）。
- 详情页重复「1080P SDR」按钮：fill 跳过 `attached_pic` 与图片编码。
- 反代生命周期从独立 systemd 改为插件内自管。

## 目录结构

```
manifest            # fnOS 应用清单
cmd/main            # 应用启停脚本（框架调用）
config/             # privilege / resource
app/server/         # Python 后端 + enhance-proxy 二进制
app/ui/             # 管理页面（预构建）
build.sh            # 打包脚本：./build.sh → *.fpk
```

## 构建

```bash
./build.sh   # 生成 fn-media-enhance-generic-<version>.fpk
```

## 许可

见 [LICENSE](LICENSE)：Python 插件与管理 UI 为 MIT；Go 反代组件衍生自 Ponphil/LitePan，沿用 PolyForm Noncommercial 1.0.0（仅限非商业用途）。
