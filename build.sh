#!/bin/bash
# 构建 fpk：在仓库根目录运行 ./build.sh
set -e
cd "$(dirname "$0")"
APPNAME=$(grep '^appname=' manifest | cut -d= -f2)
VER=$(grep '^version=' manifest | cut -d= -f2)
OUT="${APPNAME}-${VER}.fpk"

rm -rf build
mkdir -p build/app build/fpk
cp -r app/server app/ui build/app/

tar czf build/app.tgz -C build/app .
cp manifest ICON.PNG ICON_256.PNG LICENSE build/fpk/
cp -r config cmd build/fpk/
cp build/app.tgz build/fpk/app.tgz

tar czf "$OUT" -C build/fpk .
echo "built: $OUT"
tar tzf "$OUT"
