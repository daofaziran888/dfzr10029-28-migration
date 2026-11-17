#!/usr/bin/env bash
# 本地 E2E 包装：容器里没有 root，Chromium 需要的系统库解包在 /tmp/deps。
# 这里只在库目录存在时注入 LD_LIBRARY_PATH，正常环境可以直接跑 npx playwright test。
set -euo pipefail
cd "$(dirname "$0")"
EXTRA_LIBS=""
for dir in \
  /tmp/debs/rootnspr/usr/lib/aarch64-linux-gnu \
  /tmp/debs/rootnss/usr/lib/aarch64-linux-gnu \
  /tmp/deps/usr/lib/aarch64-linux-gnu \
  /tmp/deps/lib/aarch64-linux-gnu; do
  if [ -d "$dir" ]; then
    EXTRA_LIBS="${EXTRA_LIBS:+$EXTRA_LIBS:}$dir"
  fi
done
if [ -n "$EXTRA_LIBS" ]; then
  export LD_LIBRARY_PATH="${EXTRA_LIBS}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
fi
exec npx playwright test "$@"
