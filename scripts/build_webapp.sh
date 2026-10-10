#!/usr/bin/env bash
# Сборка Mini App при деплое на Render (Build Command: после pip install).
# Node берём из окружения Render, если он не старше 22.12 (нужен Vite 8),
# иначе скачиваем официальный Node с nodejs.org.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ ! -f webapp/package.json ]; then
  echo "No webapp, nothing to build"
  exit 0
fi

NODE_VERSION="v24.21.0"
node_ok() {
  command -v node >/dev/null 2>&1 &&
    node -e 'const [major, minor] = process.versions.node.split(".").map(Number);
             process.exit(major > 22 || (major === 22 && minor >= 12) ? 0 : 1)'
}
if ! node_ok; then
  echo "Downloading Node $NODE_VERSION"
  curl -fsSL "https://nodejs.org/dist/$NODE_VERSION/node-$NODE_VERSION-linux-x64.tar.xz" |
    tar -xJ -C /tmp
  export PATH="/tmp/node-$NODE_VERSION-linux-x64/bin:$PATH"
fi
echo "Node $(node --version)"
npm --prefix webapp ci
npm --prefix webapp run build
