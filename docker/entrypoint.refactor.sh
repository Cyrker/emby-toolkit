#!/bin/sh
set -eu

umask "${UMASK:-022}"
mkdir -p "${APP_DATA_DIR:-/config}"

exec "$@"
