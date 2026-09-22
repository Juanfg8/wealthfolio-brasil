#!/usr/bin/env bash
#
# Pull one Wealthfolio snapshot off the server and copy it somewhere the server
# cannot reach. Closes the gap between the backups that exist today (created by
# hand, or automatically before a database migration, and always written to
# `backups/` on the SAME volume as the live database) and a real offsite copy
# that survives losing that volume.
#
# This script does not run on the server, holds no credentials, and never
# touches production configuration. It reads the password from stdin, uses the
# same session cookie the web UI uses, and writes only into $DEST.
#
#   Restore drill (do this at least once, or the backups are untested):
#     1. Run this script to fetch a snapshot.
#     2. On a scratch machine, start a server with a fresh WF_DB_PATH and the
#        SAME WF_SECRET_KEY, then stop it.
#     3. wealthfolio-server db restore <file>        # validates, prints summary
#        wealthfolio-server db restore <file> --yes  # confirms replacement
#     4. Start it, open the UI, check net worth against the source.
#   A snapshot taken from an encrypted database needs the original
#   WF_SECRET_KEY. A portable .wfbackup export needs only its own password and
#   restores onto any installation; prefer it for true disaster recovery.
#   See docs/self-host/backups.md.
#
# Usage:
#   WF_URL=https://your-host ./scripts/offsite-backup.sh /path/to/offsite/dir
#   WF_URL=... WF_REMOTE=s3:my-bucket/wealthfolio ./scripts/offsite-backup.sh /tmp/wf
#
#   WF_URL     base URL of the server (required)
#   WF_REMOTE  optional rclone remote; the snapshot is copied there after download
#
set -euo pipefail

DEST=${1:-}
: "${WF_URL:?set WF_URL to the server base URL, e.g. https://your-host}"
if [ -z "$DEST" ]; then
  echo "usage: WF_URL=https://host $0 <destination-directory>" >&2
  exit 2
fi
mkdir -p "$DEST"

JAR=$(mktemp -t wf-cookies.XXXXXX)
trap 'rm -f "$JAR"' EXIT

api() { curl -fsS --cookie "$JAR" --cookie-jar "$JAR" "$@"; }

# Log in only if the server actually requires a password. Read it from the
# terminal without echoing, and pass it as a file so it never reaches argv.
if api "$WF_URL/api/v1/auth/status" | grep -q '"requiresPassword":true'; then
  IFS= read -r -s -p 'Wealthfolio password: ' WF_PASSWORD
  printf '\n'
  BODY=$(mktemp -t wf-login.XXXXXX)
  trap 'rm -f "$JAR" "$BODY"' EXIT
  printf '{"password":%s}' "$(printf '%s' "$WF_PASSWORD" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')" >"$BODY"
  unset WF_PASSWORD
  api -X POST -H 'Content-Type: application/json' --data-binary "@$BODY" \
    "$WF_URL/api/v1/auth/login" >/dev/null
  rm -f "$BODY"
fi

echo "Creating a snapshot on the server..."
FILENAME=$(api -X POST "$WF_URL/api/v1/utilities/database/backup" |
  python3 -c 'import json,sys; print(json.load(sys.stdin)["filename"])')
echo "Downloading $FILENAME..."
api -o "$DEST/$FILENAME" \
  "$WF_URL/api/v1/utilities/database/backups/$FILENAME/download"

# A zero-byte or truncated download is worse than no backup, because it looks
# like one. Refuse to report success on an obviously empty file.
SIZE=$(wc -c <"$DEST/$FILENAME" | tr -d ' ')
if [ "$SIZE" -lt 4096 ]; then
  echo "Downloaded file is only $SIZE bytes; refusing to treat it as a backup." >&2
  exit 1
fi
echo "Saved $DEST/$FILENAME ($SIZE bytes)"

if [ -n "${WF_REMOTE:-}" ]; then
  echo "Copying to $WF_REMOTE..."
  rclone copy "$DEST/$FILENAME" "$WF_REMOTE"
  echo "Copied to $WF_REMOTE"
fi

echo "Done. This snapshot is NOT restore-tested until you run the drill above."
