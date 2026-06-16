#!/usr/bin/env bash
#
# capture_ios_screenshots.sh — App Store iPhone screenshot capture.
#
# Drives an iOS Simulator to produce the 4 core App Store screenshots with:
#   • screenshot mode ON (generated placeholder covers, no third-party album
#     art — App Store Guideline 5.2.1), via the -screenshotMode launch arg;
#   • a clean 9:41 / full-signal / full-battery status bar;
#   • deep links that open each screen ALREADY scrolled to the right spot
#     (approachnote://song/{id}?screenshot=featured|recordings), so no manual
#     scrolling is needed.
#
# Output: marketing/iPhone screens/0N_*.png at 1320x2868 (iPhone 17 Pro Max,
# App Store Connect's 6.9" requirement).
#
# Usage:
#   marketing/scripts/capture_ios_screenshots.sh            # build, then capture
#   marketing/scripts/capture_ios_screenshots.sh --no-build # use installed app
#
set -euo pipefail

# ---------------------------------------------------------------------------
# Config — edit these to change device, target content, or timing.
# ---------------------------------------------------------------------------
DEVICE_NAME="${DEVICE_NAME:-iPhone 17 Pro Max}"   # 6.9" → 1320x2868
BUNDLE_ID="com.approachnote.ios"
EXPECTED_W=1320
EXPECTED_H=2868

# Content shown in each shot. These are production UUIDs used previously; swap
# them for any well-populated standard / artist. (Covers are generated in
# screenshot mode, so the album is only about title + recordings content.)
SONG_FEATURED_ID="2c152c65-135f-4931-9336-aa6be2a6e6c1"   # 02 — Featured Recordings carousel
SONG_RECORDINGS_ID="f9c4fa68-498b-4c1e-bb15-04be2c5b4537" # 03 — All Recordings list
ARTIST_ID="171d6d17-3b26-4bc6-9858-1a98bb5ab1a6"          # 04 — Artist detail
LIST_LETTER="${LIST_LETTER:-K}"                            # 01 — songs list pre-scroll (blank = top)

LOAD_WAIT="${LOAD_WAIT:-6}"   # seconds to wait for network load + in-app scroll

# ---------------------------------------------------------------------------
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DEST="$REPO/marketing/iPhone screens"
TMP="$(mktemp -d)"
export DEVELOPER_DIR="${DEVELOPER_DIR:-/Applications/Xcode.app/Contents/Developer}"
DO_BUILD=1
[ "${1:-}" = "--no-build" ] && DO_BUILD=0

log() { printf '\033[1;34m▸ %s\033[0m\n' "$*"; }

# ---------------------------------------------------------------------------
# Resolve + boot the simulator (by UDID, so we never rely on an ambiguous
# "booted" when multiple sims are up).
# ---------------------------------------------------------------------------
UDID="$(xcrun simctl list devices available | grep -F "$DEVICE_NAME (" | head -1 | grep -oE '[0-9A-Fa-f-]{36}' | head -1 || true)"
if [ -z "$UDID" ]; then
  echo "ERROR: no available simulator named '$DEVICE_NAME'. Available 6.9\" devices:" >&2
  xcrun simctl list devices available | grep -E 'iPhone 1[5-9] Pro Max' >&2 || true
  exit 1
fi
log "Device: $DEVICE_NAME ($UDID)"

if ! xcrun simctl list devices | grep -F "$UDID" | grep -q "(Booted)"; then
  log "Booting simulator…"
  xcrun simctl boot "$UDID"
fi
xcrun simctl bootstatus "$UDID" >/dev/null

# ---------------------------------------------------------------------------
# Build + install (unless --no-build).
# ---------------------------------------------------------------------------
if [ "$DO_BUILD" = 1 ]; then
  log "Building app for simulator…"
  DERIVED="$TMP/DerivedData"
  xcodebuild -project "$REPO/apps/ApproachNote.xcodeproj" \
    -scheme ApproachNote \
    -destination "id=$UDID" \
    -configuration Debug \
    -derivedDataPath "$DERIVED" \
    CODE_SIGNING_ALLOWED=NO \
    build >/dev/null
  APP="$DERIVED/Build/Products/Debug-iphonesimulator/ApproachNote.app"
  log "Installing $(basename "$APP")…"
  xcrun simctl install "$UDID" "$APP"
fi

# ---------------------------------------------------------------------------
# Clean App Store status bar (persists across app launches until cleared).
# ---------------------------------------------------------------------------
log "Setting clean status bar (9:41, full signal/battery)…"
xcrun simctl status_bar "$UDID" override \
  --time "9:41" \
  --dataNetwork wifi --wifiMode active --wifiBars 3 \
  --cellularMode active --cellularBars 4 \
  --batteryState charged --batteryLevel 100

# Launch args: screenshot mode on, onboarding suppressed, optional list letter.
LAUNCH_ARGS=(-screenshotMode YES -hasCompletedOnboarding YES)
[ -n "$LIST_LETTER" ] && LAUNCH_ARGS+=(-screenshotListLetter "$LIST_LETTER")

# capture <out-name> [deep-link-url]
# Cold-launches the app each time (avoids sheet stacking), optionally opens a
# deep link, waits for load+scroll, then screenshots.
capture() {
  local name="$1" url="${2:-}"
  log "Capturing $name…"
  xcrun simctl terminate "$UDID" "$BUNDLE_ID" >/dev/null 2>&1 || true
  xcrun simctl launch "$UDID" "$BUNDLE_ID" "${LAUNCH_ARGS[@]}" >/dev/null
  sleep 2
  if [ -n "$url" ]; then
    xcrun simctl openurl "$UDID" "$url"
  fi
  sleep "$LOAD_WAIT"
  xcrun simctl io "$UDID" screenshot "$TMP/$name.png" >/dev/null 2>&1
}

capture "01_songlist"
capture "02_songdetails" "approachnote://song/$SONG_FEATURED_ID?screenshot=featured"
capture "03_recordings"  "approachnote://song/$SONG_RECORDINGS_ID?screenshot=recordings"
capture "04_artist"      "approachnote://artist/$ARTIST_ID"

# ---------------------------------------------------------------------------
# Reset status bar and place the finals.
# ---------------------------------------------------------------------------
xcrun simctl status_bar "$UDID" clear || true
xcrun simctl terminate "$UDID" "$BUNDLE_ID" >/dev/null 2>&1 || true

mkdir -p "$DEST"
log "Saving to: $DEST"
for n in 01_songlist 02_songdetails 03_recordings 04_artist; do
  cp "$TMP/$n.png" "$DEST/$n.png"
  w=$(sips -g pixelWidth  "$DEST/$n.png" | awk '/pixelWidth/{print $2}')
  h=$(sips -g pixelHeight "$DEST/$n.png" | awk '/pixelHeight/{print $2}')
  flag=""
  { [ "$w" != "$EXPECTED_W" ] || [ "$h" != "$EXPECTED_H" ]; } && flag="  ⚠️ expected ${EXPECTED_W}x${EXPECTED_H}"
  printf '  %-18s %sx%s%s\n' "$n.png" "$w" "$h" "$flag"
done

rm -rf "$TMP"
log "Done. Review the 4 PNGs, then upload to App Store Connect."
