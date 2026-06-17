#!/usr/bin/env bash
#
# capture_ios_screenshots.sh — App Store iPhone screenshot capture.
#
# Drives an iOS Simulator to produce the 4 core App Store screenshots with:
#   • screenshot mode ON (generated placeholder covers, no third-party album
#     art — App Store Guideline 5.2.1), via the -screenshotMode launch arg;
#   • a clean 9:41 / full-signal / full-battery status bar;
#   • launch args that open each screen full-screen, ALREADY scrolled to the
#     right spot (-screenshotSongId/-screenshotArtistId/-screenshotAnchor), so
#     no manual scrolling is needed and it works the same on iPhone and iPad.
#
# Output (per profile):
#   iPhone → marketing/iPhone screens/0N_*.png  1320x2868 (iPhone 17 Pro Max, 6.9")
#   iPad   → marketing/iPad screens/0N_*.png    2064x2752 (iPad Pro 13-inch, 13")
#
# Usage:
#   marketing/scripts/capture_ios_screenshots.sh                 # iPhone, build
#   marketing/scripts/capture_ios_screenshots.sh --ipad          # iPad, build
#   marketing/scripts/capture_ios_screenshots.sh --ipad --no-build
#
set -euo pipefail

# ---------------------------------------------------------------------------
# Flags: --ipad / --iphone (default) select the device profile; --no-build
# reuses the already-installed app.
# ---------------------------------------------------------------------------
PROFILE="iphone"
DO_BUILD=1
for arg in "$@"; do
  case "$arg" in
    --ipad)     PROFILE="ipad" ;;
    --iphone)   PROFILE="iphone" ;;
    --no-build) DO_BUILD=0 ;;
    *) echo "Unknown option: $arg (use --ipad / --iphone / --no-build)" >&2; exit 2 ;;
  esac
done

# ---------------------------------------------------------------------------
# Config — edit these to change device, target content, or timing.
# ---------------------------------------------------------------------------
BUNDLE_ID="com.approachnote.ios"
if [ "$PROFILE" = "ipad" ]; then
  DEVICE_NAME="${DEVICE_NAME:-iPad Pro 13-inch (M4)}"   # 13" → 2064x2752
  DEST_SUBDIR="iPad screens"
  EXPECTED_W=2064; EXPECTED_H=2752
else
  DEVICE_NAME="${DEVICE_NAME:-iPhone 17 Pro Max}"       # 6.9" → 1320x2868
  DEST_SUBDIR="iPhone screens"
  EXPECTED_W=1320; EXPECTED_H=2868
fi

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
PROJECT="$REPO/apps/ApproachNote.xcodeproj"
DEST="$REPO/marketing/$DEST_SUBDIR"
TMP="$(mktemp -d)"
export DEVELOPER_DIR="${DEVELOPER_DIR:-/Applications/Xcode.app/Contents/Developer}"

log() { printf '\033[1;34m▸ %s\033[0m\n' "$*"; }

# ---------------------------------------------------------------------------
# Resolve + boot the simulator (by UDID, so we never rely on an ambiguous
# "booted" when multiple sims are up). We pull the UDID from xcodebuild's
# eligible destinations — NOT `simctl list` — because simctl also lists sims on
# runtimes older than the app's deployment target, which xcodebuild rejects.
# ---------------------------------------------------------------------------
UDID="$(xcodebuild -project "$PROJECT" -scheme ApproachNote -showdestinations 2>/dev/null \
  | grep 'platform:iOS Simulator' \
  | grep -F "name:$DEVICE_NAME }" \
  | grep -oE 'id:[0-9A-Fa-f-]{36}' | tail -1 | cut -d: -f2 || true)"
if [ -z "$UDID" ]; then
  echo "ERROR: no build-eligible simulator named '$DEVICE_NAME'. Eligible destinations:" >&2
  xcodebuild -project "$PROJECT" -scheme ApproachNote -showdestinations 2>/dev/null \
    | grep 'platform:iOS Simulator' | grep -E 'iPhone|iPad' >&2 || true
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

# capture <out-name> [extra launch args...]
# Cold-launches the app each time with the screenshot launch args (which open
# the right screen full-screen, pre-scrolled), waits for load+scroll, then
# screenshots. Uses launch args rather than `simctl openurl` because openurl
# prompts on iPad and presents detail as a centered form sheet.
capture() {
  local name="$1"; shift
  log "Capturing $name…"
  xcrun simctl terminate "$UDID" "$BUNDLE_ID" >/dev/null 2>&1 || true
  xcrun simctl launch "$UDID" "$BUNDLE_ID" "${LAUNCH_ARGS[@]}" "$@" >/dev/null
  sleep "$LOAD_WAIT"
  xcrun simctl io "$UDID" screenshot "$TMP/$name.png" >/dev/null 2>&1
}

capture "01_songlist"
capture "02_songdetails" -screenshotSongId "$SONG_FEATURED_ID" -screenshotAnchor featured
capture "03_recordings"  -screenshotSongId "$SONG_RECORDINGS_ID" -screenshotAnchor recordings
capture "04_artist"      -screenshotArtistId "$ARTIST_ID"

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
