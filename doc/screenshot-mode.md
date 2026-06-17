# Screenshot mode (App Store Guideline 5.2.1)

App Store Review rejected the app under **Guideline 5.2.1 – Intellectual
Property** because the App Store **screenshots/previews** displayed protected
third-party album cover artwork. Showing licensed cover art *inside* the app
(via the Spotify / Apple Music / Cover Art Archive APIs) is fine; the problem
is only the marketing assets uploaded to App Store Connect.

"Screenshot mode" swaps every real album cover for original, procedurally
generated artwork so the screenshots never contain a copyrighted cover.

## How it works

- `Shared/Support/GenericCoverArt.swift` generates abstract cover art that is
  fully determined by a seed string (the real artwork URL). The same recording
  always renders the same generated cover, and a grid shows a varied spread —
  so screenshots look natural. The art is entirely our own.
- `Shared/Support/ScreenshotMode.swift` holds the on/off flag.
- iOS routes almost all album art through `CachedAsyncImage`, which substitutes
  generated art when the flag is on (one chokepoint). The few screens that use
  raw `AsyncImage` (iOS `SongDetailView`; Mac `RecordingCard`,
  `FeaturedRecordingCard`, `RecordingDetailView`, `ContentView`,
  `PerformerDetailView`) wrap their image in `CoverArtImage`, which does the
  same swap.

## Turning it on

Any one of these enables it (all resolve to the `screenshotMode` UserDefaults
key, except the env var):

1. **Launch argument** `-screenshotMode YES` — `UserDefaults` parses
   `-key value` launch arguments automatically, so this works with fastlane
   `snapshot`:

   ```swift
   app.launchArguments += ["-screenshotMode", "YES"]
   ```

2. **Environment variable** `SCREENSHOT_MODE=1` (or `true` / `yes`).

3. **In-app toggle** — Settings → *Developer → Screenshot mode* (DEBUG builds
   only; absent from release builds).

For manual screenshots: run a Debug build, flip the toggle in Settings,
navigate to the screens you want, and capture. Toggle it back off when done.

## Automated capture (the 4 App Store screens)

`marketing/scripts/capture_ios_screenshots.sh` regenerates the four screenshots
end-to-end, with no manual scrolling, for **iPhone and iPad**:

```bash
marketing/scripts/capture_ios_screenshots.sh             # iPhone, build + capture
marketing/scripts/capture_ios_screenshots.sh --ipad      # iPad, build + capture
marketing/scripts/capture_ios_screenshots.sh --ipad --no-build  # reuse installed app
```

| Profile | Device | Size (App Store) | Output |
|---|---|---|---|
| *(default)* | iPhone 17 Pro Max | 1320×2868 (6.9") | `marketing/iPhone screens/` |
| `--ipad` | iPad Pro 13-inch (M4) | 2064×2752 (13") | `marketing/iPad screens/` |

It resolves a **build-eligible** sim UDID from `xcodebuild -showdestinations`
(not `simctl list`, which also lists sims on runtimes below the deployment
target), builds + installs, sets a clean 9:41 / full-signal / full-battery
status bar, then for each screen cold-launches with `-screenshotMode YES`
(generated covers) + `-hasCompletedOnboarding YES` (skip onboarding) and the
per-screen launch args below, and captures:

| File | Launch args | Lands on |
|---|---|---|
| `01_songlist` | `-screenshotListLetter K` | songs list at "K" |
| `02_songdetails` | `-screenshotSongId {id} -screenshotAnchor featured` | Featured Recordings carousel |
| `03_recordings` | `-screenshotSongId {id} -screenshotAnchor recordings` | All Recordings (first decade expanded) |
| `04_artist` | `-screenshotArtistId {id}` | artist detail |

Edit the config block at the top of the script to change device, target
song/artist UUIDs, or the list letter.

### How the screenshot states work

Navigation is driven by **launch arguments**, not `simctl openurl` — openurl
prompts ("Open in ApproachNote?") on iPad and presents detail as a centered
form sheet. `ApproachNoteApp` reads `-screenshotSongId`/`-screenshotArtistId`
at launch and presents the detail via `fullScreenCover` (full-screen on both
iPhone and iPad). `-screenshotAnchor featured|recordings` scrolls
`SongDetailView` to that section (`ScrollViewReader` anchors `featured` /
`recordings`); `-screenshotListLetter` scrolls the songs list
(`ScreenshotMode.listLetter`). All are inert without a value, so normal use and
the real `approachnote://` deep-link path are unaffected.

To add a new screenshot screen: add a `.id("anchor")` to the target section,
teach `ScreenshotMode`/`ApproachNoteApp` the new launch arg if needed, and add
a `capture` line to the script.

If a stale "Open in ApproachNote?" SpringBoard alert lingers (e.g. after a
manual `openurl`), reboot the sim (`xcrun simctl shutdown <udid> && … boot`).

Note: `04_artist` shows the real artist *photograph* (not an album cover, so
outside the 5.2.1 album-art issue). Screenshot mode only swaps album covers.

## Resubmission

After regenerating the screenshots with screenshot mode on, replace the
screenshots/previews in App Store Connect and note in **App Review
Information** that all album artwork shown is original, generated placeholder
art (no third-party covers). The toggle does not affect release builds, so
normal users still see real cover art in the app.
