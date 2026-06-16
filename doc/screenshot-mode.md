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

## Resubmission

After regenerating the screenshots with screenshot mode on, replace the
screenshots/previews in App Store Connect and note in **App Review
Information** that all album artwork shown is original, generated placeholder
art (no third-party covers). The toggle does not affect release builds, so
normal users still see real cover art in the app.
