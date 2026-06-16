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

`marketing/scripts/capture_ios_screenshots.sh` regenerates the four iPhone
screenshots end-to-end (~2.5 min), with no manual scrolling:

```bash
marketing/scripts/capture_ios_screenshots.sh            # build, boot, capture
marketing/scripts/capture_ios_screenshots.sh --no-build # reuse installed app
```

It boots an **iPhone 17 Pro Max** sim (1320×2868 = App Store 6.9"), builds +
installs the app, sets a clean 9:41 / full-signal / full-battery status bar,
then for each screen cold-launches with `-screenshotMode YES` (generated
covers) + `-hasCompletedOnboarding YES` (skip onboarding) and captures:

| File | Deep link | Lands on |
|---|---|---|
| `01_songlist` | *(launch)* `-screenshotListLetter K` | songs list at "K" |
| `02_songdetails` | `approachnote://song/{id}?screenshot=featured` | Featured Recordings carousel |
| `03_recordings` | `approachnote://song/{id}?screenshot=recordings` | All Recordings (first decade expanded) |
| `04_artist` | `approachnote://artist/{id}` | artist detail |

Output lands in `marketing/iPhone screens/`. Edit the config block at the top
of the script to change device, target song/artist UUIDs, or the list letter.

### Deep-link "screenshot states"

The `?screenshot=<anchor>` query param on a song deep link opens the screen
already scrolled to that section (anchors: `featured`, `recordings`), wired in
`SongDetailView` via `ScrollViewReader`. The param is inert without a value, so
it has no effect in normal use. The songs-list pre-scroll uses the
`-screenshotListLetter` launch argument instead (`ScreenshotMode.listLetter`).
To add a new screenshot screen: add a `.id("anchor")` to the target section and
a `capture` line to the script.

Note: `04_artist` shows the real artist *photograph* (not an album cover, so
outside the 5.2.1 album-art issue). Screenshot mode only swaps album covers.

## Resubmission

After regenerating the screenshots with screenshot mode on, replace the
screenshots/previews in App Store Connect and note in **App Review
Information** that all album artwork shown is original, generated placeholder
art (no third-party covers). The toggle does not affect release builds, so
normal users still see real cover art in the app.
