# Layout & Spacing

Reference view: **`SongDetailView`** — the polished design template. Match its
rhythm in new screens until these values are codified as constants on
`ApproachNoteTheme`. At that point this document becomes the spec and the
code references the tokens.

## Spacing scale

The view uses six recurring values. Treat anything outside this set as a smell.

| Token | Value | Meaning                                | Where it shows up                                         |
|-------|-------|----------------------------------------|-----------------------------------------------------------|
| xxs   | 4 pt  | Hairline offset                        | Nudging a sub-element below its header                    |
| xs    | 8 pt  | Tight component internals              | Icon ↔ label, paragraph stack inside a single block       |
| sm    | 12 pt | Grouped elements within a section      | Card body stack, song-info header stack                   |
| md    | 16 pt | Section separation                     | Between sibling sections inside a screen                  |
| lg    | 20 pt | Horizontal carousel item gap           | Gap between cards in a horizontal shelf / carousel        |
| xl    | 24 pt | Screen edge gutter, page-header top    | Left/right screen padding; space above title from nav bar |

## Component sizes

| Element                                  | Value  |
|------------------------------------------|--------|
| Featured-recording artwork (square)      | 204 pt |
| Album art corner radius                  | 12 pt  |

## Detail header (collapsing brand bar)

Detail screens (`SongDetailView`, and to follow `PerformerDetailView` /
`RecordingDetailView`) use a custom brand-colored header instead of the system
nav bar, because the system bar can't give us the taller band, white-outlined
circle buttons (including the back chevron), or a category→specific title swap.
Lives in `iOS/Components/DetailHeader.swift`. Unlike the spacing scale above,
these values **are** codified — in `DetailHeaderMetrics`.

| Metric                  | Value      | Meaning                                                  |
|-------------------------|------------|----------------------------------------------------------|
| `expandedHeight`        | 92 pt      | Header height below the status bar at rest               |
| `collapsedHeight`       | 58 pt      | Header height once fully scrolled                        |
| `buttonDiameter`        | 36 pt      | Circle button size                                       |
| `collapsedBottomPadding`| 12 pt      | Breathing room under the buttons when collapsed          |
| `expandedBottomPadding` | 24 pt      | Breathing room under the buttons when expanded           |
| `titleSwapOffset`       | 64 pt      | Scroll distance after which the label swaps to the title |

Behavior:
- **Height** interpolates from `expandedHeight` to `collapsedHeight` as the
  user scrolls; the button row is bottom-aligned with breathing room
  interpolated `12 → 24` so the buttons sit low in the expanded band and ride
  up as it collapses.
- **Buttons** are white-outlined circles with a white glyph on the brand fill.
  A `.filled` variant (white fill, brand glyph) is reserved for a primary
  action like play (e.g. on `RecordingDetailView`).
- **Title** is centered and shows the generic category ("Song", "Artist",
  "Recording") until `titleSwapOffset`, then cross-fades to the specific title
  (song / performer / album name).
- **Overscroll**: on pull-down the brand fill extends below the bar so the
  content background never shows through.
- **Swipe-back** is preserved via `SwipeBackEnabler` (the system bar / back
  button are hidden).

Adoption — each detail screen does two things; the `.collapsingDetailHeader`
modifier owns the rest (scroll-offset tracking, nav-bar hiding, swipe-back,
screen background, and the `DetailHeaderBar` overlay):
1. Place a `DetailHeaderSpacer()` at the top of the scroll content.
2. Apply `.collapsingDetailHeader(expandedTitle:collapsedTitle:trailing:)` to the
   `ScrollView`. Omit `trailing` for a back-only header (e.g. PerformerDetailView).

```swift
ScrollView {
    VStack(spacing: 0) {
        DetailHeaderSpacer()
        // …screen content…
    }
}
.collapsingDetailHeader(expandedTitle: "Song", collapsedTitle: song?.title ?? "Song") {
    DetailCircleButton(systemName: "plus", accessibilityLabel: "Add", action: { … })
}
```

## Section anatomy in `SongDetailView`

```
Outer container          VStack(spacing: 0)              + .padding(.bottom)
└── Top section          VStack(spacing: 16 / md)
    ├─ horizontal gutter .padding(.horizontal, 24 / xl)
    ├─ top inset         .padding(.top, 24 / xl)
    └─ bottom inset      .padding(.bottom, 16 / md)
    │
    ├── Title block            VStack(spacing: 12 / sm)
    │   ├── Title + composer
    │   ├── Song reference     HStack(spacing: 8 / xs)   + .padding(.top, 4 / xxs)
    │   ├── Research status
    │   ├── Summary info       VStack(spacing: 8 / xs)   + .padding(.top, 4 / xxs)
    │   ├── External refs                                  .padding(.top, 8 / xs)
    │   └── Featured Recordings carousel
    │       ├── header stack   VStack(spacing: 16 / md)  + .padding(.top, 16 / md)
    │       └── items          HStack(spacing: 20 / lg)
    │
    ├── Recordings section
    ├── Transcriptions section
    └── Backing Tracks section
```

Sibling spacing between Recordings / Transcriptions / Backing Tracks is
inherited from the outer `VStack(spacing: 16 / md)`.

### `AuthoritativeRecordingCard` (carousel item)

```
VStack(spacing: 12 / sm)
├── Artwork — 204×204 pt, corner radius 12 pt, drop shadow
└── Info stack — VStack(spacing: 4 / xxs)
    ├── Year
    ├── Artist
    ├── Album
    └── Title
```

## Status

`ApproachNoteTheme` now codifies the spacing scale alongside its type and color
tokens:

```swift
extension ApproachNoteTheme {
    static let spacingXXS: CGFloat = 4   // hairline offset
    static let spacingXS:  CGFloat = 8   // tight component internals
    static let spacingSM:  CGFloat = 12  // grouped elements within a section
    static let spacingMD:  CGFloat = 16  // section separation
    static let spacingLG:  CGFloat = 20  // horizontal carousel item gap
    static let spacingXL:  CGFloat = 24  // screen edge gutter, page-header top
}
```

Reference these tokens in views instead of literals — `SongDetailView` is the
adopted template. Treat any spacing value outside this set as a smell. Issue
#199 tracks rolling adoption through the remaining screens.
