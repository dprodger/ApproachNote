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

`ApproachNoteTheme` defines type and color tokens but **not** spacing tokens
yet. The values above live as literals in `SongDetailView.swift`. When they
get promoted, the suggested names are:

```swift
extension ApproachNoteTheme {
    static let spacingXXS: CGFloat = 4
    static let spacingXS:  CGFloat = 8
    static let spacingSM:  CGFloat = 12
    static let spacingMD:  CGFloat = 16
    static let spacingLG:  CGFloat = 20
    static let spacingXL:  CGFloat = 24
}
```

Until then, this doc is the source of truth — copy the values, don't invent
new ones.
