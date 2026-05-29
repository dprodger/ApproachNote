# Trays (bottom sheets)

Reference view: **`RecordingFilterSheet`** (`iOS/Views/RecordingFilterSheet.swift`)
— the Filter tray reached from `SongDetailView`. It is the design template for
modal bottom-sheet trays. Match its presentation, chrome, and control styling
in new trays until these values are codified as reusable components.

All values resolve through `ApproachNoteTheme` tokens (see
[layout.md](layout.md) for the spacing scale and the semantic color roles).
Treat any raw literal outside those tokens as a smell — the one deliberate
exception in the reference is noted below.

## Presentation

| Aspect            | Value                                          |
|-------------------|------------------------------------------------|
| Container         | `NavigationStack` → `ScrollView` → `VStack`    |
| Detents           | `.presentationDetents([.medium, .large])`      |
| Grabber           | `.presentationDragIndicator(.visible)`         |
| Sheet background   | `ApproachNoteTheme.background`                 |

The tray opens at medium height with a visible drag indicator, and can be
dragged up to full height.

## Navigation bar

The tray carries a themed inline nav bar, styled **from the live palette** —
not the global `UINavigationBar` appearance, which is set once at launch and
goes stale when the palette changes. This mirrors `jazzNavigationBar`.

```swift
.navigationTitle("Filter Recordings")
.navigationBarTitleDisplayMode(.inline)
.toolbarBackground(ApproachNoteTheme.brand, for: .navigationBar)
.toolbarBackground(.visible, for: .navigationBar)
.toolbarColorScheme(.dark, for: .navigationBar)
```

| Element            | Placement          | Styling                                                       |
|--------------------|--------------------|---------------------------------------------------------------|
| Title              | inline             | (system, on brand bar via `.toolbarColorScheme(.dark)`)       |
| Dismiss action     | trailing           | `Button("Done")` · `.fontWeight(.semibold)` · `textOnDark`    |
| Reset action       | leading (when active) | `Button("Clear All")` · `textOnDark`, shown only when filters are active |

Both toolbar buttons use `textOnDark` so they read against the brand bar; the
primary "Done" action is `.semibold`, the secondary "Clear All" is regular
weight and conditionally hidden.

## Content layout

| Aspect                    | Value                                              |
|---------------------------|----------------------------------------------------|
| Outer VStack alignment    | `.leading`                                         |
| Inter-section spacing     | `28` (off-scale — see note)                        |
| Content padding           | `.padding()` (16, screen gutter)                   |
| Intra-section spacing     | `spacingXS` (8)                                    |
| Header → first control    | `.padding(.top, spacingXXS)` (4)                   |
| Trailing slack            | `Spacer(minLength: 40)`                            |

> **Off-scale note:** the 28 pt gap between major sections sits above
> `spacingXL` (24). It is the one intentional literal in the reference and a
> candidate to fold into the spacing scale if more trays need it.

### Section header

Each section leads with a title + descriptive subtitle stack:

```swift
Text("Playback availability")
    .font(ApproachNoteTheme.headline())
    .foregroundColor(ApproachNoteTheme.textPrimary)
Text("Select which service(s) you'd like to include for playback")
    .font(ApproachNoteTheme.subheadline())
    .foregroundColor(ApproachNoteTheme.textSecondary)
```

- **Title:** `headline()`, `textPrimary`
- **Subtitle:** `subheadline()`, `textSecondary`

## Controls

### Checkbox row (multi-select)

Plain-styled button; the whole row is the tap target.

| Aspect            | Value                                                                |
|-------------------|----------------------------------------------------------------------|
| Button style      | `.buttonStyle(.plain)`                                               |
| Row HStack spacing | `spacingSM` (12)                                                    |
| Icon              | `checkmark.circle.fill` / `circle`, `title3()`                       |
| Icon color        | selected → `brand`; unselected → `textSecondary.opacity(0.5)`        |
| Label             | `body()` + `.bodyLineSpacing()`, `textPrimary`                       |
| Row vertical padding | `spacingXS` (8)                                                   |
| Hit target        | `.contentShape(Rectangle())`                                         |

### Selectable chip / pill (single-select)

Laid out in a 3-column `LazyVGrid` (`spacingXS` gutters). Each chip:

| Aspect            | Selected                  | Unselected                                 |
|-------------------|---------------------------|--------------------------------------------|
| Font              | `subheadline()`           | `subheadline()`                            |
| Background        | `brand`                   | `surface`                                  |
| Foreground        | `textOnAccent`            | `textPrimary`                              |
| Border            | none (`Color.clear`)      | `textSecondary.opacity(0.5)`, 1 pt stroke  |
| Corner radius     | `8`                       | `8`                                        |
| Padding           | `spacingXS` vertical + horizontal | same                               |
| Width             | `.frame(maxWidth: .infinity)` | same                                   |

Long labels degrade with `.lineLimit(1)` + `.minimumScaleFactor(0.8)`.

## Summary of token usage

- **Fonts:** `headline()` (section titles), `subheadline()` (subtitles, chips),
  `body()` (row labels), `title3()` (control icons).
- **Colors:** `brand` (selection fill, active icons), `surface` (resting chip),
  `background` (sheet), `textPrimary` / `textSecondary` (text hierarchy),
  `textOnAccent` (text on selected chip), `textOnDark` (nav bar buttons).
- **Spacing:** `spacingXXS`/`spacingXS`/`spacingSM` throughout; `28` only for
  major section separation.
- **Selection idiom:** filled `brand` background (chips) or filled icon glyph
  (checkboxes) — never rely on color alone where a glyph state is available.
