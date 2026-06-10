# Web Layout Grid

Spec for the **marketing site** (`backend/templates/`, `backend/static/css/style.css`).
For the iOS/Mac app spacing system, see [`layout.md`](layout.md) instead.

## The content area

Page content lives inside a centered `.container`:

| Property            | Value            |
|---------------------|------------------|
| `max-width`         | 1120px           |
| Horizontal padding  | 1.5rem (24px) each side |
| `box-sizing`        | `border-box` (global reset) |
| **Inner width**     | **1072px** at full width (1120 − 2 × 24) |

## The 4-column grid

The content area divides into **4 equal columns separated by 3 gutters**:

```
| col |  gut  | col |  gut  | col |  gut  | col |
 238    40     238    40     238    40     238     = 1072px
```

- **Columns** are fluid — `repeat(4, 1fr)`, so they shrink with the viewport.
  At the 1072px inner width they each resolve to ~238px, but that number isn't
  meaningful on its own; the browser computes it. Don't hard-code column widths.
- **Gutters** are a fixed token: `--grid-gutter: 2.5rem` (40px). This is the
  only fixed value in the grid.

### Columns vs. grid lines (the `1 / 3` gotcha)

CSS `grid-column` counts the **lines around the columns, not the columns.**
4 columns are bordered by **5 lines** (numbered 1–5):

```
line 1   line 2   line 3   line 4   line 5
   |  col 1  |  col 2  |  col 3  |  col 4  |
   1 ------- 2 ------- 3 ------- 4 ------- 5
```

So `grid-column: 1 / 3` means "from **line 1** to **line 3**" → it covers
**columns 1 and 2**. The end number is the line *after* the last column you
want, which is why a two-column span ends on `3`, not `2`.

| Span         | `grid-column` | Columns covered | Resolved width |
|--------------|---------------|-----------------|----------------|
| First two    | `1 / 3`       | 1 + 2           | 516px (238 + 40 + 238) |
| Last two     | `3 / 5`       | 3 + 4           | 516px          |
| All four     | `1 / -1`      | 1–4             | 1072px         |

(`-1` is shorthand for the last line, i.e. line 5.)

### Tokens & utility

```css
:root { --grid-gutter: 2.5rem; }

.grid-4 {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    column-gap: var(--grid-gutter);
}
```

Compose `.grid-4` onto a `.container` and have children opt into spans:

```html
<div class="container grid-4">
    <div style="grid-column: 1 / 3">…</div>   <!-- first two columns -->
    <div style="grid-column: 3 / 5">…</div>   <!-- last two columns -->
</div>
```

## Applied: the hero

The home-page hero (`.hero .container.grid-4`) is the reference implementation:

- **`.hero-copy`** (headline, paragraph, App Store badge) → `grid-column: 1 / 3`
  (columns 1–2).
- **`.hero-media`** (the duotone photo) → `grid-column: 3 / 5` (columns 3–4).
- The middle gutter (between columns 2 and 3) is the separation between copy and
  image — a full 40px.
- Vertical rhythm lives on the `.hero` section (`padding: 5rem 0`), giving the
  image breathing room above (header) and below (the `.explore` hairline) rather
  than butting against them.

The hero image is square (`aspect-ratio: 1 / 1`), so at the 2-column span it
renders 516px × 516px.

## Responsive

Below **820px** the side-by-side grid is too tight for the wide Colt headline, so
the hero collapses: `.hero .container` becomes a single column and copy/image
stack (`grid-column: 1 / -1`). Container padding tightens to 1.25rem under 768px.

## Image sizing

Hero images display at most **~516px** wide (the 2-column span at the 1120px
container; smaller on narrower viewports). Sources are **1100 × 1100** — comfortably
over 2× for retina while staying lean (~70–160 KB each). They are built by
`marketing/scripts/build_hero_images.py` (set `SIZE` and re-run to change the
output dimension); the `<img>` carries matching `width`/`height` attributes so
the box is reserved before the JPEG loads.
