# Docs UX behavior registry

This file is the contract for how the documentation site is supposed to
behave. It lives outside `docs/src/` on purpose: it is **not** rendered into
the book — it exists so intentional UX decisions survive theme refactors and
mdBook upgrades. Before changing anything in `docs/theme/` or the docs CI,
check the entries below; if a change intentionally alters a behavior, update
its entry (and note the commit) in the same PR. If a behavior regresses
without an entry change, that is a bug.

Behaviors are grouped by area. Commit hashes refer to the history of the
branch that introduced the docs and its successors.

## Versioning

- **Version picker.** The header's right-button cluster contains a version
  pill immediately after the theme switch. It shows the version the current
  build lives under and opens a dropdown of all published versions. `latest`
  is always listed first and is the default: it is the build of `main`,
  served at the site root. Tagged versions (`v*` tags whose tree contains
  `docs/book.toml`) are served under `/<tag>/` and listed newest-first from
  `versions.json`, which CI writes at the site root on every deploy.
- **Same-page switching.** Switching versions keeps the reader on the page
  they were reading when the target version has it (checked with a HEAD
  request; the `#fragment` is preserved).
- **Missing-page fallback.** If the target version does not have the page,
  the reader lands on that version's start page with a notice explaining
  that the requested page is not available in this version. The requested
  page is remembered for the session: switching to a version that does have
  it returns the reader to that page. Navigating anywhere else drops the
  remembered page — from then on, switching follows the current page again.
- **Old-version banner.** Every page of a non-latest version shows a banner
  ("You are viewing documentation for vX.Y.Z") with a link that switches to
  latest using the same same-page-with-fallback rules.
- **Local builds.** `mdbook build`/`serve` have no `versions.json`; the
  picker degrades to showing only the current version and nothing errors.

## Header

- **Logo.** The wordmark swaps with the theme: `tn-light.svg` on light,
  `tn-dark.svg` on dark. Below 768px the wordmark is always replaced by the
  square TEL badge (`tel-badge.svg`); between 768px and 899px the badge also
  takes over whenever the sidebar is open, because the overlay squeezes the
  header too much for the wordmark. All logo art is SVG. (c513775b,
  61b30594, 52558404)
- **Desktop-only chrome.** The print, repository, and edit links hide below
  768px; the search pill collapses to its icon. The theme switch and the
  version pill always remain. (c513775b)
- **Search.** The search pill shows a ⌘K hint and opens a floating centered
  panel; `⌘K`/`Ctrl+K` and `/`/`s` all open it. (c513775b)
- **Theme switch.** A three-way Light / System / Dark pill relays clicks to
  the hidden stock theme list so book.js keeps ownership of theme state and
  localStorage. Only the light and navy palettes ship. (c513775b)

## Cursor

- The default cursor across the site is the TEL badge, rendered at 24px from
  `tel-cursor.svg` with a PNG fallback for Safari, with the hotspot at the
  badge's center (12,12) so clicks land where the logo's center sits.
  Interactive elements still show their native cursors (pointer, text).
  (da39018d, 06a7e888, a9a126d1)

## Sidebar

- **Whole-row links.** Each chapter row is one visual pill: hovering
  anywhere on the row highlights the row as a single element, the pointer
  cursor covers the entire `<li>`, and clicking anywhere on the row (not
  just the link text) navigates. The fold chevron is a fixed 20px button
  whose glyph rotates in place without escaping the row. (6032ef17,
  0b296421)
- **Active-row collapse.** Clicking the row of the page the reader is
  already on toggles its section fold instead of reloading the page.
  (61b30594)
- **Overlay persistence.** Below 1080px the sidebar is an overlay that stock
  mdBook closes on every page load. An open sidebar stays open across
  in-book navigation for the rest of the session; a fresh visit still starts
  closed, and shrinking the window still collapses it. (61b30594)

## Content

- **No blank pages.** Every page reachable from the sidebar has at least a
  brief description of its contents — section indexes must introduce and
  link their children. (cb42dcb1)
- **On this page rail.** At 1280px and wider, a right-hand rail lists the
  page's h2/h3 headings with a deterministic scroll-spy: the active entry is
  the last heading above the reading line, and reaching the bottom marks the
  last heading. (c513775b)
- **Sticky header clearance.** Page titles are never hidden under the
  sticky header on load or on anchor jumps. (c513775b)
