# Commercial API Terms Review — YouTube & Spotify

Review date: 2026-04-23
Sources:
- YouTube API ToS: https://developers.google.com/youtube/terms/api-services-terms-of-service
- YouTube Developer Policies: https://developers.google.com/youtube/terms/developer-policies
- YouTube Branding Guidelines: https://developers.google.com/youtube/terms/branding-guidelines
- Spotify Developer Terms: https://developer.spotify.com/terms
- Spotify Design Guidelines: https://developer.spotify.com/documentation/design

Short answer: **yes, ApproachNote can go commercial on both**, but each has gotchas that probably require small changes. Details below.

---

## 1. Commercial use

### YouTube Data API — allowed, but watch the monetization rules

- **Paid app / subscription is fine** (Dev Policies III.G.2.a).
- **You cannot gate YouTube playback behind a paywall** — Dev Policies III.F.3: "must not charge users to watch content" and must not require any user action other than clicking play. So if ApproachNote ever becomes subscription-only, the YouTube video links have to stay accessible, or at minimum the "play" action can't be gated.
- **Can't sell ads on pages showing YouTube data** unless non-YouTube content on that page offers "enough independent value to justify" the ad (III.G.1.d). Not relevant today, relevant if you add ads.
- **30-day storage cap**: this one directly affects the codebase. Dev Policies III.E.4.c/d: cached YouTube API data (video IDs, titles, thumbnails, channel names) must be **deleted or refreshed within 30 days**. The YouTube handler in the `research_jobs` pipeline stores match results on recordings — you'll need a periodic refresh/expiry job, or re-query before display beyond the 30-day window.
- **User-deletion endpoint required**: if you store anything tied to a signed-in user (not today, but keep in mind for OAuth features), you must delete within 7 days of request (III.E.4.g).
- **Feature parity** (III.C.8): you can't *consistently* demote YouTube relative to Spotify/Apple Music in the UI. Showing all three equally is fine.
- **Can't substantially duplicate YouTube itself** (III.I.1) — a jazz-study discovery app is clearly not that.
- **Privacy policy is mandatory** (ToS §7). Must disclose what you access/store and that data is shared with Google.
- **Acquisition clause**: 15-day notification if the company is acquired (§25.9).

### Spotify Web API — allowed, but EULA + quota process

- **Paid / freemium is fine.** You just can't monetize the Spotify *data itself* (IV.2.5, V.5) — i.e., no selling metadata, no feeding it to ad networks.
- **Extended quota application** (VI.7): the default quota is "development mode" with a 25-user cap. To launch commercially you must submit an extended-quota request through the dev dashboard describing the use case. No guaranteed timeline — apply well before launch.
- **Metadata must stay fresh** (IV.3): no indefinite storage; "reasonable efforts" to delete stale data. The periodic `research_jobs` refresh already helps here, but make sure Spotify matches get re-checked rather than frozen forever.
- **No ML training on Spotify content** (IV.2.1.a). If you ever do recommendation or categorization, don't train on Spotify metadata.
- **No stream ripping / download enabling** (IV.2.2.3) — you're just deep-linking, so fine.
- **EULA requirement** (V.11): ApproachNote's EULA must
  - disclaim warranties on Spotify's behalf,
  - prohibit reverse-engineering, and
  - name **Spotify as a third-party beneficiary**.
  This is an easy-to-miss, specific clause you probably need to add to your app's terms.
- **Personal data deletion within 5 days** of a user disconnecting their Spotify account (Appendix A.5.c) — relevant only if you add Spotify OAuth login later.
- **Multi-service (YouTube + Spotify + Apple Music in one app) is not prohibited.** Each provider's rules apply independently.

---

## 2. Branding requirements

### YouTube

- Use one of: "Developed with YouTube" badge, the YouTube logo, or the YouTube icon — placed next to where YouTube results appear, linking back to YouTube content.
- **Logo cannot be the most prominent element** on the screen.
- **Cannot use "YouTube," "YT," or variants in the app name or description.** ApproachNote is fine.
- Don't alter logo colors (except the allowed monochrome "Developed with YouTube" variants).
- Don't imply content originated from YouTube when it didn't (Dev Policies III.F.2.b).

### Spotify

- Show the Spotify logo (icon + wordmark, or icon alone if cramped) wherever Spotify metadata/links appear. Minimum 70px full logo, 21px icon.
- Button text must be one of: **"PLAY ON SPOTIFY," "OPEN SPOTIFY," "LISTEN ON SPOTIFY,"** or **"GET SPOTIFY FREE"** if not installed. Link buttons should match this.
- Spotify Green on black/white; monochrome otherwise. No rotating, stretching, or modifying the mark.
- **Album art rules**: display as-provided with rounded corners (4px small / 8px large). No cropping, no text overlays, no branding on top of artwork. If space is tight, omit art entirely rather than crop.
- Show metadata (track/artist names) as Spotify provides it — truncation OK, editing not.
- **Don't imply partnership or endorsement** without written approval (IX.7).

---

## Concrete action list for ApproachNote

1. **Add a 30-day YouTube cache expiry/refresh** to the `research_jobs` pipeline — this is the most material code change.
2. **Apply for Spotify extended quota** before public commercial launch.
3. **Publish a privacy policy** covering YouTube and Spotify data sharing; EU consent compliance if EU users are in scope.
4. **Add an EULA** with the Spotify third-party-beneficiary clause, warranty disclaimer, and no-reverse-engineering language.
5. **Audit the player/link UI** to make sure YouTube playback isn't gated by any future paywall, and Spotify buttons use one of the four approved labels.
6. **Audit branding**: add the YouTube attribution logo/badge near YouTube results; use the Spotify logo + approved button copy next to Spotify links; don't overlay anything on album art.
7. **Verify you don't train ML on Spotify data** if/when you add any recommendation features.

Nothing here blocks commercialization — the substantive code work is the 30-day YouTube refresh and the Spotify EULA/quota paperwork.
