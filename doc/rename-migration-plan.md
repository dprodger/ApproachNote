# Rename migration: `Jazz-Liner-Notes` / `JazzReference` → `ApproachNote`

> **Status (2026-04-19): Complete.** All five tiers have shipped:
> - Tier 1 (cosmetic text)
> - Tier 2 (Swift types, Xcode file + target names)
> - Tier 3 (Keychain service + App Group)
> - Tier 4 (bundle IDs, share-extension folder renames, Sign in with Apple linking)
> - Tier 5 (repo + local directory rename + doc cleanup) — issue [#146](https://github.com/dprodger/ApproachNote/issues/146)
>
> This file is retained as a historical execution reference. The surviving
> legacy-named items in the sections below have been renamed; the file paths
> and pbxproj line numbers cited reflect pre-migration state.

GitHub issue (original planning): [#134](https://github.com/dprodger/ApproachNote/issues/134).

Inventory of every place the legacy names appeared, ordered from cheapest to
most expensive to change.

**Target naming convention**
- Code identifiers, bundle IDs, directories, file names → `ApproachNote` (camelCase, no space)
- Human-facing display text → `Approach Note` (two words, with space) — already correct in most places
- Reverse-DNS bundle prefix → `com.approachnote.*` (matches the existing `com.approachnote.admin-web` Services ID)

Scope counted via grep: **148 files, 342 occurrences**. Most of that volume is trivial (boilerplate file-header comments). The risky surface is narrow and concentrated in a dozen files.

---

## Tier 1 — Trivial text changes (cosmetic, zero risk)

Pure find-and-replace. Ships in minutes, no user impact, no build-system churn.

| What | Where | Notes |
|---|---|---|
| Swift `// JazzReference` file-header comments | ~40 files across `apps/iOS/`, `apps/Mac/`, `apps/Shared/`, `apps/MusicBrainzImporter/` | Xcode-generated boilerplate. Replace with `// ApproachNote`. |
| Swift `// JazzReferenceMac` headers | ~10 files in `apps/Mac/` | Same as above, Mac variant. |
| `"via JazzReference admin"` edit-note string | [backend/templates/admin/orphans_review.html:1331](backend/templates/admin/orphans_review.html:1331) | Appears in MusicBrainz work-relationship edit notes. Change to `"via Approach Note admin"`. |
| `"Import to JazzReference"` share-sheet label | [apps/MusicBrainzImporter/Info.plist:8](apps/MusicBrainzImporter/Info.plist:8) | iOS share-extension label. Already corrected on Mac variant (says "Import to Approach Note"). |
| Logger subsystem fallbacks `"com.jazzreference"` / `"com.jazzreference.MusicBrainzImporter"` | [apps/Shared/Support/Logging.swift:15](apps/Shared/Support/Logging.swift:15), [apps/MusicBrainzImporter/ShareViewController.swift:16](apps/MusicBrainzImporter/ShareViewController.swift:16), [apps/MusicBrainzImporter/YouTubeViews.swift:11](apps/MusicBrainzImporter/YouTubeViews.swift:11), [apps/MusicBrainzImporter/SharedDataManagers.swift:11](apps/MusicBrainzImporter/SharedDataManagers.swift:11) | Dead code path — only hit if `Bundle.main.bundleIdentifier` is nil, which never happens in a real build. Cosmetic only. |
| Docs in `doc/` (architecture review, script guide, merge ideas, etc.) | `doc/architecture-review-2026-04.md`, `doc/script_development_guide.md`, `doc/Layered Data Architecture Design.md`, `doc/ios-mac-code-sharing-assessment.md`, `doc/Potential recording merge ideas.md` | Purely documentation; change references as encountered or leave historical as-is. |
| `CLAUDE.md` (4 occurrences) | [CLAUDE.md](CLAUDE.md) | Update after other tiers settle — some refer to file/class names that change in Tier 2. |
| `README.md`, `sql/jazz-db-schema.sql` comment banner, `sql/jazz_starter_data.sql` | root README + SQL headers | Cosmetic header banners. |
| `apps/Mac/SETUP.md` and `apps/Mac/TODO.md` | Setup notes | Purely doc. |

**Recommendation:** one PR titled "Tier 1: cosmetic rename to ApproachNote", merged freely.

---

## Tier 2 — Swift types / Xcode file & target names (hours, compile-breaking until complete)

Low semantic risk but the changes must land atomically so the project keeps building.

### Types to rename

| Old → New | Refs | Files |
|---|---|---|
| `JazzTheme` → `ApproachNoteTheme` | **1,683 across 70 files** | Defined in [apps/Shared/Support/JazzTheme.swift](apps/Shared/Support/JazzTheme.swift). Use Xcode → rename symbol (works on Swift). |
| `JazzReferenceApp` struct → `ApproachNoteApp` | 1 definition + Xcode entry | [apps/iOS/App/JazzReferenceApp.swift:9](apps/iOS/App/JazzReferenceApp.swift:9). |

### Files to rename

| Old → New | Notes |
|---|---|
| `apps/iOS/App/JazzReferenceApp.swift` → `ApproachNoteApp.swift` | Touches Xcode `PBXBuildFile` entries in [apps/Approach Note.xcodeproj/project.pbxproj](apps/Approach%20Note.xcodeproj/project.pbxproj). |
| `apps/iOS/App/JazzReference.entitlements` → `ApproachNote.entitlements` | Referenced at pbxproj lines 1104, 1138 via `CODE_SIGN_ENTITLEMENTS`. |
| `apps/Mac/App/JazzReferenceMacApp.swift` → `ApproachNoteMacApp.swift` | pbxproj lines 18, 121, 334, 644. |
| `apps/Mac/App/JazzReferenceMac.entitlements` → `ApproachNoteMac.entitlements` | pbxproj line 746 `CODE_SIGN_ENTITLEMENTS`. Also referenced in Mac release entitlements. |
| `apps/Shared/Support/JazzTheme.swift` → `ApproachNoteTheme.swift` | Type rename + file rename in one PR. |

### Xcode project identifiers

In [apps/Approach Note.xcodeproj/project.pbxproj](apps/Approach%20Note.xcodeproj/project.pbxproj):

| What | Line(s) |
|---|---|
| `productName = JazzReference;` (iOS target) | 489 |
| Target name `JazzReferenceTests` | 493, 506, 1295 |
| Target name `JazzReferenceUITests` | 513, 526, 1304 |
| Legacy `remoteInfo = JazzReference;` cross-refs | 65, 72 |
| File-reference paths citing `JazzReferenceMac.entitlements` / `JazzReferenceMacApp.swift` | 120–121, 333–334 |
| Product-reference artifacts `JazzReferenceTests.xctest`, `JazzReferenceUITests.xctest` | 147–148, 383–384 |
| `TEST_HOST = ".../JazzReference.app/.../JazzReference"` | 1184, 1205 |
| `TEST_TARGET_NAME = JazzReference` | 1224, 1243 |

### Scheme files

[apps/Approach Note.xcodeproj/xcshareddata/xcschemes/Approach Note.xcscheme](apps/Approach%20Note.xcodeproj/xcshareddata/xcschemes/Approach%20Note.xcscheme) lines 53–66 reference `JazzReferenceTests.xctest` and `JazzReferenceUITests.xctest`. Update after target renames.

**Recommendation:** one PR per rename cluster (Theme, App struct, entitlements/schemes, test targets). Each is a compile-check gate. Use Xcode's rename-symbol wherever possible to catch references automatically.

---

## Tier 3 — Keychain service + App Group (affects users with the app installed)

### Keychain service name

[apps/Shared/Auth/KeychainHelper.swift:18](apps/Shared/Auth/KeychainHelper.swift:18):
```swift
private let service = "me.rodger.david.JazzReference"
```

**Impact of changing:** all users with the app installed lose their saved refresh tokens → forced re-login on next launch. Acceptable if the user base is small (beta); otherwise ship a one-shot migration that copies items from the old service to the new one before switching.

Target value: `com.approachnote.ApproachNote` (or just `com.approachnote`).

### App Group identifier

`group.me.rodger.david.JazzReference` appears in:

| Location | Role |
|---|---|
| [apps/iOS/App/JazzReference.entitlements:11](apps/iOS/App/JazzReference.entitlements:11) | iOS main app membership |
| [apps/MusicBrainzImporter/MusicBrainzImporter.entitlements:7](apps/MusicBrainzImporter/MusicBrainzImporter.entitlements:7) | iOS share extension |
| [apps/Mac/App/JazzReferenceMac.entitlements:13](apps/Mac/App/JazzReferenceMac.entitlements:13) | Mac main app (dev) |
| [apps/Approach Note MacRelease.entitlements:9](apps/Approach%20Note%20MacRelease.entitlements:9) | Mac main app (release) |
| [apps/MusicBrainzImporterMac/MusicBrainzImporterMac.entitlements:7](apps/MusicBrainzImporterMac/MusicBrainzImporterMac.entitlements:7) | Mac share extension (dev) |
| [apps/MusicBrainzImporterMac/MusicBrainzImporterMacRelease.entitlements:7](apps/MusicBrainzImporterMac/MusicBrainzImporterMacRelease.entitlements:7) | Mac share extension (release) |
| [apps/iOS/ShareExtensionBridge/SharedArtistData.swift:26, :189](apps/iOS/ShareExtensionBridge/SharedArtistData.swift:26) | Swift constants |
| [apps/iOS/ShareExtensionBridge/SharedSongDataManager.swift:37](apps/iOS/ShareExtensionBridge/SharedSongDataManager.swift:37) | Swift constants |
| [apps/Mac/ShareExtensionBridge/SharedDataManagers.swift:45, :166, :308](apps/Mac/ShareExtensionBridge/SharedDataManagers.swift:45) | Swift constants (3 occurrences) |
| [apps/MusicBrainzImporter/ShareViewController.swift:28](apps/MusicBrainzImporter/ShareViewController.swift:28) | Swift constants |
| [apps/MusicBrainzImporterMac/MacShareViewController.swift:24](apps/MusicBrainzImporterMac/MacShareViewController.swift:24) | Swift constants |

**Impact of changing:**
1. Shared `UserDefaults` data between app and share extension is lost (search-in-progress state, pending shares). Usually tolerable — worst case the user re-invokes the share extension.
2. Apple Developer portal: register a NEW App Group (`group.com.approachnote.shared` or similar), add every target to it in Capabilities. Old provisioning profiles must be regenerated.
3. Keep a Swift constant `oldAppGroupIdentifier` for one release so a migration shim can `UserDefaults(suiteName: old)` → copy → `UserDefaults(suiteName: new)` on first launch, then drop.

**Recommendation:** bundle Tier 3 with Tier 4 since bundle-ID changes also force new provisioning profiles — don't incur that churn twice.

---

## Tier 4 — Bundle IDs (Apple Developer work; affects Sign-in-with-Apple identity continuity)

### Current bundle IDs

| Target | Bundle ID | pbxproj lines |
|---|---|---|
| iOS main app | `me.rodger.david.Jazz-Liner-Notes` | 773, 826 |
| Mac main app | `me.rodger.david.JazzReferenceMac` (from entitlements + Info.plist), plus a duplicate iOS-ish `me.rodger.david.JazzReference` at pbxproj 1121, 1155 | see below |
| iOS share extension | `me.rodger.david.JazzReference.MusicBrainzImporter` | 935, 964 |
| Mac share extension | `me.rodger.david.Jazz-Liner-Notes.MusicBrainzImporterMac` | 869, 905 |
| iOS tests | `me.rodger.david.JazzReferenceTests` | 1176, 1197 |
| iOS UI tests | `me.rodger.david.JazzReferenceUITests` | 1216, 1235 |

There's also a legacy `me.rodger.david.JazzReference` baked into [apps/iOS/App/Info.plist:34](apps/iOS/App/Info.plist:34) (likely a URL scheme / keyword). [apps/Mac/App/Info.plist:41](apps/Mac/App/Info.plist:41) carries `me.rodger.david.JazzReferenceMac`. The `MacShareViewController.swift:1049` hard-checks `bundleIdentifier == "me.rodger.david.Jazz-Liner-Notes"` to find the companion app.

### Proposed new bundle IDs

| Target | New bundle ID |
|---|---|
| iOS main | `com.approachnote.ios` |
| Mac main | `com.approachnote.mac` |
| iOS share | `com.approachnote.ios.ShareImporter` |
| Mac share | `com.approachnote.mac.ShareImporter` |
| iOS tests | `com.approachnote.ios.Tests` |
| iOS UI tests | `com.approachnote.ios.UITests` |

### Share-extension target + folder renames

Rename the share extensions alongside the bundle-ID change. They handle more than MusicBrainz imports (general-purpose share-sheet entry point into the app), so the current name is misleading.

| Current | New |
|---|---|
| `apps/MusicBrainzImporter/` | `apps/ShareImporter/` |
| `apps/MusicBrainzImporterMac/` | `apps/ShareImporterMac/` |
| Xcode target `MusicBrainzImporter` | `ShareImporter` |
| Xcode target `MusicBrainzImporterMac` | `ShareImporterMac` |

Filesystem moves via `git mv` to preserve history. pbxproj needs updates for `PRODUCT_NAME` / target-name entries, `path = MusicBrainzImporter*` group and file-reference entries, build-phase file references under the renamed groups, and the embed-app-extensions copy-files phase references from the main iOS/Mac targets. Grep for remaining literal references to `MusicBrainzImporter` / `MusicBrainzImporterMac` (Logger fallback strings, CLAUDE.md repo map, etc.) and update.

### Apple Developer portal actions required

1. Register each new App ID in **Certificates, Identifiers & Profiles → Identifiers → App IDs**.
2. For App IDs that need Sign in with Apple, enable the capability and configure it (match primary App ID / App ID Group so users retain the same `sub` where feasible).
3. Create new provisioning profiles for every target (dev + release, iOS + Mac).
4. Create a new App Group `group.com.approachnote.shared` (Tier 3) and add every target.
5. Update the existing Services ID `com.approachnote.admin-web` → its "Primary App ID" should point at the new iOS main App ID (since that's where users sign in).

### App Store Connect implications

- **Bundle IDs cannot be changed on existing App Store / TestFlight records.** A rename forces a new app record.
- Existing TestFlight testers must be re-invited to the new record.
- The old record must either be sunset or ported as a compatibility build that prompts users to install the new app.
- If the app hasn't shipped to the public App Store yet (only internal TestFlight), this cost is low. If it has, factor in review-cycle time.

### Sign in with Apple identity continuity

Apple's `sub` claim is per-App-ID. Changing bundle IDs → existing users who signed in with Apple will receive a **new** `sub` on first login to the renamed app, which **will not match** their existing `users.apple_id` row → they'll get a brand new user account (losing repertoires, favorites, etc.).

**Mitigation options**, in order of preference:
1. **App ID Group the old and new bundle IDs** under the same Primary App ID. Apple will return the same `sub` for users signed in to either App ID. This is the recommended path. Requires that both App IDs be Services-ID-grouped and the primary remain one of them.
2. **Email-based linking**: on first Apple sign-in with an unknown `sub`, fall back to matching on email; if a user with that email exists and has an existing `apple_id`, update the row with the new `sub`. Works only if Apple returns the email (it only does so on first sign-in, unless the user has granted email scope previously).
3. **Opt-in account-recovery UI**: "already have an account? log in with email/password to link your new Apple ID". Ugly.

**Recommendation:** Tier 3 + Tier 4 as a single focused PR + Apple Developer configuration sprint, protected behind option 1 above. Deploy to a fresh App Store Connect record and invite TestFlight users to the new one. Include a backend change (extend `/auth/apple`) to handle option 2 as a safety net.

---

## Tier 5 — Repo + local directory + Xcode project file rename (highest friction)

### GitHub repo

`dprodger/JazzReference` → `dprodger/ApproachNote`.

- GitHub provides automatic redirects on old URLs indefinitely for stars/issues but the canonical URL changes.
- Every local clone needs `git remote set-url origin git@github.com:dprodger/ApproachNote.git`.
- Any external references (blog posts, README badges, deployment hooks that pull by repo URL) need updating.

### Local directory

`/Users/drodger/dev/JazzReference` → `/Users/drodger/dev/ApproachNote`.

- Moves `.claude/` local state, shell aliases, Xcode recent-projects list.
- `CLAUDE.md` references `JazzReference` as the working directory name — update the path in auto-memory after the move.

### Xcode project file

`apps/Approach Note.xcodeproj` is already named correctly. Its internal `project.pbxproj` has many `JazzReference*` identifiers (covered in Tier 2).

**Recommendation:** last step — do it after all code-level references are clean. Repo rename before code rename creates a churn moment where the repo name and contents disagree; do code first.

---

## Critical coupling between tiers

- **Keychain service rename (T3) and bundle-ID rename (T4) interact**: Keychain items are already keyed to the bundle's access group. Changing the bundle ID effectively orphans all existing Keychain entries regardless of whether the service string changes. So ship both together with a single migration shim, or not at all until you're ready to walk users through re-login.
- **App Group rename (T3) requires entitlements regeneration → so does bundle-ID rename (T4)**. Do once.
- **Sign in with Apple + bundle-ID rename**: the `users.apple_id` mismatch is the single most user-visible side effect of T4. Address it in the same PR.
- **Tier 2 (Xcode rename) should land before Tier 4**: you want the pbxproj already free of legacy target names before regenerating provisioning profiles, so profile names match.

## Suggested execution order

1. **Tier 1** (today, one PR): cosmetic comments and docs. Safe, builds confidence.
2. **Tier 2 part A**: `JazzTheme` → `ApproachNoteTheme`. Large but mechanical; one Xcode rename-symbol.
3. **Tier 2 part B**: Swift file renames (`JazzReferenceApp.swift`, entitlements files) + Xcode target renames (`JazzReferenceTests` → `ApproachNoteTests`). Compiles, tests still pass.
4. **Tier 3 + 4 together**: new App Group, new bundle IDs, Keychain migration shim, `/auth/apple` email-fallback linking. Requires Apple Developer work. Plan for a TestFlight-record migration.
5. **Tier 5**: repo rename, directory rename, final doc cleanup. Trivial once the code is settled.
