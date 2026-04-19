# Mac App - Xcode Setup Instructions

This document explains how to add the macOS app target to your existing Xcode project.

## Overview

The macOS app shares code with the iOS app via the `Shared/` directory:
- **Shared files** (in `Shared/`): Models, NetworkManager, ApproachNoteTheme, AuthenticationManager, etc.
- **macOS-specific files** (in `Mac/`): ApproachNoteMacApp.swift, Views/*, Auth/*

## Step-by-Step Setup

### Step 1: Open the Project in Xcode

Open `apps/Approach Note.xcodeproj` in Xcode.

### Step 2: Add a New macOS App Target

1. In Xcode, go to **File → New → Target...**
2. Select **macOS** tab at the top
3. Choose **App** and click **Next**
4. Configure the target:
   - **Product Name**: `ApproachNoteMac`
   - **Team**: Your development team (FX893D85BJ)
   - **Organization Identifier**: `me.rodger.david`
   - **Bundle Identifier**: `com.approachnote.mac`
   - **Interface**: SwiftUI
   - **Language**: Swift
   - **Storage**: None
   - Uncheck "Include Tests"
5. Click **Finish**

### Step 3: Delete Auto-Generated Files

Xcode will create a default `ApproachNoteMac` folder with boilerplate files. Delete these:
- Delete the auto-generated folder from the project
- Keep the target, but we'll use our prepared files instead

### Step 4: Add the Prepared Mac Folder

1. Right-click on the project root in the navigator
2. Select **Add Files to "Approach Note"...**
3. Navigate to the `Mac` folder we created
4. Make sure:
   - **Copy items if needed** is UNCHECKED
   - **Create groups** is selected
   - **Add to targets**: Only check `ApproachNoteMac`
5. Click **Add**

### Step 5: Configure Shared Files

Shared files are now in the `Shared/` directory and automatically included in both targets via Xcode's synchronized groups. No manual configuration needed.

The shared files include:
- `Shared/Support/Models.swift`
- `Shared/Support/NetworkManager.swift`
- `Shared/Support/ApproachNoteTheme.swift`
- `Shared/Support/HelperViews.swift`
- `Shared/Support/PreviewHelpers.swift`
- `Shared/Auth/AuthenticationManager.swift`
- `Shared/Auth/KeychainHelper.swift`
- `Shared/Auth/Models/User.swift`
- `Shared/Managers/FavoritesManager.swift`
- `Shared/Managers/RepertoireManager.swift`

### Step 6: Handle Platform-Specific Code

The `AuthenticationManager.swift` has iOS-specific code for Google Sign-In. You have two options:

**Option A: Conditional Compilation (Recommended)**

Edit `AuthenticationManager.swift` to wrap iOS-specific code:

```swift
#if os(iOS)
import GoogleSignIn
#endif

// ... and wrap the signInWithGoogle method:

#if os(iOS)
@MainActor
func signInWithGoogle() async -> Bool {
    // existing iOS code
}
#else
@MainActor
func signInWithGoogle() async -> Bool {
    // macOS placeholder - implement later if needed
    errorMessage = "Google Sign-In not yet available on macOS"
    return false
}
#endif
```

**Option B: Create macOS-specific AuthenticationManager**

Copy `AuthenticationManager.swift` to the macOS folder and modify it for macOS.

### Step 7: Configure Build Settings

Select the `ApproachNoteMac` target and configure:

1. **General Tab:**
   - Deployment Target: macOS 14.0 (or your minimum)
   - App Category: Music

2. **Signing & Capabilities:**
   - Team: Your team
   - Signing Certificate: Sign to Run Locally (for development)
   - Check **Hardened Runtime** if distributing

3. **Build Settings:**
   - Search for `INFOPLIST_FILE` and set to: `Mac/App/Info.plist`
   - Search for `CODE_SIGN_ENTITLEMENTS` and set to: `Mac/App/ApproachNoteMac.entitlements`
   - Search for `ASSETCATALOG_COMPILER_APPICON_NAME` and set to: `AppIcon`

### Step 8: Fix Any Compilation Errors

Common issues and fixes:

1. **UIImage not found**: The iOS app's `CachedAsyncImage.swift` uses UIImage. Don't add it to macOS target - use SwiftUI's built-in `AsyncImage` instead (which we do in the macOS views).

2. **UIApplication not found**: Wrap in `#if os(iOS)` or use macOS equivalents.

3. **UIImpactFeedbackGenerator**: This is iOS-only haptics. The macOS views don't use it.

### Step 9: Add App Icon (Optional)

1. Open `Mac/Assets.xcassets/AppIcon.appiconset`
2. Add your app icons at the required sizes:
   - 16x16 @1x and @2x
   - 32x32 @1x and @2x
   - 128x128 @1x and @2x
   - 256x256 @1x and @2x
   - 512x512 @1x and @2x

### Step 10: Build and Run

1. Select the `ApproachNoteMac` scheme in the toolbar
2. Select **My Mac** as the destination
3. Press **⌘R** to build and run

## File Structure After Setup

```
apps/
├── Shared/                      # Shared between iOS and Mac
│   ├── Auth/
│   │   ├── AuthenticationManager.swift
│   │   ├── KeychainHelper.swift
│   │   └── Models/User.swift
│   ├── Managers/
│   │   ├── FavoritesManager.swift
│   │   └── RepertoireManager.swift
│   └── Support/
│       ├── Models.swift
│       ├── NetworkManager.swift
│       ├── ApproachNoteTheme.swift
│       ├── HelperViews.swift
│       └── PreviewHelpers.swift
├── iOS/                         # iOS-specific
│   ├── App/
│   ├── Auth/Views/
│   ├── Components/
│   ├── Managers/
│   ├── Support/                 # iOS-only (CachedAsyncImage, Toast)
│   └── Views/
├── Mac/                         # macOS-specific
│   ├── App/
│   │   ├── Info.plist
│   │   ├── ApproachNoteMac.entitlements
│   │   └── ApproachNoteMacApp.swift
│   ├── Auth/                    # Mac auth views
│   ├── Assets.xcassets/
│   └── Views/
│       ├── ContentView.swift
│       ├── SongsListView.swift
│       ├── ArtistsListView.swift
│       ├── RecordingsListView.swift
│       ├── SongDetailView.swift
│       ├── PerformerDetailView.swift
│       └── RecordingDetailView.swift
└── Approach Note.xcodeproj
```

## Troubleshooting

### "No such module 'GoogleSignIn'"
The macOS target doesn't need GoogleSignIn. Make sure the package is only linked to the iOS target, not macOS.

### Keychain errors
Make sure `com.apple.security.keychain-access-groups` entitlement is set if needed, or use the default keychain access.

### Network requests failing
Ensure `com.apple.security.network.client` is set to `true` in entitlements (already configured).

## Next Steps

After basic setup works:
1. Add macOS-specific features (menu bar items, keyboard shortcuts)
2. Implement Google Sign-In for macOS (if needed)
3. Add Touch Bar support (if applicable)
4. Consider adding a menu bar utility app
