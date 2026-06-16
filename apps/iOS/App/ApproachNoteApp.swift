// ApproachNoteApp.swift
// Main app entry point with deep link handling


import Foundation
import PostHog
import UIKit
import SwiftUI
import GoogleSignIn
import os

@main
struct ApproachNoteApp: App {
    @UIApplicationDelegateAdaptor(AppDelegate.self) var appDelegate
    @State private var showingArtistCreation = false
    @State private var importedArtistData: ImportedArtistData?
    @Environment(\.scenePhase) var scenePhase
    @State private var showingSongCreation = false
    @State private var importedSongData: ImportedSongData?
    @State private var importedYouTubeData: ImportedYouTubeData?
    @StateObject private var repertoireManager = RepertoireManager()
    @StateObject private var authManager = AuthenticationManager()
    @StateObject private var favoritesManager = FavoritesManager()

    // Password reset state
    @State private var resetPasswordToken: String?

    // Deep link navigation state
    @State private var deepLinkSongId: String?
    @State private var deepLinkArtistId: String?
    // Optional `?screenshot=<anchor>` carried alongside a song deep link, used
    // to open the screen pre-scrolled for App Store screenshot capture.
    @State private var deepLinkScreenshotAnchor: String?

    // Onboarding state - persisted across launches
    @AppStorage("hasCompletedOnboarding") private var hasCompletedOnboarding = false
    @State private var showingOnboarding = false

    init() {
        // Configure navigation bar fonts from ApproachNoteTheme
        ApproachNoteTheme.configureNavigationBarAppearance()

        // Restore previous Google Sign-In session (skip in previews)
        #if DEBUG
        if ProcessInfo.processInfo.environment["XCODE_RUNNING_FOR_PREVIEWS"] != "1" {
            GIDSignIn.sharedInstance.restorePreviousSignIn { user, error in
                if error != nil || user == nil {
                    // User is not signed in
                } else {
                    // User is signed in
                }
            }
        }
        #else
        GIDSignIn.sharedInstance.restorePreviousSignIn { user, error in
            if error != nil || user == nil {
                // User is not signed in
            } else {
                // User is signed in
            }
        }
        #endif
    }
    
    var body: some Scene {
        WindowGroup {
            ContentView()
                .onOpenURL { url in
                    NSLog("🔗 Received deep link: \(url)")
                    
                    // Handle password reset: approachnote://auth/reset-password?token=xyz
                    if url.scheme == "approachnote" && url.host == "auth" && url.path == "/reset-password" {
                        NSLog("🔑 Password reset deep link detected")
                        
                        // Extract token from query parameters
                        if let components = URLComponents(url: url, resolvingAgainstBaseURL: false),
                           let queryItems = components.queryItems,
                           let tokenItem = queryItems.first(where: { $0.name == "token" }),
                           let token = tokenItem.value {
                            NSLog("✅ Found reset token, showing ResetPasswordView")
                            resetPasswordToken = token
                        } else {
                            NSLog("❌ No token found in reset password deep link")
                        }
                    }
                    // Handle artist import: approachnote://import-artist
                    else if url.scheme == "approachnote" && url.host == "import-artist" {
                        NSLog("🎵 Artist import deep link detected")
                        checkForImportedArtist()
                    }
                    // Handle song import: approachnote://import-song
                    else if url.scheme == "approachnote" && url.host == "import-song" {
                        NSLog("🎵 Song import deep link detected")
                        checkForImportedSong()
                    }
                    // Handle YouTube import: approachnote://import-youtube
                    else if url.scheme == "approachnote" && url.host == "import-youtube" {
                        NSLog("🎬 YouTube import deep link detected")
                        checkForImportedYouTube()
                    }
                    // Handle song view: approachnote://song/{songId}
                    else if url.scheme == "approachnote" && url.host == "song" {
                        let songId = url.path.trimmingCharacters(in: CharacterSet(charactersIn: "/"))
                        NSLog("🎵 Song deep link detected: %@", songId)
                        if !songId.isEmpty {
                            // Clear any pending import data to avoid sheet conflicts
                            importedSongData = nil
                            importedArtistData = nil
                            deepLinkScreenshotAnchor = screenshotAnchor(from: url)
                            deepLinkSongId = songId
                        }
                    }
                    // Handle artist view: approachnote://artist/{artistId}
                    else if url.scheme == "approachnote" && url.host == "artist" {
                        let artistId = url.path.trimmingCharacters(in: CharacterSet(charactersIn: "/"))
                        NSLog("🎵 Artist deep link detected: %@", artistId)
                        if !artistId.isEmpty {
                            // Clear any pending import data to avoid sheet conflicts
                            importedSongData = nil
                            importedArtistData = nil
                            deepLinkArtistId = artistId
                        }
                    }
                    else {
                        NSLog("❓ Unrecognized deep link format: \(url)")
                    }
                }
                .onAppear {
                    // PHASE 5: Connect RepertoireManager to AuthenticationManager
                    repertoireManager.setAuthManager(authManager)
                    Log.ui.debug("Connected RepertoireManager to AuthenticationManager")

                    // Connect FavoritesManager to AuthenticationManager
                    favoritesManager.setAuthManager(authManager)
                    Log.ui.debug("Connected FavoritesManager to AuthenticationManager")

                    // Show onboarding on first launch
                    if !hasCompletedOnboarding {
                        showingOnboarding = true
                    }
                }
                .onChange(of: scenePhase) { oldPhase, newPhase in
                    if newPhase == .active {
                        // Only check for imports if we're not handling a direct deep link
                        // This prevents sheet conflicts
                        if deepLinkSongId == nil && deepLinkArtistId == nil {
                            checkForImportedArtist()
                            checkForImportedSong()
                            checkForImportedYouTube()
                        }
                    }
                }
                .onChange(of: authManager.isAuthenticated) { wasAuthenticated, isAuthenticated in
                    // PHASE 5: Update repertoire manager when auth state changes
                    if isAuthenticated {
                        Log.ui.debug("User authenticated - loading repertoires")
                        Task {
                            await repertoireManager.loadRepertoires()
                        }
                        Log.ui.debug("User authenticated - loading favorites")
                        Task {
                            await favoritesManager.loadFavorites()
                        }
                    } else {
                        Log.ui.debug("User logged out - clearing repertoires")
                        Task {
                            await repertoireManager.loadRepertoires()
                        }
                        Log.ui.debug("User logged out - clearing favorites")
                        favoritesManager.clearFavorites()
                    }
                }
                .sheet(item: $importedArtistData) { data in
                    // CHANGED: Use .sheet(item:) instead of isPresented
                    NavigationStack {
                        ArtistCreationView(importedData: data)
                    }
                    .environmentObject(authManager)
                }
                .sheet(item: $importedSongData) { data in
                    NavigationStack {
                        SongCreationView(importedData: data)
                    }
                    .environmentObject(authManager)
                    .environmentObject(repertoireManager)
                }
                .sheet(item: Binding(
                    get: { resetPasswordToken.map { ResetPasswordData(token: $0) } },
                    set: { resetPasswordToken = $0?.token }
                )) { data in
                    ResetPasswordView(token: data.token)
                        .environmentObject(authManager)
                }
                .sheet(item: Binding(
                    get: { deepLinkSongId.map { DeepLinkSongData(songId: $0, screenshotAnchor: deepLinkScreenshotAnchor) } },
                    set: { newValue in
                        deepLinkSongId = newValue?.songId
                        if newValue == nil { deepLinkScreenshotAnchor = nil }
                    }
                )) { data in
                    NavigationStack {
                        SongDetailView(songId: data.songId, screenshotAnchor: data.screenshotAnchor)
                            .environmentObject(repertoireManager)
                    }
                }
                .sheet(item: Binding(
                    get: { deepLinkArtistId.map { DeepLinkArtistData(artistId: $0) } },
                    set: { deepLinkArtistId = $0?.artistId }
                )) { data in
                    NavigationStack {
                        PerformerDetailView(performerId: data.artistId)
                    }
                }
                .sheet(item: $importedYouTubeData) { data in
                    NavigationStack {
                        YouTubeImportView(youtubeData: data) {
                            // On successful import, clear the data
                            SharedYouTubeDataManager.clearSharedData()
                            importedYouTubeData = nil
                        } onCancel: {
                            SharedYouTubeDataManager.clearSharedData()
                            importedYouTubeData = nil
                        }
                    }
                }
                .fullScreenCover(isPresented: $showingOnboarding) {
                    OnboardingView(isPresented: $showingOnboarding)
                        .onDisappear {
                            // Mark onboarding as completed when dismissed
                            hasCompletedOnboarding = true
                        }
                }
                .ignoresSafeArea()
                .environmentObject(authManager)
                .onOpenURL { url in
                    GIDSignIn.sharedInstance.handle(url)
                }
                .environmentObject(repertoireManager)
                .environmentObject(favoritesManager)
        }
    }
    
    /// Extracts the optional `?screenshot=<anchor>` query value from a deep link.
    private func screenshotAnchor(from url: URL) -> String? {
        URLComponents(url: url, resolvingAgainstBaseURL: false)?
            .queryItems?
            .first(where: { $0.name == "screenshot" })?
            .value
    }

    private func checkForImportedArtist() {
        if let data = SharedArtistDataManager.retrieveSharedData() {
            NSLog("📥 Imported artist data detected: %@", data.name)
            importedArtistData = data
            showingArtistCreation = true
        }
    }
    
    private func checkForImportedSong() {
        if let data = SharedSongDataManager.retrieveSharedData() {
            NSLog("📥 Imported song data detected: %@", data.title)
            importedSongData = data
            showingSongCreation = true
        }
    }

    private func checkForImportedYouTube() {
        // Don't check if we already have YouTube data being displayed
        guard importedYouTubeData == nil else {
            NSLog("ℹ️ YouTube import already in progress, skipping check")
            return
        }

        if let data = SharedYouTubeDataManager.retrieveSharedData() {
            NSLog("📥 Imported YouTube data detected: %@", data.title)
            // Clear the shared data immediately to prevent duplicate imports
            SharedYouTubeDataManager.clearSharedData()
            importedYouTubeData = data
        }
    }
    
    // Handle URL for Google Sign In
    func application(_ app: UIApplication,
                    open url: URL,
                    options: [UIApplication.OpenURLOptionsKey: Any] = [:]) -> Bool {
        return GIDSignIn.sharedInstance.handle(url)
    }

}





class AppDelegate: NSObject, UIApplicationDelegate {
    func application(_: UIApplication, didFinishLaunchingWithOptions _: [UIApplication.LaunchOptionsKey: Any]? = nil) -> Bool {
        let POSTHOG_PROJECT_TOKEN = "phc_x5LR6pDRx6iPpXuSkKGh2NuteruD86yFY4YnEJeWyYoF"
        let POSTHOG_HOST = "https://us.i.posthog.com"

        let config = PostHogConfig(projectToken: POSTHOG_PROJECT_TOKEN, host: POSTHOG_HOST)

        NSLog("within posthog app delegate")
        // Enable session recording. Requires enabling in your project settings as well.
        // Default is false.
        config.sessionReplay = true

        // We opt-out of the aggressive global masking. PostHog still auto-masks
        // password/OTP/credit-card fields via heuristics; login/signup views are
        // additionally wrapped in .postHogMask() at the view layer.
        config.sessionReplayConfig.maskAllTextInputs = false
        config.sessionReplayConfig.maskAllImages = false

        // Whether logs are captured in recordings. Default is false.
        //
        // Support for remote configuration
        // in the [session replay settings](https://app.posthog.com/settings/project-replay#replay-log-capture)
        // requires SDK version 3.41.1 or higher.
        config.sessionReplayConfig.captureLogs = false

        // Whether network requests are captured in recordings. Default is true
        // Only metric-like data like speed, size, and response code are captured.
        // No data is captured from the request or response body.
        //
        // Support for remote configuration
        // in the [session replay settings](https://app.posthog.com/settings/project-replay#replay-network)
        // requires SDK version 3.41.1 or higher.
        config.sessionReplayConfig.captureNetworkTelemetry = true

        // Whether replays are created using high quality screenshots. Default is false.
        // Required for SwiftUI.
        // If disabled, replays are created using wireframes instead.
        // The screenshot may contain sensitive information, so use with caution
        config.sessionReplayConfig.screenshotMode = true

        // Sample rate for session recordings. A value between 0.0 and 1.0.
        // 1.0 means 100% of sessions will be recorded. 0.5 means 50%, and so on.
        // Default is nil (all sessions are recorded).
        //
        // Support for remote configuration
        // in the [session replay triggers](https://us.posthog.com/settings/project-replay#replay-triggers)
        // requires SDK version 3.42.0 or higher.
        config.sessionReplayConfig.sampleRate = nil

        PostHogSDK.shared.setup(config)

        return true
    }
}
// MARK: - Helper Structs

// Helper struct for password reset sheet binding
struct ResetPasswordData: Identifiable {
    let id = UUID()
    let token: String
}

// Helper struct for deep link song navigation
struct DeepLinkSongData: Identifiable {
    let id = UUID()
    let songId: String
    // Optional screenshot anchor (e.g. "featured", "recordings") that opens the
    // song screen pre-scrolled for App Store screenshot capture.
    var screenshotAnchor: String? = nil
}

// Helper struct for deep link artist navigation
struct DeepLinkArtistData: Identifiable {
    let id = UUID()
    let artistId: String
}

