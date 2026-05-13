//
//  ApproachNoteMacApp.swift
//  Approach Note
//
//  macOS app entry point
//

import SwiftUI
import os
#if canImport(GoogleSignIn)
import GoogleSignIn
#endif

@main
struct ApproachNoteMacApp: App {
    @StateObject private var authManager = AuthenticationManager()
    @StateObject private var repertoireManager = RepertoireManager()
    @StateObject private var favoritesManager = FavoritesManager()

    // Onboarding state - persisted across launches
    @AppStorage("hasCompletedOnboarding") private var hasCompletedOnboarding = false
    @State private var showingOnboarding = false

    // Deep link state for password reset
    @State private var resetPasswordToken: String?
    @State private var showResetPasswordSheet = false

    // Import state from Share Extension
    @State private var importedArtistData: ImportedArtistData?
    @State private var importedSongData: ImportedSongData?
    @State private var importedYouTubeData: ImportedYouTubeData?

    // Deep link navigation state
    @State private var deepLinkSongId: String?
    @State private var deepLinkArtistId: String?

    @Environment(\.scenePhase) var scenePhase

    var body: some Scene {
        WindowGroup {
            ContentView()
                .handlesExternalEvents(preferring: ["approachnote"], allowing: ["*"])
                .environmentObject(authManager)
                .environmentObject(repertoireManager)
                .environmentObject(favoritesManager)
                .onAppear {
                    // Connect managers to AuthenticationManager
                    repertoireManager.setAuthManager(authManager)
                    favoritesManager.setAuthManager(authManager)

                    #if canImport(GoogleSignIn)
                    // Restore Google Sign-In session if available
                    GIDSignIn.sharedInstance.restorePreviousSignIn { user, error in
                        if let error = error {
                            Log.ui.error("Google Sign-In restore error: \(error.localizedDescription)")
                        }
                    }
                    #endif

                    // Show onboarding on first launch
                    if !hasCompletedOnboarding {
                        showingOnboarding = true
                    }
                }
                .onChange(of: authManager.isAuthenticated) { _, isAuthenticated in
                    Task {
                        await repertoireManager.loadRepertoires()
                        if isAuthenticated {
                            await favoritesManager.loadFavorites()
                        }
                    }
                }
                .onChange(of: scenePhase) { _, newPhase in
                    if newPhase == .active {
                        // Check for pending imports when app becomes active
                        if deepLinkSongId == nil && deepLinkArtistId == nil {
                            checkForImportedArtist()
                            checkForImportedSong()
                            checkForImportedYouTube()
                        }
                    }
                }
                .onOpenURL { url in
                    handleDeepLink(url)
                }
                .sheet(isPresented: $showResetPasswordSheet) {
                    if let token = resetPasswordToken {
                        MacResetPasswordView(token: token)
                            .environmentObject(authManager)
                    }
                }
                .sheet(isPresented: $showingOnboarding) {
                    MacOnboardingView(isPresented: $showingOnboarding)
                        .onDisappear {
                            // Mark onboarding as completed when dismissed
                            hasCompletedOnboarding = true
                        }
                }
                // Import sheets
                .sheet(item: $importedArtistData) { data in
                    MacArtistCreationView(importedData: data)
                        .environmentObject(authManager)
                        .frame(minWidth: 400, minHeight: 300)
                }
                .sheet(item: $importedSongData) { data in
                    MacSongCreationView(importedData: data)
                        .environmentObject(authManager)
                        .environmentObject(repertoireManager)
                        .frame(minWidth: 400, minHeight: 400)
                }
                .sheet(item: $importedYouTubeData) { data in
                    MacYouTubeImportView(youtubeData: data) {
                        // On successful import, clear the data
                        SharedYouTubeDataManager.clearSharedData()
                        importedYouTubeData = nil
                    } onCancel: {
                        SharedYouTubeDataManager.clearSharedData()
                        importedYouTubeData = nil
                    }
                    .environmentObject(authManager)
                    .frame(minWidth: 500, minHeight: 500)
                }
                // Deep link navigation sheets
                .sheet(item: Binding(
                    get: { deepLinkSongId.map { DeepLinkData(id: $0) } },
                    set: { deepLinkSongId = $0?.id }
                )) { data in
                    MacSongDetailSheet(songId: data.id)
                        .environmentObject(repertoireManager)
                }
                .sheet(item: Binding(
                    get: { deepLinkArtistId.map { DeepLinkData(id: $0) } },
                    set: { deepLinkArtistId = $0?.id }
                )) { data in
                    MacPerformerDetailSheet(performerId: data.id)
                }
        }
        .handlesExternalEvents(matching: ["approachnote", "*"]) // Route URLs to existing window
        .windowStyle(.automatic)
        .defaultSize(width: 1200, height: 800)
        .commands {
            // Add menu commands
            CommandGroup(replacing: .newItem) { }

            CommandMenu("View") {
                Button("Songs") {
                    NotificationCenter.default.post(name: .navigateToSongs, object: nil)
                }
                .keyboardShortcut("1", modifiers: .command)

                Button("Artists") {
                    NotificationCenter.default.post(name: .navigateToArtists, object: nil)
                }
                .keyboardShortcut("2", modifiers: .command)

                Button("Recordings") {
                    NotificationCenter.default.post(name: .navigateToRecordings, object: nil)
                }
                .keyboardShortcut("3", modifiers: .command)
            }
        }

        #if os(macOS)
        Settings {
            SettingsView()
                .environmentObject(authManager)
                .environmentObject(favoritesManager)
        }
        #endif
    }

    // MARK: - Deep Link Handling

    private func handleDeepLink(_ url: URL) {
        NSLog("🔗 Received deep link: %@", url.absoluteString)

        // Handle Google Sign-In callback
        #if canImport(GoogleSignIn)
        if GIDSignIn.sharedInstance.handle(url) {
            return
        }
        #endif

        guard url.scheme == "approachnote" else { return }

        // Handle password reset: approachnote://auth/reset-password?token=xyz
        if url.host == "auth" && url.path == "/reset-password" {
            if let components = URLComponents(url: url, resolvingAgainstBaseURL: false),
               let token = components.queryItems?.first(where: { $0.name == "token" })?.value {
                resetPasswordToken = token
                showResetPasswordSheet = true
            }
            return
        }

        // Handle artist import: approachnote://import-artist
        if url.host == "import-artist" {
            NSLog("🎵 Artist import deep link detected")
            checkForImportedArtist()
            return
        }

        // Handle song import: approachnote://import-song
        if url.host == "import-song" {
            NSLog("🎵 Song import deep link detected")
            checkForImportedSong()
            return
        }

        // Handle YouTube import: approachnote://import-youtube
        if url.host == "import-youtube" {
            NSLog("🎬 YouTube import deep link detected")
            checkForImportedYouTube()
            return
        }

        // Handle song view: approachnote://song/{songId}
        if url.host == "song" {
            let songId = url.path.trimmingCharacters(in: CharacterSet(charactersIn: "/"))
            NSLog("🎵 Song deep link detected: %@", songId)
            if !songId.isEmpty {
                importedSongData = nil
                importedArtistData = nil
                deepLinkSongId = songId
            }
            return
        }

        // Handle artist view: approachnote://artist/{artistId}
        if url.host == "artist" {
            let artistId = url.path.trimmingCharacters(in: CharacterSet(charactersIn: "/"))
            NSLog("🎵 Artist deep link detected: %@", artistId)
            if !artistId.isEmpty {
                importedSongData = nil
                importedArtistData = nil
                deepLinkArtistId = artistId
            }
            return
        }

        NSLog("❓ Unrecognized deep link format: %@", url.absoluteString)
    }

    // MARK: - Import Checking

    private func checkForImportedArtist() {
        if let data = SharedArtistDataManager.retrieveSharedData() {
            NSLog("✅ Imported artist data detected: %@", data.name)
            importedArtistData = data
        }
    }

    private func checkForImportedSong() {
        if let data = SharedSongDataManager.retrieveSharedData() {
            NSLog("✅ Imported song data detected: %@", data.title)
            importedSongData = data
        }
    }

    private func checkForImportedYouTube() {
        guard importedYouTubeData == nil else {
            NSLog("ℹ️ YouTube import already in progress, skipping check")
            return
        }

        if let data = SharedYouTubeDataManager.retrieveSharedData() {
            NSLog("✅ Imported YouTube data detected: %@", data.title)
            SharedYouTubeDataManager.clearSharedData()
            importedYouTubeData = data
            NSLog("✅ Set importedYouTubeData, sheet should show")
        }
    }
}

// MARK: - Helper Types

struct DeepLinkData: Identifiable {
    let id: String
}

// Placeholder views for deep link navigation
// These should be replaced with actual views or navigate to existing views
struct MacSongDetailSheet: View {
    let songId: String
    @EnvironmentObject var repertoireManager: RepertoireManager
    @Environment(\.dismiss) var dismiss

    var body: some View {
        VStack {
            Text("Song Detail")
                .font(ApproachNoteTheme.headline())
            Text("Song ID: \(songId)")
                .font(ApproachNoteTheme.caption())
                .foregroundColor(.secondary)
            Button("Close") {
                dismiss()
            }
            .padding()
        }
        .frame(minWidth: 400, minHeight: 300)
    }
}

struct MacPerformerDetailSheet: View {
    let performerId: String
    @Environment(\.dismiss) var dismiss

    var body: some View {
        VStack {
            Text("Artist Detail")
                .font(ApproachNoteTheme.headline())
            Text("Artist ID: \(performerId)")
                .font(ApproachNoteTheme.caption())
                .foregroundColor(.secondary)
            Button("Close") {
                dismiss()
            }
            .padding()
        }
        .frame(minWidth: 400, minHeight: 300)
    }
}

// MARK: - Navigation Notifications

extension Notification.Name {
    static let navigateToSongs = Notification.Name("navigateToSongs")
    static let navigateToArtists = Notification.Name("navigateToArtists")
    static let navigateToRecordings = Notification.Name("navigateToRecordings")
}
