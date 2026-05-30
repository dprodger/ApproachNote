//
//  FavoritesManager.swift
//  Approach Note
//
//  Manages user's favorite recordings with automatic token refresh
//

import SwiftUI
import Combine
import os

/// Manages the user's favorited recordings
@MainActor
class FavoritesManager: ObservableObject {
    /// IDs of favorited recordings (for quick lookup)
    @Published var favoriteRecordingIds: Set<String> = []

    /// Full list of favorite recordings (for Settings display)
    @Published var favoriteRecordings: [FavoriteRecordingResponse] = []

    /// Whether favorites are being loaded
    @Published var isLoading = false

    /// Error message if loading fails
    @Published var errorMessage: String?

    /// Whether user is authenticated
    @Published var isAuthenticated = false

    private let favoritesService = FavoritesService()

    // Reference to AuthenticationManager (set by app)
    private weak var authManager: AuthenticationManager?

    init() {
        // Nothing to restore from UserDefaults - favorites are server-side
    }

    /// Connect to AuthenticationManager and load favorites if authenticated
    func setAuthManager(_ authManager: AuthenticationManager) {
        self.authManager = authManager
        self.isAuthenticated = authManager.isAuthenticated

        // Load favorites if already authenticated
        if authManager.isAuthenticated {
            Task {
                await loadFavorites()
            }
        }
    }

    /// Load all favorites from the API (requires authentication)
    func loadFavorites() async {
        // Skip if already loading
        guard !isLoading else {
            Log.data.debug("Already loading favorites - skipping duplicate request")
            return
        }

        // Check authentication
        guard let authManager = authManager,
              authManager.isAuthenticated,
              let token = await authManager.validAccessToken() else {
            await MainActor.run {
                self.favoriteRecordingIds = []
                self.favoriteRecordings = []
                self.isAuthenticated = false
                Log.data.debug("Not authenticated - clearing favorites")
            }
            return
        }

        await MainActor.run {
            isLoading = true
            errorMessage = nil
        }

        let favorites = await favoritesService.fetchUserFavorites(authToken: token)

        await MainActor.run {
            self.favoriteRecordings = favorites
            self.favoriteRecordingIds = Set(favorites.map { $0.id })
            self.isLoading = false
            self.isAuthenticated = true
        }
    }

    /// Check if a recording is favorited
    func isFavorited(_ recordingId: String) -> Bool {
        favoriteRecordingIds.contains(recordingId)
    }

    /// Toggle favorite status for a recording
    /// - Returns: The new favorite count, or nil on error
    @discardableResult
    func toggleFavorite(recordingId: String) async -> Int? {
        guard let authManager = authManager,
              let token = await authManager.validAccessToken() else {
            await MainActor.run {
                errorMessage = "Please sign in to favorite recordings"
            }
            return nil
        }

        let wasFavorited = isFavorited(recordingId)

        // Optimistic UI update
        await MainActor.run {
            if wasFavorited {
                favoriteRecordingIds.remove(recordingId)
                favoriteRecordings.removeAll { $0.id == recordingId }
            } else {
                favoriteRecordingIds.insert(recordingId)
            }
        }

        let result: Int?
        if wasFavorited {
            result = await favoritesService.removeFavorite(recordingId: recordingId, authToken: token)
        } else {
            result = await favoritesService.addFavorite(recordingId: recordingId, authToken: token)
        }

        if result == nil {
            // Revert optimistic update on failure
            await MainActor.run {
                if wasFavorited {
                    favoriteRecordingIds.insert(recordingId)
                } else {
                    favoriteRecordingIds.remove(recordingId)
                }
                errorMessage = wasFavorited ? "Failed to remove favorite" : "Failed to add favorite"
            }

            // Reload favorites to ensure consistency
            await loadFavorites()
        } else {
            // If we added a favorite, reload to get the full recording data
            if !wasFavorited {
                await loadFavorites()
            }
            Log.data.debug("Favorite toggled for recording \(recordingId, privacy: .private)")
        }

        return result
    }

    /// Add a recording to favorites
    /// - Returns: The new favorite count, or nil on error
    func addFavorite(recordingId: String) async -> Int? {
        guard let authManager = authManager,
              let token = await authManager.validAccessToken() else {
            await MainActor.run {
                errorMessage = "Please sign in to favorite recordings"
            }
            return nil
        }

        // Optimistic UI update
        favoriteRecordingIds.insert(recordingId)

        let result = await favoritesService.addFavorite(recordingId: recordingId, authToken: token)

        if result == nil {
            // Revert on failure
            await MainActor.run {
                favoriteRecordingIds.remove(recordingId)
                errorMessage = "Failed to add favorite"
            }
        } else {
            // Reload to get full recording data
            await loadFavorites()
            Log.data.debug("Added favorite: \(recordingId, privacy: .private)")
        }

        return result
    }

    /// Remove a recording from favorites
    /// - Returns: The new favorite count, or nil on error
    func removeFavorite(recordingId: String) async -> Int? {
        guard let authManager = authManager,
              let token = await authManager.validAccessToken() else {
            await MainActor.run {
                errorMessage = "Please sign in to manage favorites"
            }
            return nil
        }

        // Optimistic UI update
        await MainActor.run {
            favoriteRecordingIds.remove(recordingId)
            favoriteRecordings.removeAll { $0.id == recordingId }
        }

        let result = await favoritesService.removeFavorite(recordingId: recordingId, authToken: token)

        if result == nil {
            // Revert on failure
            await MainActor.run {
                favoriteRecordingIds.insert(recordingId)
                errorMessage = "Failed to remove favorite"
            }
            // Reload to ensure consistency
            await loadFavorites()
        } else {
            Log.data.debug("Removed favorite: \(recordingId, privacy: .private)")
        }

        return result
    }

    /// Clear all favorites state (call on logout)
    func clearFavorites() {
        favoriteRecordingIds = []
        favoriteRecordings = []
        isAuthenticated = false
        Log.data.debug("Favorites cleared")
    }

    /// Number of favorited recordings
    var favoriteCount: Int {
        favoriteRecordingIds.count
    }
}
