//
//  RepertoireManager.swift
//  Approach Note
//
//  Manages repertoire selection and persistence across app launches
//  UPDATED FOR AUTOMATIC TOKEN REFRESH: Uses AuthenticationManager for all API calls
//

import SwiftUI
import Combine
import os

/// Manages the current repertoire selection and provides repertoire list
@MainActor
class RepertoireManager: ObservableObject {
    /// All available repertoires (including "All Songs")
    @Published var repertoires: [Repertoire] = []
    
    /// Currently selected repertoire for viewing
    @Published var selectedRepertoire: Repertoire = .allSongs
    
    /// Last repertoire used for adding songs (for quick-add)
    @Published var lastUsedRepertoire: Repertoire?
    
    /// Whether repertoires are being loaded
    @Published var isLoading = false
    
    /// Error message if loading fails
    @Published var errorMessage: String?
    
    /// Whether user is authenticated (updated by AuthenticationManager)
    @Published var isAuthenticated = false
    
    private let songService = SongService()
    private let selectedRepertoireKey = "selectedRepertoireId"
    private let lastUsedRepertoireKey = "lastUsedRepertoireId"
    
    // Reference to AuthenticationManager (set by app)
    private weak var authManager: AuthenticationManager?
    
    init() {
        // Repertoire IDs are restored from UserDefaults on demand elsewhere.
    }
    
    /// Connect to AuthenticationManager and load repertoires if authenticated
    func setAuthManager(_ authManager: AuthenticationManager) {
        self.authManager = authManager
        self.isAuthenticated = authManager.isAuthenticated
        
        // Load repertoires if already authenticated
        if authManager.isAuthenticated {
            Task {
                await loadRepertoires()
            }
        }
    }
    
    /// Load all repertoires from the API (requires authentication)
    func loadRepertoires() async {
        // Skip if already loading (prevents duplicate concurrent requests)
        guard !isLoading else {
            Log.data.debug("Already loading repertoires - skipping duplicate request")
            return
        }

        // Check authentication
        guard let authManager = authManager,
              authManager.isAuthenticated else {
            await MainActor.run {
                self.repertoires = [.allSongs]
                self.selectedRepertoire = .allSongs
                self.isAuthenticated = false
            }
            return
        }
        
        await MainActor.run {
            isLoading = true
            errorMessage = nil
        }
        
        let url = URL.api(path: "/repertoires/")

        do {
            // Use AuthenticationManager's method which handles token refresh automatically
            let data = try await authManager.makeAuthenticatedRequest(url: url)

            let fetchedRepertoires = try JSONDecoder().decode([Repertoire].self, from: data)
            
            await MainActor.run {
                // Add "All Songs" as first option
                self.repertoires = [.allSongs] + fetchedRepertoires
                
                // Restore previously selected repertoire if it exists
                if let savedId = UserDefaults.standard.string(forKey: selectedRepertoireKey),
                   let saved = self.repertoires.first(where: { $0.id == savedId }) {
                    self.selectedRepertoire = saved
                } else {
                    self.selectedRepertoire = .allSongs
                }

                // Restore last used repertoire if it exists
                if let lastUsedId = UserDefaults.standard.string(forKey: lastUsedRepertoireKey),
                   let lastUsed = fetchedRepertoires.first(where: { $0.id == lastUsedId }) {
                    self.lastUsedRepertoire = lastUsed
                }

                self.isLoading = false
                self.isAuthenticated = true
            }
        } catch URLError.userAuthenticationRequired {
            // Token refresh failed - user needs to log in again
            await MainActor.run {
                self.isAuthenticated = false
                self.repertoires = [.allSongs]
                self.selectedRepertoire = .allSongs
                errorMessage = "Authentication expired. Please sign in again."
                isLoading = false
                Log.data.warning("Authentication required - token refresh failed")
            }
        } catch {
            await MainActor.run {
                errorMessage = "Failed to load repertoires: \(error.localizedDescription)"
                isLoading = false
                Log.data.error("Error loading repertoires: \(error.localizedDescription)")
            }
        }
    }
    
    /// Add song to repertoire (requires authentication)
    func addSongToRepertoire(songId: String, repertoireId: String) async -> Bool {
        guard let authManager = authManager else {
            await MainActor.run {
                errorMessage = "Not authenticated"
            }
            return false
        }
        
        let url = URL.api(path: "/repertoires/\(repertoireId)/songs/\(songId)")

        Log.data.debug("Adding song \(songId, privacy: .private) to repertoire \(repertoireId, privacy: .private)")
        
        do {
            // POST request with automatic token refresh
            _ = try await authManager.makeAuthenticatedRequest(
                url: url,
                method: "POST"
            )
            
            Log.data.info("Song added to repertoire")
            // Refresh repertoires to update counts
            await loadRepertoires()
            return true
            
        } catch let error as NSError where error.code == 409 {
            // Song already in repertoire
            await MainActor.run {
                errorMessage = "Song already in repertoire"
            }
            Log.data.warning("Song already in repertoire")
            return false
            
        } catch URLError.userAuthenticationRequired {
            await MainActor.run {
                errorMessage = "Authentication expired"
                isAuthenticated = false
            }
            Log.data.warning("Authentication required")
            return false
            
        } catch {
            await MainActor.run {
                errorMessage = "Failed to add song: \(error.localizedDescription)"
            }
            Log.data.error("Failed to add song: \(error.localizedDescription)")
            return false
        }
    }
    
    /// Create new repertoire (requires authentication)
    func createRepertoire(name: String, description: String?) async -> Bool {
        guard let authManager = authManager else {
            await MainActor.run {
                errorMessage = "Not authenticated"
            }
            return false
        }
        
        let url = URL.api(path: "/repertoires/")

        var body: [String: Any] = ["name": name]
        if let desc = description {
            body["description"] = desc
        }
        
        guard let bodyData = try? JSONSerialization.data(withJSONObject: body) else {
            return false
        }
        
        Log.data.debug("Creating repertoire: \(name, privacy: .public)")
        
        do {
            _ = try await authManager.makeAuthenticatedRequest(
                url: url,
                method: "POST",
                body: bodyData
            )
            
            Log.data.info("Repertoire created: \(name, privacy: .public)")
            await loadRepertoires()
            return true
            
        } catch URLError.userAuthenticationRequired {
            await MainActor.run {
                errorMessage = "Authentication expired"
                isAuthenticated = false
            }
            return false
            
        } catch {
            await MainActor.run {
                errorMessage = "Failed to create repertoire: \(error.localizedDescription)"
            }
            Log.data.error("Failed to create repertoire: \(error.localizedDescription)")
            return false
        }
    }
    
    /// Delete repertoire (requires authentication)
    func deleteRepertoire(id: String) async -> Bool {
        guard let authManager = authManager else {
            return false
        }
        
        let url = URL.api(path: "/repertoires/\(id)")

        Log.data.debug("Deleting repertoire: \(id, privacy: .private)")
        
        do {
            _ = try await authManager.makeAuthenticatedRequest(
                url: url,
                method: "DELETE"
            )
            
            Log.data.info("Repertoire deleted")
            await loadRepertoires()
            return true
            
        } catch URLError.userAuthenticationRequired {
            await MainActor.run {
                isAuthenticated = false
            }
            return false
            
        } catch {
            Log.data.error("Failed to delete repertoire: \(error.localizedDescription)")
            return false
        }
    }
    
    func selectRepertoire(_ repertoire: Repertoire) {
        selectedRepertoire = repertoire
        
        // Save selection
        if repertoire.id != "all" {
            UserDefaults.standard.set(repertoire.id, forKey: selectedRepertoireKey)
        } else {
            UserDefaults.standard.removeObject(forKey: selectedRepertoireKey)
        }
        
        // Fetch songs with authentication if needed
        Task {
            if repertoire.id != "all", let authManager = authManager, let token = authManager.getAccessToken() {
                await songService.fetchSongsInRepertoire(
                    repertoireId: repertoire.id,
                    authToken: token
                )
            } else {
                await songService.fetchSongsInRepertoire(repertoireId: repertoire.id)
            }
        }
    }
    
    /// Update last used repertoire (when adding a song)
    func setLastUsedRepertoire(_ repertoire: Repertoire) {
        lastUsedRepertoire = repertoire
        
        // Save to UserDefaults
        UserDefaults.standard.set(repertoire.id, forKey: lastUsedRepertoireKey)
        
        let repName = repertoire.name
        Log.data.debug("Last used repertoire updated: \(repName, privacy: .public)")
    }
    
    /// Get display name for current repertoire with song count
    var currentRepertoireDisplayName: String {
        if selectedRepertoire.id == "all" {
            return "All Songs"
        } else {
            let count = selectedRepertoire.songCount
            return "\(selectedRepertoire.name) (\(count))"
        }
    }
    
    /// Whether we're currently showing all songs
    var isShowingAllSongs: Bool {
        selectedRepertoire.id == "all"
    }
    
    /// Get repertoires available for adding songs (excludes "All Songs")
    var addableRepertoires: [Repertoire] {
        repertoires.filter { $0.id != "all" }
    }
}
