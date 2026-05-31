//
//  SongCreationView.swift
//  Approach Note
//
//  Created by Dave Rodger on 11/1/25.
//

import SwiftUI
import os

// Notification for when a song is created and lists need to refresh
extension Notification.Name {
    static let songCreated = Notification.Name("songCreated")
}

struct SongCreationView: View {
    @Environment(\.dismiss) var dismiss
    @EnvironmentObject var authManager: AuthenticationManager
    @EnvironmentObject var repertoireManager: RepertoireManager

    // Form fields - pre-populated with imported data
    @State private var title: String
    @State private var composer: String
    @State private var musicbrainzId: String
    @State private var workType: String

    @State private var isSaving = false
    @State private var showingError = false
    @State private var errorMessage = ""

    @State private var createdSongId: String?
    @State private var showAddToRepertoirePrompt = false
    
    // Initialize with imported data (or empty if creating manually)
    init(importedData: ImportedSongData? = nil) {
        NSLog("🎨 SongCreationView.init()")
        NSLog("   Received title: '%@'", importedData?.title ?? "NIL")
        
        _title = State(initialValue: importedData?.title ?? "")
        _composer = State(initialValue: importedData?.composerString ?? "")
        _musicbrainzId = State(initialValue: importedData?.musicbrainzId ?? "")
        _workType = State(initialValue: importedData?.workType ?? "")

        NSLog("✅ Init complete, title state: '%@'", importedData?.title ?? "EMPTY")
    }
    
    var body: some View {
        NavigationView {
            Form {
                Section(header: Text("Basic Information")) {
                    TextField("Song Title", text: $title)
                    TextField("Composer", text: $composer)
                }
            }
            .navigationTitle("Create Song")
            .navigationBarTitleDisplayMode(.inline)
            .safeAreaInset(edge: .bottom) {
                VStack(spacing: 12) {
                    ApproachNoteButton("Create Song", isLoading: isSaving, action: saveSong)
                        .disabled(title.isEmpty || isSaving)

                    ApproachNoteButton("Cancel", style: .secondary) {
                        dismiss()
                    }
                }
                .padding(.horizontal)
                .padding(.bottom, 8)
            }
            .onAppear {
                NSLog("📱 SongCreationView.onAppear()")
                NSLog("   title = '%@'", title)
                NSLog("   composer = '%@'", composer)
                NSLog("   musicbrainzId = '%@'", musicbrainzId)
            }
            .alert("Error", isPresented: $showingError) {
                Button("OK", role: .cancel) { }
            } message: {
                Text(errorMessage)
            }
            .alert(
                "Add to \"\(repertoireManager.selectedRepertoire.name)\"?",
                isPresented: $showAddToRepertoirePrompt
            ) {
                Button("Add") {
                    addNewSongToSelectedRepertoire()
                }
                Button("Just View It", role: .cancel) {
                    repertoireManager.selectRepertoire(.allSongs)
                    dismiss()
                }
            } message: {
                Text("You're viewing the \"\(repertoireManager.selectedRepertoire.name)\" repertoire. Add \"\(title)\" to it? Choosing \"Just View It\" switches to All Songs so you can find the new song.")
            }
        }
    }
    
    private func saveSong() {
        // Validate required fields
        guard !title.isEmpty else {
            errorMessage = "Song title is required"
            showingError = true
            return
        }

        isSaving = true

        // Save song to API
        Task {
            do {
                let newSongId = try await saveSongToAPI()

                // Fire-and-forget: queue the new song for backend research so it
                // starts enriching with MusicBrainz / Spotify / Apple Music data
                // while the user is still deciding where to put it.
                Task {
                    _ = await ResearchService().refreshSongData(songId: newSongId)
                }

                await MainActor.run {
                    isSaving = false
                    Log.ui.info("Song saved successfully")
                    NotificationCenter.default.post(name: .songCreated, object: nil)

                    // If the user is viewing a specific repertoire, prompt to add.
                    // Otherwise just dismiss — they'll see the new song in All Songs.
                    if repertoireManager.selectedRepertoire.id != "all" {
                        createdSongId = newSongId
                        showAddToRepertoirePrompt = true
                    } else {
                        dismiss()
                    }
                }
            } catch {
                await MainActor.run {
                    isSaving = false
                    errorMessage = "Failed to save song: \(error.localizedDescription)"
                    showingError = true
                }
            }
        }
    }

    private func addNewSongToSelectedRepertoire() {
        guard let songId = createdSongId else {
            dismiss()
            return
        }
        let repertoireId = repertoireManager.selectedRepertoire.id
        Task {
            _ = await repertoireManager.addSongToRepertoire(songId: songId, repertoireId: repertoireId)
            await MainActor.run { dismiss() }
        }
    }
    
    // MARK: - API Integration
    
    private func saveSongToAPI() async throws -> String {
        let url = URL.api(path: "/songs")

        var songData: [String: Any] = [
            "title": title,
        ]

        if !composer.isEmpty {
            songData["composer"] = composer
        }
        if !musicbrainzId.isEmpty {
            songData["musicbrainz_id"] = musicbrainzId
        }
        if !workType.isEmpty {
            songData["external_references"] = ["work_type": workType]
        }

        let body = try JSONSerialization.data(withJSONObject: songData)

        Log.ui.debug("Sending song creation request: url=\(url, privacy: .private), title=\(title, privacy: .public), composer=\(composer, privacy: .public), musicbrainzId=\(musicbrainzId, privacy: .private)")

        let data = try await authManager.makeAuthenticatedRequest(
            url: url,
            method: "POST",
            body: body
        )

        guard let json = try JSONSerialization.jsonObject(with: data) as? [String: Any],
              let song = json["song"] as? [String: Any],
              let songId = song["id"] as? String else {
            throw NSError(
                domain: "API",
                code: -1,
                userInfo: [NSLocalizedDescriptionKey: "Missing song ID in response"]
            )
        }

        Log.ui.info("Song created successfully (id: \(songId, privacy: .private))")
        return songId
    }
}

#Preview {
    SongCreationView(importedData: ImportedSongData(
        title: "All the Things You Are",
        musicbrainzId: "bc8bca8d-967d-305d-9291-0b73cdd6f930",
        composers: ["Jerome Kern", "Oscar Hammerstein II"],
        workType: "Song",
        key: "A♭ major",
        annotation: nil,
        wikipediaUrl: nil,
        sourceUrl: nil,
        importedAt: Date()
    ))
}
