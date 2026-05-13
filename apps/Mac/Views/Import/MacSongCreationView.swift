//
//  MacSongCreationView.swift
//  Approach Note
//
//  Song creation view for importing from the Share Extension
//

import SwiftUI
import os

struct MacSongCreationView: View {
    @Environment(\.dismiss) var dismiss
    @EnvironmentObject var authManager: AuthenticationManager
    @EnvironmentObject var repertoireManager: RepertoireManager

    // Form fields - pre-populated with imported data
    @State private var title: String
    @State private var composer: String
    @State private var musicbrainzId: String
    @State private var workType: String
    @State private var key: String

    @State private var isSaving = false
    @State private var showingError = false
    @State private var errorMessage = ""

    @State private var createdSongId: String?
    @State private var showAddToRepertoirePrompt = false

    // Initialize with imported data
    init(importedData: ImportedSongData? = nil) {
        _title = State(initialValue: importedData?.title ?? "")
        _composer = State(initialValue: importedData?.composerString ?? "")
        _musicbrainzId = State(initialValue: importedData?.musicbrainzId ?? "")
        _workType = State(initialValue: importedData?.workType ?? "")
        _key = State(initialValue: importedData?.key ?? "")
    }

    var body: some View {
        VStack(spacing: 0) {
            // Header
            HStack {
                Button("Cancel") {
                    dismiss()
                }
                .keyboardShortcut(.cancelAction)

                Spacer()

                Text("Create Song")
                    .font(ApproachNoteTheme.headline())

                Spacer()

                Button("Save") {
                    saveSong()
                }
                .keyboardShortcut(.defaultAction)
                .disabled(title.isEmpty || isSaving)
            }
            .padding()

            Divider()

            // Form content
            Form {
                Section {
                    TextField("Song Title", text: $title)
                        .textFieldStyle(.roundedBorder)

                    TextField("Composer", text: $composer)
                        .textFieldStyle(.roundedBorder)

                    TextField("MusicBrainz ID", text: $musicbrainzId)
                        .textFieldStyle(.roundedBorder)
                        .font(.system(.body, design: .monospaced))
                } header: {
                    Text("Basic Information")
                }

                Section {
                    TextField("Work Type (e.g., Song, Instrumental)", text: $workType)
                        .textFieldStyle(.roundedBorder)

                    TextField("Key (e.g., Eb, F minor)", text: $key)
                        .textFieldStyle(.roundedBorder)
                } header: {
                    Text("Additional Details")
                }

                Section {
                    Text("Additional details (structure, recordings) can be added later through the app.")
                        .font(ApproachNoteTheme.caption())
                        .foregroundColor(.secondary)
                }
            }
            .formStyle(.grouped)
            .padding()

            if isSaving {
                ProgressView("Saving...")
                    .padding()
            }
        }
        .frame(minWidth: 400, minHeight: 400)
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

    private func saveSong() {
        guard !title.isEmpty else {
            errorMessage = "Song title is required"
            showingError = true
            return
        }

        isSaving = true

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

        Log.ui.debug("Sending song creation request: url=\(url, privacy: .private), title=\(title, privacy: .public)")

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

extension Notification.Name {
    static let songCreated = Notification.Name("songCreated")
}

#Preview {
    MacSongCreationView(importedData: ImportedSongData(
        title: "All the Things You Are",
        musicbrainzId: "bc8bca8d-967d-305d-9291-0b73cdd6f930",
        composers: ["Jerome Kern", "Oscar Hammerstein II"],
        workType: "Song",
        key: "A flat major",
        annotation: nil,
        wikipediaUrl: nil,
        sourceUrl: nil,
        importedAt: Date()
    ))
}
