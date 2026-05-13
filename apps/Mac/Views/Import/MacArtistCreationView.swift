//
//  MacArtistCreationView.swift
//  Approach Note
//
//  Artist creation view for importing from the Share Extension
//

import SwiftUI
import os

struct MacArtistCreationView: View {
    @Environment(\.dismiss) var dismiss
    @EnvironmentObject var authManager: AuthenticationManager

    // Form fields - pre-populated with imported data
    @State private var name: String
    @State private var musicbrainzId: String

    @State private var isSaving = false
    @State private var showingError = false
    @State private var errorMessage = ""

    // Initialize with imported data
    init(importedData: ImportedArtistData? = nil) {
        _name = State(initialValue: importedData?.name ?? "")
        _musicbrainzId = State(initialValue: importedData?.musicbrainzId ?? "")
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

                Text("Create Artist")
                    .font(ApproachNoteTheme.headline())

                Spacer()

                Button("Save") {
                    saveArtist()
                }
                .keyboardShortcut(.defaultAction)
                .disabled(name.isEmpty || isSaving)
            }
            .padding()

            Divider()

            // Form content
            Form {
                Section {
                    TextField("Artist Name", text: $name)
                        .textFieldStyle(.roundedBorder)

                    TextField("MusicBrainz ID", text: $musicbrainzId)
                        .textFieldStyle(.roundedBorder)
                        .font(.system(.body, design: .monospaced))
                } header: {
                    Text("Basic Information")
                }

                Section {
                    Text("Additional details (bio, dates, instruments) will be automatically fetched by the backend.")
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
        .frame(minWidth: 400, minHeight: 300)
        .alert("Error", isPresented: $showingError) {
            Button("OK", role: .cancel) { }
        } message: {
            Text(errorMessage)
        }
    }

    private func saveArtist() {
        guard !name.isEmpty else {
            errorMessage = "Artist name is required"
            showingError = true
            return
        }

        isSaving = true

        Task {
            do {
                try await saveArtistToAPI()

                await MainActor.run {
                    isSaving = false
                    Log.ui.info("Artist saved successfully")
                    NotificationCenter.default.post(name: .artistCreated, object: nil)
                    dismiss()
                }
            } catch {
                await MainActor.run {
                    isSaving = false
                    errorMessage = "Failed to save artist: \(error.localizedDescription)"
                    showingError = true
                }
            }
        }
    }

    private func saveArtistToAPI() async throws {
        let url = URL.api(path: "/performers")

        let artistData: [String: Any] = [
            "name": name,
            "musicbrainz_id": musicbrainzId.isEmpty ? NSNull() : musicbrainzId,
        ]

        let body = try JSONSerialization.data(withJSONObject: artistData)

        Log.ui.debug("Sending artist creation request: url=\(url, privacy: .private), name=\(name, privacy: .public)")

        _ = try await authManager.makeAuthenticatedRequest(
            url: url,
            method: "POST",
            body: body
        )

        Log.ui.info("Artist created successfully")
    }
}

extension Notification.Name {
    static let artistCreated = Notification.Name("artistCreated")
}

#Preview {
    MacArtistCreationView(importedData: ImportedArtistData(
        name: "Miles Davis",
        musicbrainzId: "561d854a-6a28-4aa7-8c99-323e6ce46c2a",
        sourceUrl: "https://musicbrainz.org/artist/561d854a-6a28-4aa7-8c99-323e6ce46c2a",
        importedAt: Date()
    ))
}
