//
//  MacAddToRepertoireSheet.swift
//  Approach Note
//
//  Sheet for adding songs to repertoires on macOS
//

import SwiftUI

struct MacAddToRepertoireSheet: View {
    let songId: String
    let songTitle: String
    @ObservedObject var repertoireManager: RepertoireManager
    @Environment(\.dismiss) var dismiss

    var onSuccess: ((String) -> Void)?
    var onError: ((String) -> Void)?

    @State private var isAdding = false
    @State private var isLoadingRepertoires = false
    @State private var showCreateRepertoire = false

    var body: some View {
        VStack(spacing: 0) {
            // Header
            headerView

            Divider()

            // Content
            Group {
                if !repertoireManager.isAuthenticated {
                    authRequiredView
                } else if isLoadingRepertoires {
                    loadingView
                } else if repertoireManager.addableRepertoires.isEmpty {
                    emptyRepertoiresView
                } else {
                    repertoireList
                }
            }
        }
        .frame(width: 400, height: 450)
        .overlay {
            if isAdding {
                addingOverlay
            }
        }
        .task {
            if repertoireManager.isAuthenticated &&
               (repertoireManager.repertoires.isEmpty ||
                repertoireManager.addableRepertoires.isEmpty) {
                isLoadingRepertoires = true
                await repertoireManager.loadRepertoires()
                isLoadingRepertoires = false
            }
        }
        .sheet(isPresented: $showCreateRepertoire) {
            MacCreateRepertoireView(repertoireManager: repertoireManager)
        }
    }

    // MARK: - Header

    private var headerView: some View {
        HStack {
            VStack(alignment: .leading, spacing: 4) {
                Text("Add to Repertoire")
                    .font(ApproachNoteTheme.headline())
                    .foregroundColor(ApproachNoteTheme.textPrimary)
                Text(songTitle)
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(ApproachNoteTheme.textPrimary.opacity(0.7))
                    .lineLimit(1)
            }

            Spacer()

            Button(action: { dismiss() }) {
                Text("Cancel")
                    .foregroundColor(ApproachNoteTheme.brand)
            }
            .buttonStyle(.plain)
            .disabled(isAdding)
        }
        .padding()
        .background(ApproachNoteTheme.surface)
    }

    // MARK: - Auth Required View

    private var authRequiredView: some View {
        VStack(spacing: 20) {
            Image(systemName: "lock.fill")
                .font(.system(size: 48))
                .foregroundColor(ApproachNoteTheme.brand.opacity(0.6))

            Text("Sign In Required")
                .font(ApproachNoteTheme.title2())
                .foregroundColor(ApproachNoteTheme.textPrimary)

            Text("You need to be signed in to add songs to repertoires")
                .font(ApproachNoteTheme.body())
                .foregroundColor(ApproachNoteTheme.textPrimary.opacity(0.7))
                .multilineTextAlignment(.center)
                .padding(.horizontal)

            Button("Close") {
                dismiss()
            }
            .buttonStyle(.borderedProminent)
            .tint(ApproachNoteTheme.brand)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(ApproachNoteTheme.background)
    }

    // MARK: - Loading View

    private var loadingView: some View {
        VStack(spacing: 16) {
            ProgressView()
                .controlSize(.large)
            Text("Loading repertoires...")
                .foregroundColor(ApproachNoteTheme.textPrimary.opacity(0.7))
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(ApproachNoteTheme.background)
    }

    // MARK: - Empty Repertoires View

    private var emptyRepertoiresView: some View {
        VStack(spacing: 20) {
            Image(systemName: "music.note.list")
                .font(.system(size: 48))
                .foregroundColor(ApproachNoteTheme.textSecondary.opacity(0.5))

            Text("No Repertoires Yet")
                .font(ApproachNoteTheme.title2())
                .foregroundColor(ApproachNoteTheme.textPrimary)

            Text("Create a repertoire first to start organizing your songs.")
                .font(ApproachNoteTheme.subheadline())
                .foregroundColor(ApproachNoteTheme.textPrimary.opacity(0.7))
                .multilineTextAlignment(.center)
                .padding(.horizontal)

            Button("Create Repertoire") {
                showCreateRepertoire = true
            }
            .buttonStyle(.borderedProminent)
            .tint(ApproachNoteTheme.brand)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(ApproachNoteTheme.background)
    }

    // MARK: - Repertoire List

    private var repertoireList: some View {
        ScrollView {
            VStack(spacing: 0) {
                // Quick add section
                if let lastUsed = repertoireManager.lastUsedRepertoire {
                    VStack(alignment: .leading, spacing: 8) {
                        Text("Quick Add")
                            .font(ApproachNoteTheme.caption())
                            .foregroundColor(ApproachNoteTheme.textPrimary.opacity(0.6))
                            .padding(.horizontal)
                            .padding(.top, 12)

                        Button(action: { addToRepertoire(lastUsed) }) {
                            HStack {
                                VStack(alignment: .leading, spacing: 4) {
                                    Text("Add to \(lastUsed.name)")
                                        .font(ApproachNoteTheme.headline())
                                        .foregroundColor(ApproachNoteTheme.textPrimary)
                                    Text("Last used")
                                        .font(ApproachNoteTheme.caption())
                                        .foregroundColor(ApproachNoteTheme.textPrimary.opacity(0.6))
                                }
                                Spacer()
                                Image(systemName: "arrow.right.circle.fill")
                                    .font(.title2)
                                    .foregroundColor(ApproachNoteTheme.accent)
                            }
                            .padding()
                            .background(ApproachNoteTheme.accent.opacity(0.1))
                            .cornerRadius(8)
                        }
                        .buttonStyle(.plain)
                        .disabled(isAdding)
                        .padding(.horizontal)
                    }
                }

                // All repertoires section
                VStack(alignment: .leading, spacing: 8) {
                    Text("All Repertoires")
                        .font(ApproachNoteTheme.caption())
                        .foregroundColor(ApproachNoteTheme.textPrimary.opacity(0.6))
                        .padding(.horizontal)
                        .padding(.top, 16)

                    ForEach(repertoireManager.addableRepertoires) { repertoire in
                        Button(action: { addToRepertoire(repertoire) }) {
                            HStack {
                                VStack(alignment: .leading, spacing: 4) {
                                    Text(repertoire.name)
                                        .font(ApproachNoteTheme.headline())
                                        .foregroundColor(ApproachNoteTheme.textPrimary)

                                    if let description = repertoire.description, !description.isEmpty {
                                        Text(description)
                                            .font(ApproachNoteTheme.subheadline())
                                            .foregroundColor(ApproachNoteTheme.textPrimary.opacity(0.7))
                                            .lineLimit(2)
                                    }

                                    Text("\(repertoire.songCount) songs")
                                        .font(ApproachNoteTheme.caption())
                                        .foregroundColor(ApproachNoteTheme.brand)
                                }
                                Spacer()
                            }
                            .padding()
                            .background(ApproachNoteTheme.surface)
                            .cornerRadius(8)
                        }
                        .buttonStyle(.plain)
                        .disabled(isAdding)
                        .padding(.horizontal)
                    }
                }

                // Create new repertoire button
                Button(action: { showCreateRepertoire = true }) {
                    HStack {
                        Image(systemName: "plus.circle.fill")
                            .foregroundColor(ApproachNoteTheme.brand)
                        Text("Create New Repertoire")
                            .foregroundColor(ApproachNoteTheme.brand)
                        Spacer()
                    }
                    .padding()
                }
                .buttonStyle(.plain)
                .padding(.horizontal)
                .padding(.vertical, 8)
            }
            .padding(.bottom)
        }
        .background(ApproachNoteTheme.background)
    }

    // MARK: - Adding Overlay

    private var addingOverlay: some View {
        ZStack {
            Color.black.opacity(0.3)

            VStack(spacing: 16) {
                ProgressView()
                    .controlSize(.large)
                Text("Adding to repertoire...")
                    .font(ApproachNoteTheme.headline())
            }
            .padding(30)
            .background(ApproachNoteTheme.surface)
            .cornerRadius(12)
        }
    }

    // MARK: - Add to Repertoire Action

    private func addToRepertoire(_ repertoire: Repertoire) {
        isAdding = true

        Task {
            let success = await repertoireManager.addSongToRepertoire(
                songId: songId,
                repertoireId: repertoire.id
            )

            await MainActor.run {
                isAdding = false

                if success {
                    repertoireManager.setLastUsedRepertoire(repertoire)
                    dismiss()
                    onSuccess?("Added \"\(songTitle)\" to \(repertoire.name)")
                } else {
                    dismiss()
                    let errorMessage = repertoireManager.errorMessage ?? "Failed to add song"
                    onError?(errorMessage)
                }
            }
        }
    }
}

#Preview {
    MacAddToRepertoireSheet(
        songId: "test-id",
        songTitle: "Test Song",
        repertoireManager: RepertoireManager()
    )
}
