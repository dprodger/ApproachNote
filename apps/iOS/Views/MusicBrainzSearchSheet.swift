//
//  MusicBrainzSearchSheet.swift
//  Approach Note
//
//  Search MusicBrainz for songs and import them into the database
//

import SwiftUI

struct MusicBrainzSearchSheet: View {
    let searchQuery: String
    let onSongImported: () -> Void

    @Environment(\.dismiss) private var dismiss
    @Environment(\.openURL) private var openURL
    @EnvironmentObject var authManager: AuthenticationManager
    @StateObject private var musicBrainzService = MusicBrainzService()

    @State private var searchResults: [MusicBrainzWork] = []
    @State private var isSearching = false
    /// The search itself failed, as opposed to returning no matches.
    @State private var searchFailed = false
    @State private var selectedWork: MusicBrainzWork?
    @State private var isSubmitting = false
    @State private var resultTitle: String?
    @State private var resultMessage = ""
    @State private var dismissOnAcknowledge = false

    var body: some View {
        NavigationStack {
            VStack(spacing: 0) {
                if isSearching {
                    loadingView
                } else if searchFailed {
                    unavailableView
                } else if searchResults.isEmpty {
                    emptyView
                } else {
                    resultsList
                }
            }
            .background(ApproachNoteTheme.background)
            .navigationTitle("MusicBrainz Search")
            .navigationBarTitleDisplayMode(.inline)
            // Style the nav bar from the live palette (the global
            // UINavigationBar appearance is set once at launch and goes stale
            // when the palette changes), matching jazzNavigationBar.
            .toolbarBackground(ApproachNoteTheme.brand, for: .navigationBar)
            .toolbarBackground(.visible, for: .navigationBar)
            .toolbarColorScheme(.dark, for: .navigationBar)
            .toolbar {
                ToolbarItem(placement: .navigationBarLeading) {
                    Button("Cancel") {
                        dismiss()
                    }
                    .foregroundColor(ApproachNoteTheme.textOnDark)
                }
            }
            .task {
                await performSearch()
            }
            .alert(resultTitle ?? "", isPresented: Binding(
                get: { resultTitle != nil },
                set: { if !$0 { resultTitle = nil } }
            )) {
                Button("OK") {
                    let shouldDismiss = dismissOnAcknowledge
                    resultTitle = nil
                    if shouldDismiss {
                        onSongImported()
                        dismiss()
                    }
                }
            } message: {
                Text(resultMessage)
            }
        }
        // An alert rather than a confirmationDialog: on iPad a dialog is presented
        // as a popover, and on iPadOS 26 (with the app's
        // UIDesignRequiresCompatibility opt-out) that popover renders with no
        // readable content. Alerts are unaffected, but SwiftUI only honours one
        // alert per view, so this one hangs off the NavigationStack while the
        // result alert above stays on its content. Alerts also only take buttons,
        // so the MusicBrainz link opens through the environment.
        .alert(
            "Request Song",
            isPresented: Binding(
                get: { selectedWork != nil && !isSubmitting },
                set: { if !$0 { selectedWork = nil } }
            ),
            presenting: selectedWork
        ) { work in
            Button("Request \"\(work.title)\"") {
                Task {
                    await requestSong(work)
                }
            }

            if let url = URL(string: work.musicbrainzUrl) {
                Button("View on MusicBrainz") {
                    openURL(url)
                }
            }

            Button("Cancel", role: .cancel) {
                selectedWork = nil
            }
        } message: { work in
            Text("Request that \"\(work.title)\" by \(work.composerDisplay) be added? We'll review it and add the song if it's a good fit.")
        }
        .presentationDetents([.medium, .large])
        .presentationDragIndicator(.visible)
    }

    // MARK: - Views

    private var loadingView: some View {
        VStack {
            Spacer()
            ThemedProgressView(message: "Searching MusicBrainz...", tintColor: ApproachNoteTheme.brand)
            Spacer()
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
    }

    private var unavailableView: some View {
        VStack(spacing: ApproachNoteTheme.spacingMD) {
            Image(systemName: "exclamationmark.triangle")
                .font(.system(size: 60))
                .foregroundColor(ApproachNoteTheme.textSecondary.opacity(0.5))

            Text("Search Unavailable")
                .font(ApproachNoteTheme.headline())
                .foregroundColor(ApproachNoteTheme.textPrimary)

            Text("We couldn't reach MusicBrainz just now. This is usually temporary.")
                .font(ApproachNoteTheme.subheadline())
                .foregroundColor(ApproachNoteTheme.textSecondary)
                .multilineTextAlignment(.center)
                .padding(.horizontal)

            Button {
                Task {
                    await performSearch()
                }
            } label: {
                HStack {
                    Image(systemName: "arrow.clockwise")
                    Text("Try Again")
                }
            }
            .buttonStyle(.bordered)
            .tint(ApproachNoteTheme.brand)
            .padding(.top, ApproachNoteTheme.spacingXS)
        }
        .padding()
        .frame(maxWidth: .infinity, maxHeight: .infinity)
    }

    private var emptyView: some View {
        VStack(spacing: ApproachNoteTheme.spacingMD) {
            Image(systemName: "magnifyingglass")
                .font(.system(size: 60))
                .foregroundColor(ApproachNoteTheme.textSecondary.opacity(0.5))

            Text("No Results Found")
                .font(ApproachNoteTheme.headline())
                .foregroundColor(ApproachNoteTheme.textPrimary)

            Text("No works matching \"\(searchQuery)\" were found on MusicBrainz.")
                .font(ApproachNoteTheme.subheadline())
                .foregroundColor(ApproachNoteTheme.textSecondary)
                .multilineTextAlignment(.center)
                .padding(.horizontal)

            Link(destination: URL(string: "https://musicbrainz.org/search?query=\(searchQuery.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed) ?? searchQuery)&type=work")!) {
                HStack {
                    Image(systemName: "safari")
                    Text("Search on MusicBrainz.org")
                }
            }
            .buttonStyle(.bordered)
            .tint(ApproachNoteTheme.brand)
            .padding(.top, ApproachNoteTheme.spacingXS)
        }
        .padding()
        .frame(maxWidth: .infinity, maxHeight: .infinity)
    }

    private var resultsList: some View {
        List {
            Section {
                ForEach(searchResults) { work in
                    Button(action: {
                        selectedWork = work
                    }) {
                        workRowView(work: work)
                    }
                    .buttonStyle(.plain)
                    .listRowBackground(ApproachNoteTheme.surface)
                }
            } header: {
                Text("Results for \"\(searchQuery)\"")
                    .font(ApproachNoteTheme.caption())
                    .foregroundColor(ApproachNoteTheme.textSecondary)
            } footer: {
                Text("Tap a result to request it be added to the catalog.")
                    .font(ApproachNoteTheme.caption())
                    .foregroundColor(ApproachNoteTheme.textSecondary)
            }
        }
        .listStyle(.insetGrouped)
        .scrollContentBackground(.hidden)
    }

    private func workRowView(work: MusicBrainzWork) -> some View {
        HStack(alignment: .center, spacing: ApproachNoteTheme.spacingSM) {
            // Score indicator
            scoreIndicator(score: work.score)

            VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXXS) {
                Text(work.title)
                    .font(ApproachNoteTheme.headline())
                    .foregroundColor(ApproachNoteTheme.textPrimary)

                Text(work.composerDisplay)
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(ApproachNoteTheme.textSecondary)

                if let type = work.type {
                    Text(type)
                        .font(ApproachNoteTheme.caption())
                        .foregroundColor(ApproachNoteTheme.brand)
                }
            }

            Spacer()

            // Link to MusicBrainz
            Link(destination: URL(string: work.musicbrainzUrl)!) {
                Image(systemName: "arrow.up.right.square")
                    .foregroundColor(ApproachNoteTheme.brand)
            }
            .buttonStyle(.plain)
        }
        .padding(.vertical, ApproachNoteTheme.spacingXXS)
    }

    private func scoreIndicator(score: Int?) -> some View {
        let scoreValue = score ?? 0
        let color: Color = {
            if scoreValue >= 90 { return .green }
            if scoreValue >= 70 { return ApproachNoteTheme.accent }
            return ApproachNoteTheme.textSecondary
        }()

        return VStack(spacing: ApproachNoteTheme.spacingXXS) {
            Circle()
                .fill(color)
                .frame(width: 10, height: 10)
            Text("\(scoreValue)")
                .font(.system(size: 13, weight: .semibold))
                .foregroundColor(ApproachNoteTheme.textPrimary)
        }
        .frame(width: 32)
    }

    // MARK: - Actions

    private func performSearch() async {
        isSearching = true
        switch await musicBrainzService.searchMusicBrainzWorks(query: searchQuery) {
        case .results(let works):
            searchResults = works
            searchFailed = false
        case .unavailable:
            searchResults = []
            searchFailed = true
        }
        isSearching = false
    }

    private func requestSong(_ work: MusicBrainzWork) async {
        isSubmitting = true
        selectedWork = nil

        guard let token = await authManager.validAccessToken() else {
            showResult(title: "Sign In Required",
                       message: "You must be logged in to request a song.",
                       dismiss: false)
            isSubmitting = false
            return
        }

        let result = await musicBrainzService.submitSongRequest(work: work, authToken: token)
        switch result {
        case .submitted(let message):
            showResult(title: "Request Submitted", message: message, dismiss: true)
        case .alreadyKnown(let message):
            showResult(title: "Already Requested", message: message, dismiss: false)
        case .failed(let message):
            showResult(title: "Couldn't Submit Request", message: message, dismiss: false)
        }

        isSubmitting = false
    }

    private func showResult(title: String, message: String, dismiss: Bool) {
        resultMessage = message
        dismissOnAcknowledge = dismiss
        resultTitle = title
    }
}

#Preview {
    MusicBrainzSearchSheet(
        searchQuery: "Autumn Leaves",
        onSongImported: {}
    )
    .environmentObject(AuthenticationManager())
}
