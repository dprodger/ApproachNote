// SettingsView.swift
// User settings and profile view

import SwiftUI

struct SettingsView: View {
    @EnvironmentObject var authManager: AuthenticationManager
    @EnvironmentObject var favoritesManager: FavoritesManager
    @AppStorage("preferredStreamingService") private var preferredStreamingService: String = StreamingService.spotify.rawValue

    @State private var contributionStats: UserContributionStats?
    @State private var isLoadingContributions = false
    @State private var contributionsError: String?
    @State private var isShowingLogin = false

    private let contributionService = ContributionService()

    var body: some View {
        NavigationStack {
            ScrollView {
                VStack(spacing: ApproachNoteTheme.spacingXL) {
                    if authManager.isAuthenticated {
                        // User Info Section
                        VStack(spacing: ApproachNoteTheme.spacingMD) {
                            // Profile Icon
                            Circle()
                                .fill(ApproachNoteTheme.brand.gradient)
                                .frame(width: 80, height: 80)
                                .overlay {
                                    Image(systemName: "person.fill")
                                        .font(.system(size: 40))
                                        .foregroundColor(.white)
                                }

                            // Name
                            if let displayName = authManager.currentUser?.displayName {
                                Text(displayName)
                                    .font(ApproachNoteTheme.title2())
                                    .fontWeight(.semibold)
                                    .foregroundColor(ApproachNoteTheme.textPrimary)
                            }

                            // Email
                            if let email = authManager.currentUser?.email {
                                Text(email)
                                    .font(ApproachNoteTheme.body())
                                    .bodyLineSpacing()
                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                            }
                        }
                        .padding(.top, 32)

                        Divider()
                            .padding(.horizontal)
                    }

                    // Playback Settings Section
                    VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingSM) {
                        Text("Playback".uppercased())
                            .font(ApproachNoteTheme.title3())
                            .foregroundColor(ApproachNoteTheme.textPrimary)
                            .padding(.horizontal)

                        HStack {
                            Text("Preferred Service")
                                .font(ApproachNoteTheme.body())
                                .bodyLineSpacing()
                                .foregroundColor(ApproachNoteTheme.textPrimary)
                            Spacer()
                            Picker("", selection: $preferredStreamingService) {
                                ForEach(StreamingService.allCases) { service in
                                    Text(service.displayName).tag(service.rawValue)
                                }
                            }
                            .pickerStyle(.menu)
                            .tint(ApproachNoteTheme.brand)
                        }
                        .padding(.horizontal)

                        Text("Default to playing on the selected service when there are multiple services available.")
                            .font(ApproachNoteTheme.caption())
                            .foregroundColor(ApproachNoteTheme.textSecondary)
                            .padding(.horizontal)
                    }
                    .padding(.top, authManager.isAuthenticated ? 0 : ApproachNoteTheme.spacingXL)

                    if authManager.isAuthenticated {
                        Divider()
                            .padding(.horizontal)

                        // Favorites Section
                        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingSM) {
                            HStack(spacing: ApproachNoteTheme.spacingXS) {
                                Text("Favorites".uppercased())
                                    .font(ApproachNoteTheme.title3())
                                    .foregroundColor(ApproachNoteTheme.textPrimary)
                                if favoritesManager.favoriteCount > 0 {
                                    Text("\(favoritesManager.favoriteCount)")
                                        .font(ApproachNoteTheme.title3(weight: .regular))
                                        .foregroundColor(ApproachNoteTheme.textSecondary)
                                }
                            }
                            .padding(.horizontal)

                            if favoritesManager.isLoading {
                                HStack {
                                    Spacer()
                                    ProgressView()
                                        .tint(ApproachNoteTheme.textSecondary)
                                    Spacer()
                                }
                                .padding()
                            } else if favoritesManager.favoriteRecordings.isEmpty {
                                Text("No favorite recordings yet")
                                    .font(ApproachNoteTheme.body())
                                    .bodyLineSpacing()
                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                                    .padding(.horizontal)
                            } else {
                                ScrollView(.horizontal, showsIndicators: false) {
                                    HStack(alignment: .top, spacing: ApproachNoteTheme.spacingMD) {
                                        ForEach(favoritesManager.favoriteRecordings, id: \.id) { recording in
                                            NavigationLink(destination: RecordingDetailView(recordingId: recording.id)) {
                                                VStack(spacing: ApproachNoteTheme.spacingXS) {
                                                    // Album art
                                                    if let artUrl = recording.bestAlbumArtSmall,
                                                       let url = URL(string: artUrl) {
                                                        CachedAsyncImage(
                                                            url: url,
                                                            content: { image in
                                                                image
                                                                    .resizable()
                                                                    .aspectRatio(contentMode: .fill)
                                                                    .frame(width: 80, height: 80)
                                                                    .cornerRadius(8)
                                                            },
                                                            placeholder: {
                                                                Rectangle()
                                                                    .fill(ApproachNoteTheme.surface)
                                                                    .frame(width: 80, height: 80)
                                                                    .cornerRadius(8)
                                                            }
                                                        )
                                                    } else {
                                                        Rectangle()
                                                            .fill(ApproachNoteTheme.surface)
                                                            .frame(width: 80, height: 80)
                                                            .cornerRadius(8)
                                                            .overlay(
                                                                Image(systemName: "opticaldisc")
                                                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                                                            )
                                                    }

                                                    // Song title
                                                    Text(recording.songTitle ?? "Unknown")
                                                        .font(ApproachNoteTheme.caption())
                                                        .fontWeight(.medium)
                                                        .foregroundColor(ApproachNoteTheme.textPrimary)
                                                        .lineLimit(2)
                                                        .multilineTextAlignment(.center)
                                                }
                                                .frame(width: 80)
                                            }
                                            .buttonStyle(.plain)
                                        }
                                    }
                                    .padding(.horizontal)
                                }
                            }
                        }

                        Divider()
                            .padding(.horizontal)

                        // Contributions Section
                        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingSM) {
                            Text("Your Contributions".uppercased())
                                .font(ApproachNoteTheme.title3())
                                .foregroundColor(ApproachNoteTheme.textPrimary)
                                .padding(.horizontal)

                            if isLoadingContributions {
                                HStack {
                                    Spacer()
                                    ProgressView()
                                        .tint(ApproachNoteTheme.textSecondary)
                                    Spacer()
                                }
                                .padding()
                            } else if let stats = contributionStats {
                                VStack(spacing: 0) {
                                    ContributionStatRow(
                                        label: "Transcriptions",
                                        count: stats.transcriptions
                                    )

                                    Divider()

                                    ContributionStatRow(
                                        label: "Backing Tracks",
                                        count: stats.backingTracks
                                    )

                                    Divider()

                                    ContributionStatRow(
                                        label: "Tempo Markings",
                                        count: stats.tempoMarkings
                                    )

                                    Divider()

                                    ContributionStatRow(
                                        label: "Vocal/Instrumental",
                                        count: stats.instrumentalVocal
                                    )

                                    Divider()

                                    ContributionStatRow(
                                        label: "Performance Keys",
                                        count: stats.keys
                                    )
                                }
                                .padding(.horizontal)
                                .background(ApproachNoteTheme.surface)
                                .cornerRadius(8)
                                .padding(.horizontal)

                                if stats.totalContributions > 0 {
                                    Text("Total: \(stats.totalContributions) contribution\(stats.totalContributions == 1 ? "" : "s")")
                                        .font(ApproachNoteTheme.caption())
                                        .foregroundColor(ApproachNoteTheme.textSecondary)
                                        .fontWeight(.medium)
                                        .padding(.horizontal)
                                }
                            } else if let error = contributionsError {
                                HStack {
                                    Image(systemName: "exclamationmark.triangle")
                                        .foregroundColor(.orange)
                                    Text(error)
                                        .font(ApproachNoteTheme.body())
                                        .bodyLineSpacing()
                                        .foregroundColor(ApproachNoteTheme.textSecondary)
                                }
                                .padding(.horizontal)
                            }

                            Text("Thank you for helping improve the community!")
                                .font(ApproachNoteTheme.caption())
                                .foregroundColor(ApproachNoteTheme.textSecondary)
                                .padding(.horizontal)
                        }
                    }

                    Divider()
                        .padding(.horizontal)

                    // Account Actions
                    VStack(spacing: 0) {
                        if authManager.isAuthenticated {
                            ApproachNoteButton(
                                "Log Out",
                                leadingSystemImage: "rectangle.portrait.and.arrow.right"
                            ) {
                                authManager.logout()
                            }
                            .padding(.horizontal)
                        } else {
                            ApproachNoteButton("Sign In / Sign Up") {
                                isShowingLogin = true
                            }
                            .padding(.horizontal)
                        }
                    }

                    Spacer()
                }
            }
            .background(ApproachNoteTheme.background)
            .jazzNavigationBar(title: "Settings")
            .task(id: authManager.isAuthenticated) {
                await loadContributionStats()
            }
            .sheet(isPresented: $isShowingLogin) {
                LoginView()
                    .environmentObject(authManager)
            }
        }
    }

    // MARK: - Data Loading

    private func loadContributionStats() async {
        guard let token = await authManager.validAccessToken() else {
            return
        }

        isLoadingContributions = true
        contributionsError = nil

        if let stats = await contributionService.fetchUserContributionStats(authToken: token) {
            contributionStats = stats
        } else {
            contributionsError = "Could not load contributions"
        }

        isLoadingContributions = false
    }
}

// MARK: - Contribution Stat Row

private struct ContributionStatRow: View {
    let label: String
    let count: Int

    var body: some View {
        HStack(spacing: ApproachNoteTheme.spacingSM) {
            Text(label)
                .font(ApproachNoteTheme.body(weight: .bold))
                .bodyLineSpacing()
                .foregroundColor(ApproachNoteTheme.textPrimary)

            Spacer()

            Text("\(count)")
                .font(ApproachNoteTheme.title3())
                .fontWeight(.semibold)
                .foregroundColor(count > 0 ? ApproachNoteTheme.textPrimary : ApproachNoteTheme.textSecondary.opacity(0.5))
        }
        .padding(.vertical, ApproachNoteTheme.spacingSM)
        .padding(.horizontal, ApproachNoteTheme.spacingSM)
    }
}

#Preview {
    SettingsView()
        .environmentObject(AuthenticationManager())
        .environmentObject(FavoritesManager())
}
