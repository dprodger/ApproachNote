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

    private let contributionService = ContributionService()

    var body: some View {
        NavigationStack {
            ScrollView {
                VStack(spacing: 24) {
                    // User Info Section
                    VStack(spacing: 16) {
                        // Profile Icon
                        Circle()
                            .fill(ApproachNoteTheme.burgundy.gradient)
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
                                .foregroundColor(ApproachNoteTheme.charcoal)
                        }

                        // Email
                        if let email = authManager.currentUser?.email {
                            Text(email)
                                .font(ApproachNoteTheme.body())
                                .foregroundColor(ApproachNoteTheme.smokeGray)
                        }
                    }
                    .padding(.top, 32)

                    Divider()
                        .padding(.horizontal)

                    // Playback Settings Section
                    VStack(alignment: .leading, spacing: 12) {
                        Text("Playback")
                            .font(ApproachNoteTheme.headline())
                            .foregroundColor(ApproachNoteTheme.charcoal)
                            .padding(.horizontal)

                        VStack(spacing: 0) {
                            HStack {
                                Image(systemName: "play.circle.fill")
                                    .foregroundColor(ApproachNoteTheme.burgundy)
                                Text("Preferred Service")
                                    .font(ApproachNoteTheme.body())
                                    .foregroundColor(ApproachNoteTheme.charcoal)
                                Spacer()
                                Picker("", selection: $preferredStreamingService) {
                                    ForEach(StreamingService.allCases) { service in
                                        Text(service.displayName).tag(service.rawValue)
                                    }
                                }
                                .pickerStyle(.menu)
                                .tint(ApproachNoteTheme.burgundy)
                            }
                            .padding()
                            .background(ApproachNoteTheme.cardBackground)
                            .cornerRadius(8)
                        }
                        .padding(.horizontal)

                        Text("Play buttons will open this service when available")
                            .font(ApproachNoteTheme.caption())
                            .foregroundColor(ApproachNoteTheme.smokeGray)
                            .padding(.horizontal)
                    }

                    Divider()
                        .padding(.horizontal)

                    // Favorites Section
                    VStack(alignment: .leading, spacing: 12) {
                        HStack {
                            Image(systemName: "heart.fill")
                                .foregroundColor(.red)
                            Text("Favorites")
                                .font(ApproachNoteTheme.headline())
                                .foregroundColor(ApproachNoteTheme.charcoal)
                        }
                        .padding(.horizontal)

                        if favoritesManager.isLoading {
                            HStack {
                                Spacer()
                                ProgressView()
                                    .tint(ApproachNoteTheme.brass)
                                Spacer()
                            }
                            .padding()
                        } else if favoritesManager.favoriteRecordings.isEmpty {
                            Text("No favorite recordings yet")
                                .font(ApproachNoteTheme.body())
                                .foregroundColor(ApproachNoteTheme.smokeGray)
                                .padding(.horizontal)
                        } else {
                            ScrollView(.horizontal, showsIndicators: false) {
                                HStack(spacing: 16) {
                                    ForEach(favoritesManager.favoriteRecordings, id: \.id) { recording in
                                        NavigationLink(destination: RecordingDetailView(recordingId: recording.id)) {
                                            VStack(spacing: 8) {
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
                                                                .fill(ApproachNoteTheme.cardBackground)
                                                                .frame(width: 80, height: 80)
                                                                .cornerRadius(8)
                                                        }
                                                    )
                                                } else {
                                                    Rectangle()
                                                        .fill(ApproachNoteTheme.cardBackground)
                                                        .frame(width: 80, height: 80)
                                                        .cornerRadius(8)
                                                        .overlay(
                                                            Image(systemName: "opticaldisc")
                                                                .foregroundColor(ApproachNoteTheme.smokeGray)
                                                        )
                                                }

                                                // Song title
                                                Text(recording.songTitle ?? "Unknown")
                                                    .font(ApproachNoteTheme.caption())
                                                    .fontWeight(.medium)
                                                    .foregroundColor(ApproachNoteTheme.charcoal)
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

                        if favoritesManager.favoriteCount > 0 {
                            Text("\(favoritesManager.favoriteCount) \(favoritesManager.favoriteCount == 1 ? "recording" : "recordings")")
                                .font(ApproachNoteTheme.caption())
                                .foregroundColor(ApproachNoteTheme.smokeGray)
                                .padding(.horizontal)
                        }
                    }

                    Divider()
                        .padding(.horizontal)

                    // Contributions Section
                    VStack(alignment: .leading, spacing: 12) {
                        HStack {
                            Image(systemName: "person.3.fill")
                                .foregroundColor(ApproachNoteTheme.brass)
                            Text("Your Contributions")
                                .font(ApproachNoteTheme.headline())
                                .foregroundColor(ApproachNoteTheme.charcoal)
                        }
                        .padding(.horizontal)

                        if isLoadingContributions {
                            HStack {
                                Spacer()
                                ProgressView()
                                    .tint(ApproachNoteTheme.brass)
                                Spacer()
                            }
                            .padding()
                        } else if let stats = contributionStats {
                            VStack(spacing: 0) {
                                ContributionStatRow(
                                    icon: "music.note.list",
                                    iconColor: ApproachNoteTheme.burgundy,
                                    label: "Transcriptions",
                                    count: stats.transcriptions
                                )

                                Divider()
                                    .padding(.leading, 48)

                                ContributionStatRow(
                                    icon: "play.rectangle.fill",
                                    iconColor: .green,
                                    label: "Backing Tracks",
                                    count: stats.backingTracks
                                )

                                Divider()
                                    .padding(.leading, 48)

                                ContributionStatRow(
                                    icon: "metronome",
                                    iconColor: ApproachNoteTheme.brass,
                                    label: "Tempo Markings",
                                    count: stats.tempoMarkings
                                )

                                Divider()
                                    .padding(.leading, 48)

                                ContributionStatRow(
                                    icon: "mic.fill",
                                    iconColor: .purple,
                                    label: "Vocal/Instrumental",
                                    count: stats.instrumentalVocal
                                )

                                Divider()
                                    .padding(.leading, 48)

                                ContributionStatRow(
                                    icon: "music.note",
                                    iconColor: .blue,
                                    label: "Performance Keys",
                                    count: stats.keys
                                )
                            }
                            .padding(.horizontal)
                            .background(ApproachNoteTheme.cardBackground)
                            .cornerRadius(8)
                            .padding(.horizontal)

                            if stats.totalContributions > 0 {
                                Text("Total: \(stats.totalContributions) contribution\(stats.totalContributions == 1 ? "" : "s")")
                                    .font(ApproachNoteTheme.caption())
                                    .foregroundColor(ApproachNoteTheme.brass)
                                    .fontWeight(.medium)
                                    .padding(.horizontal)
                            }
                        } else if let error = contributionsError {
                            HStack {
                                Image(systemName: "exclamationmark.triangle")
                                    .foregroundColor(.orange)
                                Text(error)
                                    .font(ApproachNoteTheme.body())
                                    .foregroundColor(ApproachNoteTheme.smokeGray)
                            }
                            .padding(.horizontal)
                        }

                        Text("Thank you for helping improve the community!")
                            .font(ApproachNoteTheme.caption())
                            .foregroundColor(ApproachNoteTheme.smokeGray)
                            .padding(.horizontal)
                    }

                    Divider()
                        .padding(.horizontal)

                    // Account Actions
                    VStack(spacing: 0) {
                        // Log Out Button
                        Button(action: {
                            authManager.logout()
                        }) {
                            HStack {
                                Image(systemName: "rectangle.portrait.and.arrow.right")
                                    .foregroundColor(ApproachNoteTheme.burgundy)
                                Text("Log Out")
                                    .foregroundColor(ApproachNoteTheme.charcoal)
                                Spacer()
                            }
                            .padding()
                            .background(ApproachNoteTheme.cardBackground)
                            .cornerRadius(8)
                        }
                        .padding(.horizontal)
                    }

                    Spacer()
                }
            }
            .background(ApproachNoteTheme.backgroundLight)
            .jazzNavigationBar(title: "Settings")
            .task {
                await loadContributionStats()
            }
        }
    }

    // MARK: - Data Loading

    private func loadContributionStats() async {
        guard let token = authManager.getAccessToken() else {
            contributionsError = "Not authenticated"
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
    let icon: String
    let iconColor: Color
    let label: String
    let count: Int

    var body: some View {
        HStack(spacing: 12) {
            Image(systemName: icon)
                .font(.system(size: 16))
                .foregroundColor(iconColor)
                .frame(width: 24)

            Text(label)
                .font(ApproachNoteTheme.body())
                .foregroundColor(ApproachNoteTheme.charcoal)

            Spacer()

            Text("\(count)")
                .font(ApproachNoteTheme.title3())
                .fontWeight(.semibold)
                .foregroundColor(count > 0 ? ApproachNoteTheme.charcoal : ApproachNoteTheme.smokeGray.opacity(0.5))
        }
        .padding(.vertical, 12)
        .padding(.horizontal, 12)
    }
}

#Preview {
    SettingsView()
        .environmentObject(AuthenticationManager())
        .environmentObject(FavoritesManager())
}
