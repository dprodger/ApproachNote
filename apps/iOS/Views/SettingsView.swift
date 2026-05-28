// SettingsView.swift
// User settings and profile view

import SwiftUI

struct SettingsView: View {
    @EnvironmentObject var authManager: AuthenticationManager
    @EnvironmentObject var favoritesManager: FavoritesManager
    @EnvironmentObject var themeManager: ThemeManager
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
                        Text("Playback")
                            .font(ApproachNoteTheme.headline())
                            .foregroundColor(ApproachNoteTheme.textPrimary)
                            .padding(.horizontal)

                        VStack(spacing: 0) {
                            HStack {
                                Image(systemName: "play.circle.fill")
                                    .foregroundColor(ApproachNoteTheme.brand)
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
                            .padding()
                            .background(ApproachNoteTheme.surface)
                            .cornerRadius(8)
                        }
                        .padding(.horizontal)

                        Text("Play buttons will open this service when available")
                            .font(ApproachNoteTheme.caption())
                            .foregroundColor(ApproachNoteTheme.textSecondary)
                            .padding(.horizontal)
                    }

                    if authManager.isAuthenticated {
                        Divider()
                            .padding(.horizontal)

                        // Favorites Section
                        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingSM) {
                            HStack {
                                Image(systemName: "heart.fill")
                                    .foregroundColor(.red)
                                Text("Favorites")
                                    .font(ApproachNoteTheme.headline())
                                    .foregroundColor(ApproachNoteTheme.textPrimary)
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
                                    HStack(spacing: ApproachNoteTheme.spacingMD) {
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

                            if favoritesManager.favoriteCount > 0 {
                                Text("\(favoritesManager.favoriteCount) \(favoritesManager.favoriteCount == 1 ? "recording" : "recordings")")
                                    .font(ApproachNoteTheme.caption())
                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                                    .padding(.horizontal)
                            }
                        }

                        Divider()
                            .padding(.horizontal)

                        // Contributions Section
                        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingSM) {
                            HStack {
                                Image(systemName: "person.3.fill")
                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                                Text("Your Contributions")
                                    .font(ApproachNoteTheme.headline())
                                    .foregroundColor(ApproachNoteTheme.textPrimary)
                            }
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
                                        icon: "music.note.list",
                                        iconColor: ApproachNoteTheme.brand,
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
                                        iconColor: ApproachNoteTheme.textSecondary,
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
                            Button(action: {
                                authManager.logout()
                            }) {
                                HStack {
                                    Image(systemName: "rectangle.portrait.and.arrow.right")
                                        .foregroundColor(ApproachNoteTheme.brand)
                                    Text("Log Out")
                                        .foregroundColor(ApproachNoteTheme.textPrimary)
                                    Spacer()
                                }
                                .padding()
                                .background(ApproachNoteTheme.surface)
                                .cornerRadius(8)
                            }
                            .padding(.horizontal)
                        } else {
                            Button(action: {
                                isShowingLogin = true
                            }) {
                                HStack {
                                    Image(systemName: "person.crop.circle.badge.plus")
                                        .foregroundColor(ApproachNoteTheme.brand)
                                    Text("Sign In or Create Account")
                                        .foregroundColor(ApproachNoteTheme.textPrimary)
                                    Spacer()
                                }
                                .padding()
                                .background(ApproachNoteTheme.surface)
                                .cornerRadius(8)
                            }
                            .padding(.horizontal)
                        }
                    }

                    // Theme Palette (temporary — runtime palette switcher)
                    Divider()
                        .padding(.horizontal)

                    VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingSM) {
                        Text("Appearance (Preview)")
                            .font(ApproachNoteTheme.headline())
                            .foregroundColor(ApproachNoteTheme.textPrimary)
                            .padding(.horizontal)

                        VStack(spacing: 0) {
                            HStack {
                                Image(systemName: "paintpalette.fill")
                                    .foregroundColor(ApproachNoteTheme.brand)
                                Text("Palette")
                                    .font(ApproachNoteTheme.body())
                                    .bodyLineSpacing()
                                    .foregroundColor(ApproachNoteTheme.textPrimary)
                                Spacer()
                                Picker("", selection: $themeManager.palette) {
                                    ForEach(PaletteChoice.allCases) { choice in
                                        Text(choice.displayName).tag(choice)
                                    }
                                }
                                .pickerStyle(.menu)
                                .tint(ApproachNoteTheme.brand)
                            }
                            .padding()
                            .background(ApproachNoteTheme.surface)
                            .cornerRadius(8)
                        }
                        .padding(.horizontal)

                        Text("Temporary picker for evaluating design palettes. Switching reloads the view tree.")
                            .font(ApproachNoteTheme.caption())
                            .foregroundColor(ApproachNoteTheme.textSecondary)
                            .padding(.horizontal)
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
        guard let token = authManager.getAccessToken() else {
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
        HStack(spacing: ApproachNoteTheme.spacingSM) {
            Image(systemName: icon)
                .font(.system(size: 16))
                .foregroundColor(iconColor)
                .frame(width: 24)

            Text(label)
                .font(ApproachNoteTheme.body())
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
        .environmentObject(ThemeManager.shared)
}
