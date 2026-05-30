//
//  CommunitySettingsView.swift
//  Approach Note
//
//  Displays the user's community contributions in Settings
//

import SwiftUI

struct CommunitySettingsView: View {
    @EnvironmentObject var authManager: AuthenticationManager

    @State private var contributionStats: UserContributionStats?
    @State private var isLoading = true
    @State private var errorMessage: String?

    private let contributionService = ContributionService()

    var body: some View {
        Group {
            if authManager.isAuthenticated {
                authenticatedView
            } else {
                notAuthenticatedView
            }
        }
    }

    // MARK: - Authenticated View

    @ViewBuilder
    private var authenticatedView: some View {
        VStack(spacing: 0) {
            Form {
                Section {
                    // Header with user info
                    HStack(spacing: ApproachNoteTheme.spacingSM) {
                        Image(systemName: "person.3.fill")
                            .font(.system(size: 32))
                            .foregroundColor(ApproachNoteTheme.textSecondary)

                        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXXS) {
                            Text("Your Contributions")
                                .font(ApproachNoteTheme.headline())
                                .foregroundColor(ApproachNoteTheme.textPrimary)
                            Text("Thank you for helping improve the community!")
                                .font(ApproachNoteTheme.subheadline())
                                .foregroundColor(.secondary)
                        }

                        Spacer()
                    }
                    .padding(.vertical, ApproachNoteTheme.spacingXS)
                }

                Section("Contribution Statistics") {
                    if isLoading {
                        HStack {
                            Spacer()
                            ProgressView()
                                .scaleEffect(0.8)
                            Text("Loading...")
                                .foregroundColor(ApproachNoteTheme.textSecondary)
                            Spacer()
                        }
                        .padding(.vertical, ApproachNoteTheme.spacingXS)
                    } else if let stats = contributionStats {
                        contributionStatsView(stats: stats)
                    } else if let error = errorMessage {
                        HStack {
                            Image(systemName: "exclamationmark.triangle")
                                .foregroundColor(.orange)
                            Text(error)
                                .foregroundColor(ApproachNoteTheme.textSecondary)
                        }
                        .padding(.vertical, ApproachNoteTheme.spacingXS)
                    }
                }
            }
            .formStyle(.grouped)
        }
        .task {
            await loadContributionStats()
        }
    }

    @ViewBuilder
    private func contributionStatsView(stats: UserContributionStats) -> some View {
        VStack(spacing: 0) {
            ContributionStatRow(
                icon: "music.note.list",
                iconColor: ApproachNoteTheme.brand,
                label: "Transcriptions",
                count: stats.transcriptions,
                description: "Solo transcription videos submitted"
            )

            Divider()
                .padding(.leading, 40)

            ContributionStatRow(
                icon: "play.rectangle.fill",
                iconColor: .green,
                label: "Backing Tracks",
                count: stats.backingTracks,
                description: "Practice backing tracks submitted"
            )

            Divider()
                .padding(.leading, 40)

            ContributionStatRow(
                icon: "metronome",
                iconColor: ApproachNoteTheme.textSecondary,
                label: "Tempo Markings",
                count: stats.tempoMarkings,
                description: "Recording tempo contributions"
            )

            Divider()
                .padding(.leading, 40)

            ContributionStatRow(
                icon: "mic.fill",
                iconColor: .purple,
                label: "Vocal/Instrumental",
                count: stats.instrumentalVocal,
                description: "Recording type contributions"
            )

            Divider()
                .padding(.leading, 40)

            ContributionStatRow(
                icon: "music.note",
                iconColor: .blue,
                label: "Performance Keys",
                count: stats.keys,
                description: "Recording key contributions"
            )
        }

        // Total contributions summary
        if stats.totalContributions > 0 {
            HStack {
                Spacer()
                Text("Total: \(stats.totalContributions) contribution\(stats.totalContributions == 1 ? "" : "s")")
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(ApproachNoteTheme.textSecondary)
                    .fontWeight(.medium)
            }
            .padding(.top, ApproachNoteTheme.spacingXS)
        }
    }

    // MARK: - Not Authenticated View

    @ViewBuilder
    private var notAuthenticatedView: some View {
        VStack(spacing: ApproachNoteTheme.spacingXL) {
            Spacer()

            Image(systemName: "person.3.fill")
                .font(.system(size: 60))
                .foregroundColor(ApproachNoteTheme.textSecondary.opacity(0.5))

            VStack(spacing: ApproachNoteTheme.spacingXS) {
                Text("Sign In to View Contributions")
                    .font(ApproachNoteTheme.headline())
                    .foregroundColor(ApproachNoteTheme.textPrimary)

                Text("Track your contributions to the community including transcriptions, backing tracks, and recording metadata.")
                    .font(ApproachNoteTheme.body())
                    .bodyLineSpacing()
                    .foregroundColor(ApproachNoteTheme.textSecondary)
                    .multilineTextAlignment(.center)
                    .padding(.horizontal, 40)
            }

            Text("Sign in using the Account tab")
                .font(ApproachNoteTheme.subheadline())
                .foregroundColor(ApproachNoteTheme.brand)

            Spacer()
        }
        .frame(maxWidth: .infinity)
    }

    // MARK: - Data Loading

    private func loadContributionStats() async {
        guard let token = await authManager.validAccessToken() else {
            isLoading = false
            errorMessage = "Not authenticated"
            return
        }

        isLoading = true
        errorMessage = nil

        if let stats = await contributionService.fetchUserContributionStats(authToken: token) {
            contributionStats = stats
        } else {
            errorMessage = "Could not load contribution stats"
        }

        isLoading = false
    }
}

// MARK: - Contribution Stat Row

struct ContributionStatRow: View {
    let icon: String
    let iconColor: Color
    let label: String
    let count: Int
    let description: String

    var body: some View {
        HStack(spacing: ApproachNoteTheme.spacingSM) {
            Image(systemName: icon)
                .font(.system(size: 18))
                .foregroundColor(iconColor)
                .frame(width: 28)

            VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXXS) {
                Text(label)
                    .font(ApproachNoteTheme.body())
                    .bodyLineSpacing()
                    .foregroundColor(ApproachNoteTheme.textPrimary)

                Text(description)
                    .font(ApproachNoteTheme.caption())
                    .foregroundColor(ApproachNoteTheme.textSecondary)
            }

            Spacer()

            Text("\(count)")
                .font(ApproachNoteTheme.title2())
                .fontWeight(.semibold)
                .foregroundColor(count > 0 ? ApproachNoteTheme.textPrimary : ApproachNoteTheme.textSecondary.opacity(0.5))
        }
        .padding(.vertical, ApproachNoteTheme.spacingXS)
    }
}

// MARK: - Previews

#Preview("Not Authenticated") {
    CommunitySettingsView()
        .environmentObject(AuthenticationManager())
        .frame(width: 500, height: 400)
}
