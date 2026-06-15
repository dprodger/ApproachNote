//
//  CommunityDataSection.swift
//  Approach Note
//
//  Displays community-contributed metadata for a recording (key, tempo, instrumental/vocal)
//  Shows consensus values calculated from all user contributions
//

import SwiftUI

struct CommunityDataSection: View {
    let recordingId: String
    let communityData: CommunityData?
    let userContribution: UserContribution?
    let isAuthenticated: Bool
    let onEditTapped: () -> Void

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            Divider()
                .padding(.horizontal)
                .padding(.top, ApproachNoteTheme.spacingMD)

            HStack(spacing: 0) {
                Spacer().frame(width: 16)

                VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingSM) {
                    // Header
                    HStack {
                        Text("Community Data")
                            .font(ApproachNoteTheme.title2())
                            .bold()
                            .foregroundColor(ApproachNoteTheme.textPrimary)
                            .lineLimit(1)
                            .fixedSize(horizontal: true, vertical: false)

                        Spacer()

                        // Edit/Contribute button
                        if isAuthenticated {
                            Button {
                                onEditTapped()
                            } label: {
                                HStack(spacing: ApproachNoteTheme.spacingXXS) {
                                    Image(systemName: userContribution != nil ? "pencil" : "plus")
                                    Text(userContribution != nil ? "Edit" : "Contribute")
                                }
                                .font(ApproachNoteTheme.caption())
                                .foregroundColor(ApproachNoteTheme.brand)
                                .padding(.horizontal, ApproachNoteTheme.spacingXS)
                                .padding(.vertical, 6)
                                .background(ApproachNoteTheme.brand.opacity(0.1))
                                .cornerRadius(6)
                            }
                        }
                    }
                    .padding(.top, ApproachNoteTheme.spacingSM)

                    // Data rows
                    if let data = communityData, hasAnyData(data) {
                        VStack(spacing: ApproachNoteTheme.spacingXS) {
                            // Performance Key
                            CommunityDataRow(
                                label: "Key",
                                value: data.consensus.performanceKey ?? "Not set",
                                count: data.counts.key ?? 0,
                                userValue: userContribution?.performanceKey,
                                isEmpty: data.consensus.performanceKey == nil
                            )

                            // Tempo
                            CommunityDataRow(
                                label: "Tempo",
                                value: data.consensus.tempoMarking ?? "Not set",
                                count: data.counts.tempo ?? 0,
                                userValue: userContribution?.tempoMarking,
                                isEmpty: data.consensus.tempoMarking == nil,
                                subtitleText: data.consensus.tempoMarking.flatMap { TempoMarking(rawValue: $0)?.bpmRange }.map { "\($0) BPM" }
                            )

                            // Instrumental/Vocal
                            CommunityDataRow(
                                label: "Type",
                                value: formatInstrumental(data.consensus.isInstrumental),
                                count: data.counts.instrumental,
                                userValue: userContribution?.isInstrumental.map { formatInstrumentalValue($0) },
                                isEmpty: data.consensus.isInstrumental == nil
                            )
                        }
                    } else {
                        // No data yet
                        VStack(alignment: .center, spacing: ApproachNoteTheme.spacingXS) {
                            Text("No community data yet")
                                .font(ApproachNoteTheme.body())
                                .bodyLineSpacing()
                                .foregroundColor(ApproachNoteTheme.textSecondary)

                            if isAuthenticated {
                                Text("Be the first to contribute!")
                                    .font(ApproachNoteTheme.caption())
                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                            } else {
                                Text("Sign in to contribute data")
                                    .font(ApproachNoteTheme.caption())
                                    .foregroundColor(ApproachNoteTheme.brand)
                            }
                        }
                        .frame(maxWidth: .infinity)
                        .padding(.vertical, ApproachNoteTheme.spacingMD)
                    }
                }
                .padding(.bottom, ApproachNoteTheme.spacingSM)

                Spacer().frame(width: 16)
            }
        }
        .background(ApproachNoteTheme.background)
    }

    private func hasAnyData(_ data: CommunityData) -> Bool {
        (data.counts.key ?? 0) > 0 || (data.counts.tempo ?? 0) > 0 || data.counts.instrumental > 0
    }

    private func formatInstrumental(_ value: Bool?) -> String {
        switch value {
        case true: return "Instrumental"
        case false: return "Vocal"
        case nil: return "Not set"
        }
    }

    private func formatInstrumentalValue(_ value: Bool) -> String {
        value ? "Instrumental" : "Vocal"
    }
}

// MARK: - Data Row Component

struct CommunityDataRow: View {
    let label: String
    let value: String
    let count: Int
    let userValue: String?
    let isEmpty: Bool
    var subtitleText: String? = nil

    var body: some View {
        HStack(alignment: .firstTextBaseline, spacing: ApproachNoteTheme.spacingSM) {
            Text(label)
                .font(ApproachNoteTheme.subheadline())
                .foregroundColor(ApproachNoteTheme.textSecondary)
                .fixedSize(horizontal: true, vertical: false)

            Spacer()

            // Value, optional BPM range, and vote count all on one line.
            HStack(alignment: .firstTextBaseline, spacing: ApproachNoteTheme.spacingXS) {
                Text(value)
                    .font(ApproachNoteTheme.body())
                    .fontWeight(isEmpty ? .regular : .medium)
                    .foregroundColor(isEmpty ? ApproachNoteTheme.textSecondary.opacity(0.5) : ApproachNoteTheme.textPrimary)
                    .lineLimit(1)

                if let subtitle = subtitleText {
                    Text("(\(subtitle))")
                        .font(ApproachNoteTheme.footnote())
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                        .lineLimit(1)
                }

                if count > 0 {
                    Text("\(count) \(count == 1 ? "vote" : "votes")")
                        .font(ApproachNoteTheme.caption2())
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                        .lineLimit(1)
                }

                // Show user's value if different from consensus
                if let userVal = userValue, !isEmpty, userVal != value {
                    Text("You: \(userVal)")
                        .font(ApproachNoteTheme.caption2())
                        .foregroundColor(ApproachNoteTheme.brand)
                        .lineLimit(1)
                }
            }
        }
        .padding(.vertical, ApproachNoteTheme.spacingXXS)
    }
}

// MARK: - Previews

#Preview("With Data") {
    ScrollView {
        CommunityDataSection(
            recordingId: "preview-1",
            communityData: CommunityData.preview,
            userContribution: UserContribution.preview,
            isAuthenticated: true,
            onEditTapped: {}
        )
    }
}

#Preview("Empty Data - Authenticated") {
    ScrollView {
        CommunityDataSection(
            recordingId: "preview-2",
            communityData: CommunityData.previewEmpty,
            userContribution: nil,
            isAuthenticated: true,
            onEditTapped: {}
        )
    }
}

#Preview("Empty Data - Not Authenticated") {
    ScrollView {
        CommunityDataSection(
            recordingId: "preview-3",
            communityData: nil,
            userContribution: nil,
            isAuthenticated: false,
            onEditTapped: {}
        )
    }
}

#Preview("Partial Data") {
    ScrollView {
        CommunityDataSection(
            recordingId: "preview-4",
            communityData: CommunityData(
                consensus: CommunityConsensus.previewPartial,
                counts: ContributionCounts(key: 2, tempo: 0, instrumental: 0)
            ),
            userContribution: nil,
            isAuthenticated: true,
            onEditTapped: {}
        )
    }
}
