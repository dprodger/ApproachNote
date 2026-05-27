//
//  MacCommunityDataSection.swift
//  Approach Note
//
//  Displays community-contributed metadata for a recording (key, tempo, instrumental/vocal)
//  Shows consensus values calculated from all user contributions
//

import SwiftUI

struct MacCommunityDataSection: View {
    let recordingId: String
    let communityData: CommunityData?
    let userContribution: UserContribution?
    let isAuthenticated: Bool
    let onEditTapped: () -> Void

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            // Header
            HStack {
                Image(systemName: "person.3.fill")
                    .foregroundColor(ApproachNoteTheme.textSecondary)
                Text("Community Data")
                    .font(ApproachNoteTheme.headline())
                    .foregroundColor(ApproachNoteTheme.textPrimary)

                Spacer()

                // Edit/Contribute button
                if isAuthenticated {
                    Button {
                        onEditTapped()
                    } label: {
                        HStack(spacing: 4) {
                            Image(systemName: userContribution != nil ? "pencil" : "plus")
                            Text(userContribution != nil ? "Edit" : "Contribute")
                        }
                        .font(ApproachNoteTheme.caption())
                    }
                    .buttonStyle(.borderedProminent)
                    .tint(ApproachNoteTheme.brand)
                }
            }

            // Data rows
            if let data = communityData, hasAnyData(data) {
                VStack(spacing: 8) {
                    // Performance Key
                    MacCommunityDataRow(
                        icon: "music.note",
                        label: "Key",
                        value: data.consensus.performanceKey ?? "Not set",
                        count: data.counts.key ?? 0,
                        userValue: userContribution?.performanceKey,
                        isEmpty: data.consensus.performanceKey == nil
                    )

                    // Tempo
                    MacCommunityDataRow(
                        icon: "metronome",
                        label: "Tempo",
                        value: data.consensus.tempoMarking ?? "Not set",
                        count: data.counts.tempo ?? 0,
                        userValue: userContribution?.tempoMarking,
                        isEmpty: data.consensus.tempoMarking == nil,
                        helpText: data.consensus.tempoMarking.flatMap { TempoMarking(rawValue: $0)?.bpmRange }.map { "\($0) BPM" }
                    )

                    // Instrumental/Vocal
                    MacCommunityDataRow(
                        icon: data.consensus.isInstrumental == true ? "pianokeys" : "mic",
                        label: "Type",
                        value: formatInstrumental(data.consensus.isInstrumental),
                        count: data.counts.instrumental,
                        userValue: userContribution?.isInstrumental.map { formatInstrumentalValue($0) },
                        isEmpty: data.consensus.isInstrumental == nil
                    )
                }
                .padding()
                .background(ApproachNoteTheme.surface)
                .cornerRadius(8)
            } else {
                // No data yet
                VStack(alignment: .center, spacing: 8) {
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
                .padding()
                .background(ApproachNoteTheme.surface)
                .cornerRadius(8)
            }
        }
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

struct MacCommunityDataRow: View {
    let icon: String
    let label: String
    let value: String
    let count: Int
    let userValue: String?
    let isEmpty: Bool
    var helpText: String? = nil

    var body: some View {
        HStack(alignment: .center, spacing: 12) {
            Image(systemName: icon)
                .foregroundColor(isEmpty ? ApproachNoteTheme.textSecondary.opacity(0.5) : ApproachNoteTheme.textSecondary)
                .frame(width: 20)

            Text(label)
                .font(ApproachNoteTheme.subheadline())
                .foregroundColor(ApproachNoteTheme.textSecondary)
                .frame(width: 50, alignment: .leading)

            Spacer()

            VStack(alignment: .trailing, spacing: 2) {
                if let help = helpText {
                    Text(value)
                        .font(ApproachNoteTheme.body())
                        .bodyLineSpacing()
                        .fontWeight(isEmpty ? .regular : .medium)
                        .foregroundColor(isEmpty ? ApproachNoteTheme.textSecondary.opacity(0.5) : ApproachNoteTheme.textPrimary)
                        .help(help)
                } else {
                    Text(value)
                        .font(ApproachNoteTheme.body())
                        .bodyLineSpacing()
                        .fontWeight(isEmpty ? .regular : .medium)
                        .foregroundColor(isEmpty ? ApproachNoteTheme.textSecondary.opacity(0.5) : ApproachNoteTheme.textPrimary)
                }

                if count > 0 {
                    Text("\(count) \(count == 1 ? "vote" : "votes")")
                        .font(ApproachNoteTheme.caption2())
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                }

                // Show user's value if different from consensus
                if let userVal = userValue, !isEmpty, userVal != value {
                    Text("You: \(userVal)")
                        .font(ApproachNoteTheme.caption2())
                        .foregroundColor(ApproachNoteTheme.brand)
                }
            }
        }
        .padding(.vertical, 4)
    }
}

// MARK: - Previews

#Preview("With Data") {
    MacCommunityDataSection(
        recordingId: "preview-1",
        communityData: CommunityData.preview,
        userContribution: UserContribution.preview,
        isAuthenticated: true,
        onEditTapped: {}
    )
    .padding()
    .frame(width: 400)
}

#Preview("Empty Data - Authenticated") {
    MacCommunityDataSection(
        recordingId: "preview-2",
        communityData: CommunityData.previewEmpty,
        userContribution: nil,
        isAuthenticated: true,
        onEditTapped: {}
    )
    .padding()
    .frame(width: 400)
}

#Preview("Empty Data - Not Authenticated") {
    MacCommunityDataSection(
        recordingId: "preview-3",
        communityData: nil,
        userContribution: nil,
        isAuthenticated: false,
        onEditTapped: {}
    )
    .padding()
    .frame(width: 400)
}
