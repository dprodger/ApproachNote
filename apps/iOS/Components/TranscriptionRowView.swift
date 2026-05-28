// TranscriptionRowView.swift
// Tappable row component for displaying solo transcriptions
// YouTube player is shown in a sheet for better performance

import SwiftUI

// MARK: - Transcription Row View
struct TranscriptionRowView: View {
    let transcription: SoloTranscription
    let onTap: () -> Void

    var body: some View {
        Button(action: onTap) {
            HStack(spacing: ApproachNoteTheme.spacingSM) {
                // Play button thumbnail
                ZStack {
                    RoundedRectangle(cornerRadius: 8)
                        .fill(ApproachNoteTheme.accent.opacity(0.15))
                        .frame(width: 80, height: 45)

                    Image(systemName: "play.fill")
                        .font(.system(size: 20))
                        .foregroundColor(ApproachNoteTheme.accent)
                }

                // Transcription info
                VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXXS) {
                    // Album/Recording title
                    Text(transcription.albumTitle ?? "Solo Transcription")
                        .font(ApproachNoteTheme.headline())
                        .foregroundColor(ApproachNoteTheme.textPrimary)
                        .lineLimit(2)
                        .multilineTextAlignment(.leading)

                    // Recording details
                    HStack(spacing: ApproachNoteTheme.spacingSM) {
                        if let year = transcription.recordingYear {
                            HStack(spacing: ApproachNoteTheme.spacingXXS) {
                                Image(systemName: "calendar")
                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                                    .font(ApproachNoteTheme.caption())
                                Text(String(format: "%d", year))
                                    .font(ApproachNoteTheme.subheadline())
                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                            }
                        }

                        if let label = transcription.label {
                            HStack(spacing: ApproachNoteTheme.spacingXXS) {
                                Image(systemName: "opticaldisc")
                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                                    .font(ApproachNoteTheme.caption())
                                Text(label)
                                    .font(ApproachNoteTheme.subheadline())
                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                                    .lineLimit(1)
                            }
                        }
                    }
                }

                Spacer()

                // Chevron indicator
                Image(systemName: "chevron.right")
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(ApproachNoteTheme.textSecondary)
            }
            .padding()
            .frame(maxWidth: .infinity, alignment: .leading)
            .background(ApproachNoteTheme.surface)
            .cornerRadius(10)
            .padding(.horizontal)
        }
        .buttonStyle(.plain)
    }
}

// MARK: - Preview
#Preview {
    VStack {
        TranscriptionRowView(
            transcription: SoloTranscription(
                id: "preview-1",
                songId: "song-1",
                recordingId: "rec-1",
                youtubeUrl: "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
                createdAt: nil,
                updatedAt: nil,
                songTitle: "Autumn Leaves",
                albumTitle: "Kind of Blue",
                recordingYear: 1959,
                composer: "Joseph Kosma",
                label: "Columbia"
            ),
            onTap: {}
        )

        TranscriptionRowView(
            transcription: SoloTranscription(
                id: "preview-2",
                songId: "song-1",
                recordingId: "rec-2",
                youtubeUrl: "https://www.youtube.com/watch?v=abc123",
                createdAt: nil,
                updatedAt: nil,
                songTitle: "Blue in Green",
                albumTitle: nil,
                recordingYear: nil,
                composer: nil,
                label: nil
            ),
            onTap: {}
        )
    }
}
