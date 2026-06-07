// TranscriptionRowView.swift
// Tappable card showing a solo transcription as a YouTube thumbnail.
// Tapping opens the video in the YouTube app or website (full screen
// where supported).

import SwiftUI

// MARK: - Transcription Row View
struct TranscriptionRowView: View {
    let transcription: SoloTranscription
    @Environment(\.openURL) private var openURL

    var body: some View {
        Button(action: openVideo) {
            VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingSM) {
                // YouTube thumbnail
                YouTubeThumbnailView(youtubeUrl: transcription.youtubeUrl)

                // Title and metadata below the thumbnail
                VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXXS) {
                    YouTubeTitleView(
                        youtubeUrl: transcription.youtubeUrl,
                        storedTitle: transcription.albumTitle,
                        placeholder: "Solo Transcription"
                    )

                    HStack(spacing: ApproachNoteTheme.spacingSM) {
                        if let year = transcription.recordingYear {
                            metadataBadge(icon: "calendar", text: String(format: "%d", year))
                        }
                        if let label = transcription.label {
                            metadataBadge(icon: "opticaldisc", text: label)
                        }
                    }
                }
            }
            .frame(maxWidth: .infinity, alignment: .leading)
            .contentShape(Rectangle())
        }
        .buttonStyle(.plain)
    }

    private func metadataBadge(icon: String, text: String) -> some View {
        HStack(spacing: ApproachNoteTheme.spacingXXS) {
            Image(systemName: icon)
                .foregroundColor(ApproachNoteTheme.textSecondary)
                .font(ApproachNoteTheme.caption())
            Text(text)
                .font(ApproachNoteTheme.subheadline())
                .foregroundColor(ApproachNoteTheme.textSecondary)
                .lineLimit(1)
        }
    }

    private func openVideo() {
        if let url = YouTube.watchURL(from: transcription.youtubeUrl) {
            openURL(url)
        }
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
            )
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
            )
        )
    }
}
