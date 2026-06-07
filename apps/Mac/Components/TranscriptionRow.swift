//
//  TranscriptionRow.swift
//  Approach Note
//
//  Card view for a single solo transcription in Mac SongDetailView.
//  Shows a YouTube thumbnail with the title below; tapping opens the
//  video in the YouTube app or website.
//

import SwiftUI

// MARK: - Transcription Row

struct TranscriptionRow: View {
    let transcription: SoloTranscription
    @State private var isHovering = false
    @Environment(\.openURL) private var openURL

    /// Cap the thumbnail so cards stay a sensible size in a wide detail pane.
    private let cardWidth: CGFloat = 360

    var body: some View {
        Button(action: openYouTube) {
            VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingSM) {
                // YouTube thumbnail
                YouTubeThumbnailView(youtubeUrl: transcription.youtubeUrl, maxWidth: cardWidth)

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
            .frame(width: cardWidth, alignment: .leading)
            .contentShape(Rectangle())
            .opacity(isHovering ? 0.85 : 1.0)
        }
        .buttonStyle(.plain)
        .onHover { hovering in
            isHovering = hovering
        }
        .animation(.easeInOut(duration: 0.15), value: isHovering)
        .help(transcription.youtubeUrl != nil ? "Watch on YouTube" : "No video available")
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

    private func openYouTube() {
        guard let url = YouTube.watchURL(from: transcription.youtubeUrl) else { return }
        openURL(url)
    }
}
