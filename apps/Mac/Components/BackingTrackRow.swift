//
//  BackingTrackRow.swift
//  Approach Note
//
//  Card view for a single backing-track video in Mac SongDetailView.
//  Shows a YouTube thumbnail with the title below; tapping opens the
//  video in the YouTube app or website.
//

import SwiftUI

// MARK: - Backing Track Row

struct BackingTrackRow: View {
    let video: Video
    @State private var isHovering = false
    @Environment(\.openURL) private var openURL

    var body: some View {
        Button(action: openYouTube) {
            VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingSM) {
                // YouTube thumbnail
                YouTubeThumbnailView(youtubeUrl: video.youtubeUrl)

                // Title and metadata below the thumbnail
                VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXXS) {
                    YouTubeTitleView(
                        youtubeUrl: video.youtubeUrl,
                        storedTitle: video.title,
                        placeholder: "Backing Track"
                    )

                    HStack(spacing: ApproachNoteTheme.spacingXS) {
                        if let duration = video.durationSeconds {
                            metadataBadge(icon: "clock", text: formatDuration(duration))
                        }
                        if let tempo = video.tempo {
                            metadataBadge(icon: "metronome", text: "\(tempo) BPM")
                        }
                        if let key = video.keySignature {
                            metadataBadge(icon: "music.note", text: key)
                        }
                    }
                }
            }
            .frame(maxWidth: .infinity, alignment: .leading)
            .contentShape(Rectangle())
            .opacity(isHovering ? 0.85 : 1.0)
        }
        .buttonStyle(.plain)
        .onHover { hovering in
            isHovering = hovering
        }
        .animation(.easeInOut(duration: 0.15), value: isHovering)
        .help(video.youtubeUrl != nil ? "Watch on YouTube" : "No video available")
    }

    private func metadataBadge(icon: String, text: String) -> some View {
        HStack(spacing: ApproachNoteTheme.spacingXXS) {
            Image(systemName: icon)
                .foregroundColor(ApproachNoteTheme.textSecondary)
                .font(ApproachNoteTheme.caption())
            Text(text)
                .font(ApproachNoteTheme.subheadline())
                .foregroundColor(ApproachNoteTheme.textSecondary)
        }
    }

    private func openYouTube() {
        guard let url = YouTube.watchURL(from: video.youtubeUrl) else { return }
        openURL(url)
    }

    private func formatDuration(_ seconds: Int) -> String {
        let minutes = seconds / 60
        let remainingSeconds = seconds % 60
        return String(format: "%d:%02d", minutes, remainingSeconds)
    }
}
