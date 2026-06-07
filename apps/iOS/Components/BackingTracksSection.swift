//
//  BackingTracksSection.swift
//  Approach Note
//
//  Section displaying backing track videos as YouTube thumbnails.
//  Tapping a thumbnail opens the video in the YouTube app or website
//  (full screen where supported).
//

import SwiftUI

// MARK: - Backing Tracks Section

struct BackingTracksSection: View {
    let videos: [Video]

    // Two across on iPad (regular width), one across on iPhone (compact).
    @Environment(\.horizontalSizeClass) private var horizontalSizeClass

    private var columns: [GridItem] {
        let count = horizontalSizeClass == .regular ? 2 : 1
        return Array(
            repeating: GridItem(.flexible(), spacing: ApproachNoteTheme.spacingMD, alignment: .top),
            count: count
        )
    }

    var body: some View {
        if !videos.isEmpty {
            Divider()
                .padding(.horizontal, ApproachNoteTheme.spacingXL)
                .padding(.top, ApproachNoteTheme.spacingMD)

            VStack(alignment: .leading, spacing: 0) {
                Text("BACKING TRACKS")
                    .font(ApproachNoteTheme.title2())
                    .bold()
                    .foregroundColor(ApproachNoteTheme.textPrimary)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .padding(.vertical, ApproachNoteTheme.spacingSM)

                LazyVGrid(columns: columns, alignment: .leading, spacing: ApproachNoteTheme.spacingMD) {
                    ForEach(videos) { video in
                        VideoRowView(video: video)
                    }
                }
                .padding(.top, ApproachNoteTheme.spacingSM)
            }
            .padding(.horizontal, ApproachNoteTheme.spacingXL)
            .background(ApproachNoteTheme.background)
        }
    }
}

// MARK: - Video Row View

struct VideoRowView: View {
    let video: Video
    @Environment(\.openURL) private var openURL

    var body: some View {
        Button(action: openVideo) {
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
        }
    }

    private func openVideo() {
        if let url = YouTube.watchURL(from: video.youtubeUrl) {
            openURL(url)
        }
    }

    private func formatDuration(_ seconds: Int) -> String {
        let minutes = seconds / 60
        let remainingSeconds = seconds % 60
        return String(format: "%d:%02d", minutes, remainingSeconds)
    }
}

// MARK: - Preview

#Preview {
    ScrollView {
        BackingTracksSection(videos: [
            Video(
                id: "preview-1",
                songId: "song-1",
                recordingId: nil,
                youtubeUrl: "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
                title: "All of Me - Backing Track in C",
                description: "Professional backing track for practice",
                videoType: "backing_track",
                durationSeconds: 300,
                tempo: 130,
                keySignature: "C Major",
                createdAt: nil,
                updatedAt: nil
            ),
            Video(
                id: "preview-2",
                songId: "song-1",
                recordingId: nil,
                youtubeUrl: "https://www.youtube.com/watch?v=abc123",
                title: "All of Me - Slow Tempo",
                description: nil,
                videoType: "backing_track",
                durationSeconds: 360,
                tempo: 100,
                keySignature: nil,
                createdAt: nil,
                updatedAt: nil
            )
        ])
    }
}
