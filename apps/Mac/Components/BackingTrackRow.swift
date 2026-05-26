//
//  BackingTrackRow.swift
//  Approach Note
//
//  Row view for a single backing-track video in Mac SongDetailView
//

import SwiftUI

// MARK: - Backing Track Row

struct BackingTrackRow: View {
    let video: Video
    @State private var isHovering = false
    @Environment(\.openURL) private var openURL

    var body: some View {
        Button(action: openYouTube) {
            HStack(spacing: 12) {
                // Play button thumbnail
                ZStack {
                    RoundedRectangle(cornerRadius: 8)
                        .fill(ApproachNoteTheme.accent.opacity(0.15))
                        .frame(width: 80, height: 45)

                    Image(systemName: "play.fill")
                        .font(.system(size: 20))
                        .foregroundColor(ApproachNoteTheme.accent)
                }

                // Video info
                VStack(alignment: .leading, spacing: 4) {
                    Text(video.title ?? "Backing Track")
                        .font(ApproachNoteTheme.headline())
                        .foregroundColor(ApproachNoteTheme.textPrimary)
                        .lineLimit(2)

                    HStack(spacing: 8) {
                        if let duration = video.durationSeconds {
                            HStack(spacing: 4) {
                                Image(systemName: "clock")
                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                                    .font(ApproachNoteTheme.caption())
                                Text(formatDuration(duration))
                                    .font(ApproachNoteTheme.subheadline())
                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                            }
                        }

                        if let tempo = video.tempo {
                            HStack(spacing: 4) {
                                Image(systemName: "metronome")
                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                                    .font(ApproachNoteTheme.caption())
                                Text("\(tempo) BPM")
                                    .font(ApproachNoteTheme.subheadline())
                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                            }
                        }

                        if let key = video.keySignature {
                            HStack(spacing: 4) {
                                Image(systemName: "music.note")
                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                                    .font(ApproachNoteTheme.caption())
                                Text(key)
                                    .font(ApproachNoteTheme.subheadline())
                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                            }
                        }
                    }
                }

                Spacer()

                // YouTube icon indicator
                if video.youtubeUrl != nil {
                    Image(systemName: "play.rectangle.fill")
                        .font(.title2)
                        .foregroundColor(.red)
                }
            }
            .padding()
            .background(isHovering ? ApproachNoteTheme.background : Color.white)
            .cornerRadius(10)
            .overlay(
                RoundedRectangle(cornerRadius: 10)
                    .stroke(isHovering ? ApproachNoteTheme.accent.opacity(0.5) : Color.clear, lineWidth: 2)
            )
        }
        .buttonStyle(.plain)
        .onHover { hovering in
            isHovering = hovering
        }
        .animation(.easeInOut(duration: 0.15), value: isHovering)
        .help(video.youtubeUrl != nil ? "Watch on YouTube" : "No video available")
    }

    private func openYouTube() {
        guard let urlString = video.youtubeUrl,
              let url = URL(string: urlString) else { return }
        openURL(url)
    }

    private func formatDuration(_ seconds: Int) -> String {
        let minutes = seconds / 60
        let remainingSeconds = seconds % 60
        return String(format: "%d:%02d", minutes, remainingSeconds)
    }
}
