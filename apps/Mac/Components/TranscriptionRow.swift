//
//  TranscriptionRow.swift
//  Approach Note
//
//  Row view for a single solo transcription in Mac SongDetailView
//

import SwiftUI

// MARK: - Transcription Row

struct TranscriptionRow: View {
    let transcription: SoloTranscription
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

                // Transcription info
                VStack(alignment: .leading, spacing: 4) {
                    Text(transcription.albumTitle ?? "Solo Transcription")
                        .font(ApproachNoteTheme.headline())
                        .foregroundColor(ApproachNoteTheme.textPrimary)
                        .lineLimit(2)

                    HStack(spacing: 12) {
                        if let year = transcription.recordingYear {
                            HStack(spacing: 4) {
                                Image(systemName: "calendar")
                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                                    .font(ApproachNoteTheme.caption())
                                Text(String(format: "%d", year))
                                    .font(ApproachNoteTheme.subheadline())
                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                            }
                        }

                        if let label = transcription.label {
                            HStack(spacing: 4) {
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

                // YouTube icon indicator
                if transcription.youtubeUrl != nil {
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
        .help(transcription.youtubeUrl != nil ? "Watch on YouTube" : "No video available")
    }

    private func openYouTube() {
        guard let urlString = transcription.youtubeUrl,
              let url = URL(string: urlString) else { return }
        openURL(url)
    }
}
