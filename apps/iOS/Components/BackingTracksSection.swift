//
//  BackingTracksSection.swift
//  Approach Note
//
//  Collapsible section displaying backing track videos
//  Uses a single shared YouTube player presented in a sheet for better performance
//

import SwiftUI
import YouTubePlayerKit

// MARK: - Backing Tracks Section

struct BackingTracksSection: View {
    let videos: [Video]

    @State private var selectedVideo: Video?

    var body: some View {
        if !videos.isEmpty {
            Divider()
                .padding(.horizontal, ApproachNoteTheme.spacingXL)
                .padding(.top, ApproachNoteTheme.spacingMD)

            HStack(spacing: 0) {
                Spacer().frame(width: 24)

                VStack(alignment: .leading, spacing: 0) {
                    Text("BACKING TRACKS")
                        .font(ApproachNoteTheme.title())
                        .bold()
                        .foregroundColor(ApproachNoteTheme.textPrimary)
                        .frame(maxWidth: .infinity, alignment: .leading)
                        .padding(.vertical, ApproachNoteTheme.spacingSM)

                    VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingSM) {
                        ForEach(videos) { video in
                            VideoRowView(video: video) {
                                selectedVideo = video
                            }
                        }
                    }
                    .padding(.top, ApproachNoteTheme.spacingSM)
                }

                Spacer().frame(width: 24)
            }
            .background(ApproachNoteTheme.background)
            .sheet(item: $selectedVideo) { video in
                VideoPlayerSheet(video: video)
            }
        }
    }
}

// MARK: - Video Row View

struct VideoRowView: View {
    let video: Video
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

                // Video info
                VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXXS) {
                    // Video title
                    Text(video.title ?? "Backing Track")
                        .font(ApproachNoteTheme.headline())
                        .foregroundColor(ApproachNoteTheme.textPrimary)
                        .lineLimit(2)
                        .multilineTextAlignment(.leading)

                    // Metadata badges
                    HStack(spacing: ApproachNoteTheme.spacingXS) {
                        if let duration = video.durationSeconds {
                            HStack(spacing: ApproachNoteTheme.spacingXXS) {
                                Image(systemName: "clock")
                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                                    .font(ApproachNoteTheme.caption())
                                Text(formatDuration(duration))
                                    .font(ApproachNoteTheme.subheadline())
                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                            }
                        }

                        if let tempo = video.tempo {
                            HStack(spacing: ApproachNoteTheme.spacingXXS) {
                                Image(systemName: "metronome")
                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                                    .font(ApproachNoteTheme.caption())
                                Text("\(tempo) BPM")
                                    .font(ApproachNoteTheme.subheadline())
                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                            }
                        }

                        if let key = video.keySignature {
                            HStack(spacing: ApproachNoteTheme.spacingXXS) {
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

    private func formatDuration(_ seconds: Int) -> String {
        let minutes = seconds / 60
        let remainingSeconds = seconds % 60
        return String(format: "%d:%02d", minutes, remainingSeconds)
    }
}

// MARK: - Video Player Sheet

struct VideoPlayerSheet: View {
    let video: Video
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        NavigationStack {
            VStack(spacing: 0) {
                // YouTube Player
                if let youtubeUrl = video.youtubeUrl {
                    YouTubePlayerView(.init(stringLiteral: youtubeUrl)) { state in
                        switch state {
                        case .idle:
                            ZStack {
                                Rectangle()
                                    .fill(Color.black)
                                ProgressView()
                                    .tint(.white)
                            }
                        case .ready:
                            EmptyView()
                        case .error(let error):
                            ContentUnavailableView(
                                "Error",
                                systemImage: "exclamationmark.triangle.fill",
                                description: Text(verbatim: "YouTube player couldn't be loaded: \(error)")
                            )
                        }
                    }
                    .aspectRatio(16/9, contentMode: .fit)
                } else {
                    ContentUnavailableView(
                        "No Video",
                        systemImage: "video.slash",
                        description: Text("This backing track has no video URL")
                    )
                    .frame(height: 200)
                }

                // Video details
                VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingSM) {
                    if let description = video.description, !description.isEmpty {
                        Text(description)
                            .font(ApproachNoteTheme.body())
                            .bodyLineSpacing()
                            .foregroundColor(ApproachNoteTheme.textSecondary)
                    }

                    if let duration = video.durationSeconds {
                        HStack(spacing: ApproachNoteTheme.spacingXXS) {
                            Image(systemName: "clock")
                                .foregroundColor(ApproachNoteTheme.textSecondary)
                            Text("Duration: \(formatDuration(duration))")
                                .font(ApproachNoteTheme.subheadline())
                                .foregroundColor(ApproachNoteTheme.textSecondary)
                        }
                    }

                    if let tempo = video.tempo {
                        HStack(spacing: ApproachNoteTheme.spacingXXS) {
                            Image(systemName: "metronome")
                                .foregroundColor(ApproachNoteTheme.textSecondary)
                            Text("Tempo: \(tempo) BPM")
                                .font(ApproachNoteTheme.subheadline())
                                .foregroundColor(ApproachNoteTheme.textSecondary)
                        }
                    }

                    if let key = video.keySignature {
                        HStack(spacing: ApproachNoteTheme.spacingXXS) {
                            Image(systemName: "music.note")
                                .foregroundColor(ApproachNoteTheme.textSecondary)
                            Text("Key: \(key)")
                                .font(ApproachNoteTheme.subheadline())
                                .foregroundColor(ApproachNoteTheme.textSecondary)
                        }
                    }
                }
                .frame(maxWidth: .infinity, alignment: .leading)
                .padding()

                Spacer()
            }
            .background(ApproachNoteTheme.background)
            .navigationTitle(video.title ?? "Backing Track")
            .navigationBarTitleDisplayMode(.inline)
            // Style the nav bar from the live palette (the global
            // UINavigationBar appearance is set once at launch and goes stale
            // when the palette changes), matching jazzNavigationBar.
            .toolbarBackground(ApproachNoteTheme.brand, for: .navigationBar)
            .toolbarBackground(.visible, for: .navigationBar)
            .toolbarColorScheme(.dark, for: .navigationBar)
            .toolbar {
                ToolbarItem(placement: .topBarTrailing) {
                    Button("Done") {
                        dismiss()
                    }
                    .fontWeight(.semibold)
                    .foregroundColor(ApproachNoteTheme.textOnDark)
                }
            }
        }
        .presentationDetents([.medium, .large])
        .presentationDragIndicator(.visible)
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
