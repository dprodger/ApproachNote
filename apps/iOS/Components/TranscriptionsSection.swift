//
//  TranscriptionsSection.swift
//  Approach Note
//
//  Collapsible section displaying solo transcriptions
//  Uses a single shared YouTube player presented in a sheet for better performance
//

import SwiftUI
import YouTubePlayerKit

struct TranscriptionsSection: View {
    let transcriptions: [SoloTranscription]

    @State private var isSectionExpanded: Bool = true
    @State private var selectedTranscription: SoloTranscription?

    var body: some View {
        if !transcriptions.isEmpty {
            Divider()
                .padding(.horizontal, 24)
                .padding(.top, 16)

            // HStack with explicit spacers ensures DisclosureGroup chevron is properly inset
            HStack(spacing: 0) {
                Spacer().frame(width: 24)

                VStack(alignment: .leading, spacing: 0) {
                    DisclosureGroup(
                        isExpanded: $isSectionExpanded,
                    content: {
                        VStack(alignment: .leading, spacing: 12) {
                            ForEach(transcriptions) { transcription in
                                TranscriptionRowView(transcription: transcription) {
                                    selectedTranscription = transcription
                                }
                            }
                        }
                        .padding(.top, 12)
                    },
                    label: {
                        HStack {
                            Image(systemName: "music.quarternote.3")
                                .foregroundColor(ApproachNoteTheme.accent)
                            Text("Solo Transcriptions")
                                .font(ApproachNoteTheme.title2())
                                .bold()
                                .foregroundColor(ApproachNoteTheme.textPrimary)

                            Spacer()

                            Text("\(transcriptions.count)")
                                .font(ApproachNoteTheme.subheadline())
                                .foregroundColor(ApproachNoteTheme.textSecondary)
                                .padding(.horizontal, 8)
                                .padding(.vertical, 4)
                                .background(ApproachNoteTheme.accent.opacity(0.1))
                                .cornerRadius(6)
                        }
                        .padding(.vertical, 12)
                    }
                )
                .tint(ApproachNoteTheme.accent)
            }

            Spacer().frame(width: 24)
            }
            .background(ApproachNoteTheme.background)
            .sheet(item: $selectedTranscription) { transcription in
                TranscriptionPlayerSheet(transcription: transcription)
            }
        }
    }
}

// MARK: - Transcription Player Sheet

struct TranscriptionPlayerSheet: View {
    let transcription: SoloTranscription
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        NavigationStack {
            VStack(spacing: 0) {
                // YouTube Player
                if let youtubeUrl = transcription.youtubeUrl {
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
                        description: Text("This transcription has no video URL")
                    )
                    .frame(height: 200)
                }

                // Transcription details
                VStack(alignment: .leading, spacing: 12) {
                    // Recording details
                    HStack(spacing: 16) {
                        if let year = transcription.recordingYear {
                            HStack(spacing: 4) {
                                Image(systemName: "calendar")
                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                                Text(String(format: "%d", year))
                                    .font(ApproachNoteTheme.subheadline())
                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                            }
                        }

                        if let label = transcription.label {
                            HStack(spacing: 4) {
                                Image(systemName: "opticaldisc")
                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                                Text(label)
                                    .font(ApproachNoteTheme.subheadline())
                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                            }
                        }
                    }

                    if let composer = transcription.composer {
                        HStack(spacing: 4) {
                            Image(systemName: "music.note.list")
                                .foregroundColor(ApproachNoteTheme.textSecondary)
                            Text("Composed by \(composer)")
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
            .navigationTitle(transcription.albumTitle ?? "Solo Transcription")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .topBarTrailing) {
                    Button("Done") {
                        dismiss()
                    }
                }
            }
        }
    }
}

// MARK: - Previews

#Preview("Transcriptions Section") {
    ScrollView {
        TranscriptionsSection(transcriptions: [.preview1, .preview2])
    }
}

#Preview("Empty Section") {
    ScrollView {
        TranscriptionsSection(transcriptions: [])
    }
}

#Preview("Player Sheet") {
    TranscriptionPlayerSheet(transcription: .preview1)
}
