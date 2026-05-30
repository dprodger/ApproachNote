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

    @State private var selectedTranscription: SoloTranscription?

    var body: some View {
        if !transcriptions.isEmpty {
            Divider()
                .padding(.horizontal, ApproachNoteTheme.spacingXL)
                .padding(.top, ApproachNoteTheme.spacingMD)

            HStack(spacing: 0) {
                Spacer().frame(width: 24)

                VStack(alignment: .leading, spacing: 0) {
                    Text("SOLO TRANSCRIPTIONS")
                        .font(ApproachNoteTheme.title2())
                        .bold()
                        .foregroundColor(ApproachNoteTheme.textPrimary)
                        .frame(maxWidth: .infinity, alignment: .leading)
                        .padding(.vertical, ApproachNoteTheme.spacingSM)

                    VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingSM) {
                        ForEach(transcriptions) { transcription in
                            TranscriptionRowView(transcription: transcription) {
                                selectedTranscription = transcription
                            }
                        }
                    }
                    .padding(.top, ApproachNoteTheme.spacingSM)
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
                VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingSM) {
                    // Recording details
                    HStack(spacing: ApproachNoteTheme.spacingMD) {
                        if let year = transcription.recordingYear {
                            HStack(spacing: ApproachNoteTheme.spacingXXS) {
                                Image(systemName: "calendar")
                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                                Text(String(format: "%d", year))
                                    .font(ApproachNoteTheme.subheadline())
                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                            }
                        }

                        if let label = transcription.label {
                            HStack(spacing: ApproachNoteTheme.spacingXXS) {
                                Image(systemName: "opticaldisc")
                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                                Text(label)
                                    .font(ApproachNoteTheme.subheadline())
                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                            }
                        }
                    }

                    if let composer = transcription.composer {
                        HStack(spacing: ApproachNoteTheme.spacingXXS) {
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
