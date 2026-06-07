//
//  TranscriptionsSection.swift
//  Approach Note
//
//  Section displaying solo transcriptions as YouTube thumbnails.
//  Tapping a thumbnail opens the video in the YouTube app or website
//  (full screen where supported).
//

import SwiftUI

struct TranscriptionsSection: View {
    let transcriptions: [SoloTranscription]

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
        if !transcriptions.isEmpty {
            Divider()
                .padding(.horizontal, ApproachNoteTheme.spacingXL)
                .padding(.top, ApproachNoteTheme.spacingMD)

            VStack(alignment: .leading, spacing: 0) {
                Text("SOLO TRANSCRIPTIONS")
                    .font(ApproachNoteTheme.title2())
                    .bold()
                    .foregroundColor(ApproachNoteTheme.textPrimary)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .padding(.vertical, ApproachNoteTheme.spacingSM)

                LazyVGrid(columns: columns, alignment: .leading, spacing: ApproachNoteTheme.spacingMD) {
                    ForEach(transcriptions) { transcription in
                        TranscriptionRowView(transcription: transcription)
                    }
                }
                .padding(.top, ApproachNoteTheme.spacingSM)
            }
            .padding(.horizontal, ApproachNoteTheme.spacingXL)
            .background(ApproachNoteTheme.background)
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
