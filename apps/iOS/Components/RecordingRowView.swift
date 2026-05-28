//
//  RecordingRowView.swift
//  Approach Note
//
//  Updated with album artwork and authority badge support
//  UPDATED: Added optional artist name display for year-based sorting
//  UPDATED: Added back cover flip support
//

import SwiftUI

struct RecordingRowView: View {
    let recording: Recording
    /// Song title from the surrounding context (e.g. SongDetailView).
    /// API responses nested under a song don't include `song_title` on
    /// each recording, so without this hint we can't tell when the
    /// recording title duplicates the song name. Falls back to
    /// `recording.songTitle` when nil.
    var parentSongTitle: String? = nil
    /// True when at least one recording in the surrounding shelf has a
    /// distinct title. Set by the shelf so every card in the shelf
    /// reserves matching space for the title line, while shelves with
    /// no distinct titles don't allocate the space at all.
    var shelfHasAnyDistinctTitle: Bool = false
    /// Called once when the row appears on screen. SongDetailView passes
    /// a closure that forwards the recording ID to
    /// `SongDetailViewModel.requestHydration(for:)`, which drives the
    /// shell+hydrate pattern (rows start as skeletons, gain cover art +
    /// full performers when hydrated). Other call sites that already
    /// pass a fully-loaded recording leave this nil.
    var onVisible: ((String) -> Void)? = nil

    private var displayedRecordingTitle: String? {
        recording.displayTitle(comparedTo: parentSongTitle)
    }

    // Get artist name - prefer artist_credit from default release, fall back to performers
    private var artistName: String {
        // Use artist_credit from the default release if available
        if let artistCredit = recording.artistCredit, !artistCredit.isEmpty {
            return artistCredit
        }
        // Fall back to performers lookup
        if let performers = recording.performers {
            // First try to find a performer with "leader" role
            if let leader = performers.first(where: { $0.role?.lowercased() == "leader" }) {
                return leader.name
            }
            // Fall back to first performer if no leader
            if let first = performers.first {
                return first.name
            }
        }
        return "Unknown Artist"
    }

    // Front cover URL
    private var frontCoverUrl: String? {
        recording.bestAlbumArtMedium ?? recording.bestAlbumArtSmall
    }

    var body: some View {
        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXS) {
            // Album artwork
            ZStack(alignment: .topTrailing) {
                if let frontUrl = frontCoverUrl {
                    CachedAsyncImage(
                        url: URL(string: frontUrl),
                        content: { image in
                            image
                                .resizable()
                                .aspectRatio(contentMode: .fill)
                                .frame(width: 150, height: 150)
                                .clipped()
                        },
                        placeholder: {
                            ZStack {
                                ApproachNoteTheme.surface
                                ProgressView()
                                    .tint(ApproachNoteTheme.textSecondary)
                            }
                            .frame(width: 150, height: 150)
                        }
                    )
                } else {
                    Image(systemName: "opticaldisc")
                        .font(ApproachNoteTheme.largeTitle())
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                        .frame(width: 150, height: 150)
                        .background(ApproachNoteTheme.surface)
                }

                // Canonical star badge
                if recording.isCanonical == true {
                    Image(systemName: "star.fill")
                        .foregroundColor(.yellow)
                        .font(ApproachNoteTheme.caption())
                        .padding(6)
                        .background(Color.black.opacity(0.6))
                        .clipShape(Circle())
                        .padding(6)
                }
            }
            .cornerRadius(8)
            .frame(width: 150)

            // Year
            if let year = recording.recordingYear {
                Text(String(format: "%d", year))
                    .font(ApproachNoteTheme.subheadline(weight: .bold))
                    .foregroundColor(ApproachNoteTheme.textPrimary)
                    .frame(width: 150, alignment: .leading)
            }

            // Artist name
            Text(artistName)
                .font(ApproachNoteTheme.subheadline(weight: .bold))
                .foregroundColor(ApproachNoteTheme.textPrimary)
                .lineLimit(1)
                .frame(width: 150, alignment: .leading)

            // Album title — wraps naturally to 1-2 lines so the song
            // title below can pull up when the album fits on one line.
            Text(recording.albumTitle ?? "Unknown Album")
                .font(ApproachNoteTheme.subheadline())
                .foregroundColor(ApproachNoteTheme.textPrimary)
                .lineLimit(2)
                .frame(width: 150, alignment: .leading)

            // Recording title — only allocated when some card in this
            // shelf actually has a distinct title. Cards in the same
            // shelf that have no distinct title render an empty
            // placeholder so all card heights stay aligned.
            if shelfHasAnyDistinctTitle {
                Text(displayedRecordingTitle.map { "(\($0))" } ?? " ")
                    .font(ApproachNoteTheme.caption(italic: true))
                    .foregroundColor(ApproachNoteTheme.textSecondary)
                    .lineLimit(1, reservesSpace: true)
                    .frame(width: 150, alignment: .leading)
            }
        }
        .frame(width: 150)
        .onAppear {
            // Shell+hydrate hook: tell the ViewModel that this row just
            // became visible so it queues a batch hydration. No-op on
            // call sites that don't pass onVisible (rows rendered outside
            // the song recordings list, which already receive full data).
            onVisible?(recording.id)
        }
    }
}

// MARK: - Previews

#Preview("With Album Art") {
    RecordingRowView(recording: .preview1)
        .padding()
}

#Preview("Second Recording") {
    RecordingRowView(recording: .preview2)
        .padding()
}

#Preview("Minimal") {
    RecordingRowView(recording: .previewMinimal)
        .padding()
}

