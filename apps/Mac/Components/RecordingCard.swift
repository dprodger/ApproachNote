//
//  RecordingCard.swift
//  Approach Note
//
//  Card view for a single recording in the Mac SongDetailView grid
//

import SwiftUI

// MARK: - Recording Card

struct RecordingCard: View {
    let recording: Recording
    /// Song title from the surrounding context. Nested-under-song API
    /// responses don't populate `song_title` on individual recordings,
    /// so we rely on the parent to supply it for the duplicate-title
    /// check. Falls back to `recording.songTitle` when nil.
    var parentSongTitle: String? = nil
    /// True when at least one recording in the surrounding shelf has a
    /// distinct title. Set by the shelf so every card reserves matching
    /// space for the title line, while shelves with no distinct titles
    /// don't allocate the space at all.
    var shelfHasAnyDistinctTitle: Bool = false
    /// Shell+hydrate viewport hook. SongDetailView passes a closure that
    /// forwards to `SongDetailViewModel.requestHydration(for:)`; other
    /// call sites leave this nil and render fully-loaded recordings.
    var onVisible: ((String) -> Void)? = nil

    @State private var isHovering = false

    private let artworkSize: CGFloat = 160

    private var artistName: String {
        if let artistCredit = recording.artistCredit, !artistCredit.isEmpty {
            return artistCredit
        }
        if let performers = recording.performers {
            if let leader = performers.first(where: { $0.role?.lowercased() == "leader" }) {
                return leader.name
            }
            if let first = performers.first {
                return first.name
            }
        }
        return "Unknown Artist"
    }

    // Front cover URL
    private var frontCoverUrl: String? {
        recording.bestAlbumArtLarge ?? recording.bestAlbumArtMedium
    }

    private var displayedRecordingTitle: String? {
        recording.displayTitle(comparedTo: parentSongTitle)
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            // Album art with canonical star overlay
            ZStack(alignment: .topTrailing) {
                Group {
                    if let frontUrl = frontCoverUrl {
                        AsyncImage(url: URL(string: frontUrl)) { phase in
                            switch phase {
                            case .empty:
                                Rectangle()
                                    .fill(ApproachNoteTheme.surface)
                                    .overlay { ProgressView() }
                            case .success(let image):
                                image
                                    .resizable()
                                    .aspectRatio(contentMode: .fill)
                            case .failure:
                                Rectangle()
                                    .fill(ApproachNoteTheme.surface)
                                    .overlay {
                                        Image(systemName: "music.note")
                                            .font(.system(size: 40))
                                            .foregroundColor(ApproachNoteTheme.textSecondary)
                                    }
                            @unknown default:
                                EmptyView()
                            }
                        }
                    } else {
                        Rectangle()
                            .fill(ApproachNoteTheme.surface)
                            .overlay {
                                Image(systemName: "music.note")
                                    .font(.system(size: 40))
                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                            }
                    }
                }
                .frame(width: artworkSize, height: artworkSize)
                .clipShape(RoundedRectangle(cornerRadius: 10))
                .shadow(color: .black.opacity(0.15), radius: 6, x: 0, y: 3)

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
            .frame(width: artworkSize, height: artworkSize)

            // Recording info below artwork — Year → Artist → Album → (Song Title)
            VStack(alignment: .leading, spacing: 4) {
                // Year
                if let year = recording.recordingYear {
                    Text(String(year))
                        .font(ApproachNoteTheme.subheadline(weight: .bold))
                        .foregroundColor(ApproachNoteTheme.textPrimary)
                }

                // Artist name
                Text(artistName)
                    .font(ApproachNoteTheme.subheadline(weight: .bold))
                    .foregroundColor(ApproachNoteTheme.textPrimary)
                    .lineLimit(1)

                // Album title — wraps naturally to 1-2 lines so the song
                // title below can pull up when the album fits on one line.
                Text(recording.albumTitle ?? "Unknown Album")
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(ApproachNoteTheme.textPrimary)
                    .lineLimit(2)

                // Recording title — only allocated when some card in this
                // shelf actually has a distinct title.
                if shelfHasAnyDistinctTitle {
                    Text(displayedRecordingTitle.map { "(\($0))" } ?? " ")
                        .font(ApproachNoteTheme.caption(italic: true))
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                        .lineLimit(1, reservesSpace: true)
                }
            }
            .frame(width: artworkSize, alignment: .leading)
        }
        .padding(12)
        .background(isHovering ? ApproachNoteTheme.background : ApproachNoteTheme.surface)
        .clipShape(RoundedRectangle(cornerRadius: 12))
        .overlay(
            RoundedRectangle(cornerRadius: 12)
                .stroke(isHovering ? ApproachNoteTheme.brand.opacity(0.5) : Color.clear, lineWidth: 2)
        )
        .onHover { hovering in
            isHovering = hovering
        }
        .animation(.easeInOut(duration: 0.15), value: isHovering)
        .onAppear {
            // Shell+hydrate viewport hook — tells the parent ViewModel
            // this recording is now in the viewport, so it can queue a
            // batch hydration request for it.
            onVisible?(recording.id)
        }
    }
}
