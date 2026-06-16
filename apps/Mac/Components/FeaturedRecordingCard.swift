//
//  FeaturedRecordingCard.swift
//  Approach Note
//
//  Larger card view used in the featured-recordings carousel in Mac SongDetailView
//

import SwiftUI

// MARK: - Featured Recording Card

struct FeaturedRecordingCard: View {
    let recording: Recording
    var parentSongTitle: String? = nil
    /// True when at least one card in the surrounding carousel has a
    /// distinct title. Set by the carousel so cards align in height
    /// without paying for an unused title line.
    var shelfHasAnyDistinctTitle: Bool = false

    private let artworkSize: CGFloat = 180

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
        return "Various Artists"
    }

    // Front cover URL
    private var frontCoverUrl: String? {
        recording.bestAlbumArtLarge ?? recording.bestAlbumArtMedium
    }

    var body: some View {
        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingSM) {
            // Album Art
            Group {
                if let frontUrl = frontCoverUrl {
                    CoverArtImage(seed: frontUrl) {
                    AsyncImage(url: URL(string: frontUrl)) { phase in
                        switch phase {
                        case .empty:
                            Rectangle()
                                .fill(ApproachNoteTheme.textSecondary.opacity(0.2))
                                .overlay { ProgressView() }
                        case .success(let image):
                            image
                                .resizable()
                                .aspectRatio(contentMode: .fill)
                        case .failure:
                            NoAlbumArtPlaceholder(cornerRadius: 12)
                        @unknown default:
                            EmptyView()
                        }
                    }
                    }
                } else {
                    NoAlbumArtPlaceholder(cornerRadius: 12)
                }
            }
            .frame(width: artworkSize, height: artworkSize)
            .clipShape(RoundedRectangle(cornerRadius: 12))
            .shadow(color: .black.opacity(0.15), radius: 8, x: 0, y: 4)

            // Recording Info — Year → Artist → Album → (Song Title)
            VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXXS) {
                // Year
                Text(recording.recordingYear.map { String($0) } ?? " ")
                    .font(ApproachNoteTheme.subheadline(weight: .bold))
                    .foregroundColor(ApproachNoteTheme.textPrimary)

                // Artist
                Text(artistName)
                    .font(ApproachNoteTheme.subheadline(weight: .bold))
                    .foregroundColor(ApproachNoteTheme.textPrimary)
                    .lineLimit(1)

                // Album — wraps naturally to 1-2 lines so the title below
                // can pull up when the album fits on one line.
                Text(recording.albumTitle ?? "Unknown Album")
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(ApproachNoteTheme.textPrimary)
                    .lineLimit(2)

                // Recording title — only allocated when some card in the
                // carousel has a distinct title.
                if shelfHasAnyDistinctTitle {
                    Text(recording.displayTitle(comparedTo: parentSongTitle).map { "(\($0))" } ?? " ")
                        .font(ApproachNoteTheme.caption(italic: true))
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                        .lineLimit(1, reservesSpace: true)
                }
            }
            .frame(width: artworkSize, alignment: .leading)
        }
    }
}
