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
    @State private var isHovering = false
    @State private var showingBackCover = false

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

    // Back cover URL
    private var backCoverUrl: String? {
        recording.backCoverArtLarge ?? recording.backCoverArtMedium
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            // Album Art with flip support
            ZStack(alignment: .topTrailing) {
                // Album art with card-flip animation
                ZStack {
                    // Front cover
                    Group {
                        if let frontUrl = frontCoverUrl {
                            AsyncImage(url: URL(string: frontUrl)) { phase in
                                switch phase {
                                case .empty:
                                    Rectangle()
                                        .fill(ApproachNoteTheme.smokeGray.opacity(0.2))
                                        .overlay { ProgressView() }
                                case .success(let image):
                                    image
                                        .resizable()
                                        .aspectRatio(contentMode: .fill)
                                case .failure:
                                    Rectangle()
                                        .fill(ApproachNoteTheme.smokeGray.opacity(0.2))
                                        .overlay {
                                            Image(systemName: "music.note")
                                                .font(.system(size: 40))
                                                .foregroundColor(ApproachNoteTheme.smokeGray)
                                        }
                                @unknown default:
                                    EmptyView()
                                }
                            }
                        } else {
                            Rectangle()
                                .fill(ApproachNoteTheme.smokeGray.opacity(0.2))
                                .overlay {
                                    Image(systemName: "music.note")
                                        .font(.system(size: 40))
                                        .foregroundColor(ApproachNoteTheme.smokeGray)
                                }
                        }
                    }
                    .frame(width: artworkSize, height: artworkSize)
                    .clipShape(RoundedRectangle(cornerRadius: 12))
                    .opacity(showingBackCover ? 0 : 1)

                    // Back cover (pre-rotated so it appears correct after flip)
                    if let backUrl = backCoverUrl {
                        AsyncImage(url: URL(string: backUrl)) { phase in
                            switch phase {
                            case .empty:
                                Rectangle()
                                    .fill(ApproachNoteTheme.smokeGray.opacity(0.2))
                                    .overlay { ProgressView() }
                            case .success(let image):
                                image
                                    .resizable()
                                    .aspectRatio(contentMode: .fill)
                            case .failure:
                                Rectangle()
                                    .fill(ApproachNoteTheme.smokeGray.opacity(0.2))
                            @unknown default:
                                EmptyView()
                            }
                        }
                        .frame(width: artworkSize, height: artworkSize)
                        .clipShape(RoundedRectangle(cornerRadius: 12))
                        .rotation3DEffect(.degrees(180), axis: (x: 0, y: 1, z: 0))
                        .opacity(showingBackCover ? 1 : 0)
                    }
                }
                .rotation3DEffect(
                    .degrees(showingBackCover ? 180 : 0),
                    axis: (x: 0, y: 1, z: 0)
                )

                // Flip badge (shown when back cover available)
                if recording.canFlipToBackCover {
                    Button(action: {
                        withAnimation(.easeInOut(duration: 0.4)) {
                            showingBackCover.toggle()
                        }
                    }) {
                        Image(systemName: showingBackCover ? "arrow.uturn.backward" : "arrow.trianglehead.2.clockwise.rotate.90")
                            .foregroundColor(.white)
                            .font(.system(size: 10, weight: .semibold))
                            .padding(6)
                            .background(Color.black.opacity(0.6))
                            .clipShape(Circle())
                    }
                    .buttonStyle(.plain)
                    .padding(6)
                    .help(showingBackCover ? "Show front cover" : "Show back cover")
                }

            }
            .frame(width: artworkSize, height: artworkSize)
            .shadow(color: .black.opacity(0.15), radius: 8, x: 0, y: 4)

            // Recording Info
            VStack(alignment: .leading, spacing: 4) {
                Text(artistName)
                    .font(ApproachNoteTheme.subheadline(weight: .semibold))
                    .foregroundColor(ApproachNoteTheme.brass)
                    .lineLimit(1)

                // Album Title + image-source ⓘ button
                HStack(alignment: .firstTextBaseline, spacing: 4) {
                    Text(recording.albumTitle ?? "Unknown Album")
                        .font(ApproachNoteTheme.body(weight: .medium))
                        .foregroundColor(ApproachNoteTheme.charcoal)
                        .lineLimit(2)
                    if showingBackCover {
                        AlbumArtSourceBadge(
                            source: recording.backCoverSource,
                            sourceUrl: recording.backCoverSourceUrl
                        )
                    } else {
                        AlbumArtSourceBadge(
                            source: recording.displayAlbumArtSource,
                            sourceUrl: recording.displayAlbumArtSourceUrl
                        )
                    }
                }

                // Recording title (when different from song title)
                if let recordingTitle = recording.displayTitle {
                    Text("(\(recordingTitle))")
                        .font(ApproachNoteTheme.caption())
                        .italic()
                        .foregroundColor(ApproachNoteTheme.brass)
                        .lineLimit(1)
                }

                if let year = recording.recordingYear {
                    Text(String(year))
                        .font(ApproachNoteTheme.caption())
                        .foregroundColor(ApproachNoteTheme.smokeGray)
                }
            }
            .frame(width: artworkSize, alignment: .leading)
        }
        .padding(12)
        .background(isHovering ? ApproachNoteTheme.backgroundLight : ApproachNoteTheme.cardBackground)
        .clipShape(RoundedRectangle(cornerRadius: 12))
        .overlay(
            RoundedRectangle(cornerRadius: 12)
                .stroke(isHovering ? ApproachNoteTheme.burgundy.opacity(0.5) : Color.clear, lineWidth: 2)
        )
        .onHover { hovering in
            isHovering = hovering
        }
        .animation(.easeInOut(duration: 0.15), value: isHovering)
    }
}
