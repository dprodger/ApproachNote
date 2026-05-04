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
    var showArtistName: Bool = true
    /// Shell+hydrate viewport hook. SongDetailView passes a closure that
    /// forwards to `SongDetailViewModel.requestHydration(for:)`; other
    /// call sites leave this nil and render fully-loaded recordings.
    var onVisible: ((String) -> Void)? = nil

    @State private var isHovering = false
    @State private var showingBackCover = false

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

    // Back cover URL
    private var backCoverUrl: String? {
        recording.backCoverArtLarge ?? recording.backCoverArtMedium
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            // Album art with flip support and streaming button overlay
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
                                        .fill(ApproachNoteTheme.cardBackground)
                                        .overlay { ProgressView() }
                                case .success(let image):
                                    image
                                        .resizable()
                                        .aspectRatio(contentMode: .fill)
                                case .failure:
                                    Rectangle()
                                        .fill(ApproachNoteTheme.cardBackground)
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
                                .fill(ApproachNoteTheme.cardBackground)
                                .overlay {
                                    Image(systemName: "music.note")
                                        .font(.system(size: 40))
                                        .foregroundColor(ApproachNoteTheme.smokeGray)
                                }
                        }
                    }
                    .frame(width: artworkSize, height: artworkSize)
                    .clipShape(RoundedRectangle(cornerRadius: 10))
                    .opacity(showingBackCover ? 0 : 1)

                    // Back cover (pre-rotated so it appears correct after flip)
                    if let backUrl = backCoverUrl {
                        AsyncImage(url: URL(string: backUrl)) { phase in
                            switch phase {
                            case .empty:
                                Rectangle()
                                    .fill(ApproachNoteTheme.cardBackground)
                                    .overlay { ProgressView() }
                            case .success(let image):
                                image
                                    .resizable()
                                    .aspectRatio(contentMode: .fill)
                            case .failure:
                                Rectangle()
                                    .fill(ApproachNoteTheme.cardBackground)
                            @unknown default:
                                EmptyView()
                            }
                        }
                        .frame(width: artworkSize, height: artworkSize)
                        .clipShape(RoundedRectangle(cornerRadius: 10))
                        .rotation3DEffect(.degrees(180), axis: (x: 0, y: 1, z: 0))
                        .opacity(showingBackCover ? 1 : 0)
                    }
                }
                .rotation3DEffect(
                    .degrees(showingBackCover ? 180 : 0),
                    axis: (x: 0, y: 1, z: 0)
                )
                .shadow(color: .black.opacity(0.15), radius: 6, x: 0, y: 3)

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

            // Recording info below artwork
            VStack(alignment: .leading, spacing: 4) {
                // Artist name
                if showArtistName {
                    Text(artistName)
                        .font(ApproachNoteTheme.subheadline(weight: .semibold))
                        .foregroundColor(ApproachNoteTheme.brass)
                        .lineLimit(1)
                }

                // Album title with optional canonical star + image-source ⓘ button
                HStack(spacing: 4) {
                    if recording.isCanonical == true {
                        Image(systemName: "star.fill")
                            .foregroundColor(ApproachNoteTheme.gold)
                            .font(ApproachNoteTheme.caption())
                    }

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

                // Year
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
        .onAppear {
            // Shell+hydrate viewport hook — tells the parent ViewModel
            // this recording is now in the viewport, so it can queue a
            // batch hydration request for it.
            onVisible?(recording.id)
        }
    }
}
