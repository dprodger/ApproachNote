//
//  AlbumArtSourceBadge.swift
//  Approach Note
//
//  Generic info button for album art attribution. Renders a small
//  ⓘ icon (no service name visible) so it can sit alongside artwork
//  without violating partner-branding rules — Spotify in particular
//  forbids the Spotify mark from appearing over album artwork.
//  Tapping the button opens a sheet that shows the source name,
//  usage info, and a link to the source page.
//

import SwiftUI

/// Tiny "ⓘ" button surfacing the source of an album-art image. Designed
/// to be placed near (not on) the artwork — typically alongside the
/// album title or beneath the image.
struct AlbumArtSourceBadge: View {
    let source: String?
    let sourceUrl: String?

    @State private var showingDetails = false

    private var displaySource: String {
        guard let source = source else { return "" }
        switch source.lowercased() {
        case "musicbrainz":
            return "Cover Art Archive"
        case "spotify":
            return "Spotify"
        case "apple":
            return "Apple Music"
        case "wikipedia":
            return "Wikipedia"
        case "amazon":
            return "Amazon"
        default:
            return source
        }
    }

    var body: some View {
        if source != nil {
            Button {
                showingDetails = true
            } label: {
                Image(systemName: "info.circle")
                    .font(.system(size: 13, weight: .regular))
                    .foregroundColor(ApproachNoteTheme.smokeGray)
                    .contentShape(Rectangle())
            }
            .buttonStyle(.plain)
            .accessibilityLabel("Image source: \(displaySource)")
            .help("Image source: \(displaySource)")
            .sheet(isPresented: $showingDetails) {
                AlbumArtSourceSheet(source: displaySource, sourceUrl: sourceUrl)
            }
        }
    }
}

/// Detail sheet showing album art source information
private struct AlbumArtSourceSheet: View {
    let source: String
    let sourceUrl: String?
    @Environment(\.dismiss) var dismiss

    private var licenseInfo: String {
        switch source {
        case "Cover Art Archive":
            return "Images from the Cover Art Archive are typically available under various licenses including CC-BY and CC0. Check the source page for specific licensing."
        case "Spotify":
            return "Album artwork provided by Spotify. For promotional and identification purposes only."
        case "Apple Music":
            return "Album artwork provided by Apple Music. For promotional and identification purposes only."
        default:
            return "For promotional and identification purposes only."
        }
    }

    var body: some View {
        NavigationStack {
            VStack(alignment: .leading, spacing: 20) {
                // Source name
                VStack(alignment: .leading, spacing: 4) {
                    Text("Source")
                        .font(ApproachNoteTheme.caption())
                        .foregroundColor(ApproachNoteTheme.smokeGray)
                    Text(source)
                        .font(ApproachNoteTheme.headline())
                        .foregroundColor(ApproachNoteTheme.charcoal)
                }

                // License info
                VStack(alignment: .leading, spacing: 4) {
                    Text("Usage")
                        .font(ApproachNoteTheme.caption())
                        .foregroundColor(ApproachNoteTheme.smokeGray)
                    Text(licenseInfo)
                        .font(ApproachNoteTheme.subheadline())
                        .foregroundColor(ApproachNoteTheme.charcoal)
                }

                // Source link
                if let urlString = sourceUrl, let url = URL(string: urlString) {
                    Link(destination: url) {
                        HStack {
                            Image(systemName: "arrow.up.right.square")
                            Text("View on \(source)")
                        }
                        .font(ApproachNoteTheme.subheadline())
                        .foregroundColor(ApproachNoteTheme.brass)
                    }
                    .padding(.top, 8)
                }

                Spacer()
            }
            .padding()
            .frame(minWidth: 300)
            .navigationTitle("Cover Art Attribution")
            #if os(iOS)
            .navigationBarTitleDisplayMode(.inline)
            #endif
            .toolbar {
                ToolbarItem(placement: .confirmationAction) {
                    Button("Done") { dismiss() }
                        .foregroundColor(ApproachNoteTheme.amber)
                }
            }
        }
        .presentationDetents([.medium])
    }
}

// MARK: - Preview

#Preview("Source button beside artwork") {
    VStack(alignment: .leading, spacing: 16) {
        AsyncImage(url: URL(string: "https://picsum.photos/300")) { image in
            image.resizable().aspectRatio(contentMode: .fill)
        } placeholder: {
            Color.gray.opacity(0.3)
        }
        .frame(width: 200, height: 200)
        .clipShape(RoundedRectangle(cornerRadius: 8))

        HStack(spacing: 6) {
            Text("Album Title")
                .font(.headline)
            AlbumArtSourceBadge(
                source: "Spotify",
                sourceUrl: "https://open.spotify.com/album/example"
            )
        }
    }
    .padding()
}
