//
//  ArtistImageCarousel.swift
//  Approach Note
//
//  Full-bleed, edge-to-edge artist image hero. Each image fills the
//  screen width; multiple images page horizontally with a swipe. Tapping
//  an image opens a detail sheet that surfaces the source attribution;
//  nothing is overlaid on the image itself, to comply with partner-
//  branding rules (e.g. Spotify forbids its mark from appearing on art).
//

import SwiftUI

struct ArtistImageCarousel: View {
    let images: [ArtistImage]
    @State private var selectedImage: ArtistImage?

    private let carouselHeight: CGFloat = 320

    var body: some View {
        if images.isEmpty {
            EmptyView()
        } else {
            ScrollView(.horizontal, showsIndicators: false) {
                LazyHStack(spacing: 0) {
                    ForEach(images) { image in
                        ImageThumbnail(image: image, height: carouselHeight)
                            // Each image fills the full scroll-view width so a
                            // single image spans the screen and multiple images
                            // page one-per-swipe.
                            .containerRelativeFrame(.horizontal)
                            .onTapGesture { selectedImage = image }
                    }
                }
                .scrollTargetLayout()
            }
            .scrollTargetBehavior(.paging)
            .frame(height: carouselHeight)
            .overlay(alignment: .bottom) {
                if images.count > 1 {
                    PageDots(count: images.count, current: currentIndex)
                        .padding(.bottom, 10)
                }
            }
            .scrollPosition(id: $scrolledImageID)
            .sheet(item: $selectedImage) { image in
                ImageDetailSheet(image: image)
            }
        }
    }

    // Tracks which page is centered so the dots indicator can highlight it.
    @State private var scrolledImageID: String?

    private var currentIndex: Int {
        guard let id = scrolledImageID,
              let idx = images.firstIndex(where: { $0.id == id }) else { return 0 }
        return idx
    }
}

// MARK: - Page Dots

private struct PageDots: View {
    let count: Int
    let current: Int

    var body: some View {
        HStack(spacing: 6) {
            ForEach(0..<count, id: \.self) { index in
                Circle()
                    .fill(Color.white.opacity(index == current ? 0.95 : 0.45))
                    .frame(width: 7, height: 7)
            }
        }
        .padding(.horizontal, 10)
        .padding(.vertical, 6)
        .background(Capsule().fill(Color.black.opacity(0.35)))
    }
}

// MARK: - Image Thumbnail

private struct ImageThumbnail: View {
    let image: ArtistImage
    let height: CGFloat
    @State private var uiImage: UIImage?
    @State private var isLoading = true

    var body: some View {
        // Image only — source attribution lives in ImageDetailSheet (tap to open)
        // so no partner-branded watermark sits on top of the artwork. Fills the
        // full width edge-to-edge; portrait shots crop to the hero height.
        Group {
            if let uiImage = uiImage {
                Image(uiImage: uiImage)
                    .resizable()
                    .aspectRatio(contentMode: .fill)
            } else if isLoading {
                Rectangle()
                    .fill(Color.gray.opacity(0.2))
                    .overlay(ProgressView().tint(ApproachNoteTheme.accent))
            } else {
                Rectangle()
                    .fill(Color.gray.opacity(0.3))
                    .overlay(
                        Image(systemName: "photo")
                            .font(ApproachNoteTheme.largeTitle())
                            .foregroundColor(.gray)
                    )
            }
        }
        .frame(height: height)
        .frame(maxWidth: .infinity)
        .clipped()
        .onAppear { loadImage() }
    }

    private func loadImage() {
        let imageUrl = image.thumbnailUrl ?? image.url
        guard let url = URL(string: imageUrl) else {
            isLoading = false
            return
        }

        URLSession.shared.dataTask(with: url) { data, _, _ in
            DispatchQueue.main.async {
                isLoading = false
                if let data = data, let loadedImage = UIImage(data: data) {
                    self.uiImage = loadedImage
                }
            }
        }.resume()
    }
}

// MARK: - Image Detail Sheet

private struct ImageDetailSheet: View {
    let image: ArtistImage
    @Environment(\.dismiss) var dismiss
    @State private var uiImage: UIImage?
    
    private var sourceName: String {
        switch image.source.lowercased() {
        case "wikimedia": return "Wikimedia Commons"
        case "musicbrainz": return "MusicBrainz"
        case "lastfm": return "Last.fm"
        case "spotify": return "Spotify"
        default: return image.source.capitalized
        }
    }
    
    var body: some View {
        NavigationView {
            ScrollView {
                VStack(spacing: ApproachNoteTheme.spacingLG) {
                    // Full image
                    if let uiImage = uiImage {
                        Image(uiImage: uiImage)
                            .resizable()
                            .aspectRatio(contentMode: .fit)
                            .cornerRadius(8)
                    } else {
                        ProgressView()
                            .frame(height: 300)
                            .tint(ApproachNoteTheme.accent)
                    }
                    
                    // Image info
                    VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingMD) {
                        InfoRow(title: "Source", value: sourceName)
                        
                        if let license = image.licenseType {
                            InfoRow(title: "License", value: licenseName(license))
                        }
                        
                        if let attribution = image.attribution {
                            VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXXS) {
                                Text("Attribution")
                                    .font(ApproachNoteTheme.caption())
                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                                Text(cleanHTML(attribution))
                                    .font(ApproachNoteTheme.subheadline())
                                    .foregroundColor(ApproachNoteTheme.textPrimary)
                            }
                        }
                        
                        if let width = image.width, let height = image.height {
                            InfoRow(title: "Dimensions", value: "\(width) × \(height) pixels")
                        }
                        
                        if let sourcePageUrl = image.sourcePageUrl,
                           let url = URL(string: sourcePageUrl) {
                            Link(destination: url) {
                                HStack {
                                    Text("View on \(sourceName)")
                                        .font(ApproachNoteTheme.subheadline())
                                    Image(systemName: "arrow.up.forward.square")
                                        .font(ApproachNoteTheme.caption())
                                }
                                .foregroundColor(ApproachNoteTheme.textSecondary)
                            }
                        }
                    }
                    .padding()
                    .background(Color(.systemGray6))
                    .cornerRadius(12)
                    .padding(.horizontal)
                }
                .padding(.vertical)
            }
            .navigationTitle("Image Details")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .navigationBarTrailing) {
                    Button("Done") {
                        dismiss()
                    }
                    .foregroundColor(ApproachNoteTheme.textSecondary)
                }
            }
        }
        .onAppear { loadFullImage() }
    }
    
    private func licenseName(_ license: String) -> String {
        switch license.lowercased() {
        case "cc-by-sa": return "Creative Commons Attribution-ShareAlike"
        case "cc-by": return "Creative Commons Attribution"
        case "cc0": return "CC0 (Public Domain)"
        case "public-domain", "pd": return "Public Domain"
        case "fair-use": return "Fair Use"
        default: return license
        }
    }
    
    private func cleanHTML(_ html: String) -> String {
        html.replacingOccurrences(of: "<[^>]+>", with: "", options: .regularExpression)
            .trimmingCharacters(in: .whitespacesAndNewlines)
    }
    
    private func loadFullImage() {
        guard let url = URL(string: image.url) else { return }
        
        URLSession.shared.dataTask(with: url) { data, _, _ in
            DispatchQueue.main.async {
                if let data = data, let loadedImage = UIImage(data: data) {
                    self.uiImage = loadedImage
                }
            }
        }.resume()
    }
}

// MARK: - Helper View

private struct InfoRow: View {
    let title: String
    let value: String

    var body: some View {
        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXXS) {
            Text(title)
                .font(ApproachNoteTheme.caption())
                .foregroundColor(ApproachNoteTheme.textSecondary)
            Text(value)
                .font(ApproachNoteTheme.subheadline())
                .foregroundColor(ApproachNoteTheme.textPrimary)
        }
    }
}

// MARK: - Previews

#Preview("Image Carousel") {
    ArtistImageCarousel(images: [
        ArtistImage(
            id: "1",
            url: "https://picsum.photos/id/453/440/599",
            source: "wikimedia",
            sourceIdentifier: "Miles_Davis_by_Palumbo.jpg",
            licenseType: "cc-by-sa",
            licenseUrl: "https://creativecommons.org/licenses/by-sa/2.0",
            attribution: "Tom Palumbo, CC BY-SA 2.0",
            width: 440,
            height: 599,
            thumbnailUrl: "https://picsum.photos/id/453/220/300",
            sourcePageUrl: "https://commons.wikimedia.org/wiki/File:Miles_Davis_by_Palumbo.jpg"
        ),
        ArtistImage(
            id: "2",
            url: "https://picsum.photos/id/454/440/594",
            source: "wikimedia",
            sourceIdentifier: "John_Coltrane_1963.jpg",
            licenseType: "public-domain",
            licenseUrl: nil,
            attribution: "Hugo van Gelderen / Anefo, Public Domain",
            width: 440,
            height: 594,
            thumbnailUrl: "https://picsum.photos/id/454/220/297",
            sourcePageUrl: "https://commons.wikimedia.org/wiki/File:John_Coltrane_1963.jpg"
        )
    ])
}

#Preview("Empty Carousel") {
    ArtistImageCarousel(images: [])
}
