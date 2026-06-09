//
//  ArtistImageCarousel.swift
//  Approach Note
//
//  Edge-to-edge artist image hero. Images keep their aspect ratio (no
//  top/bottom cropping) and may bleed past the screen's left/right gutters,
//  but the hero height is capped to the window so tall portraits stay
//  on-screen. Multiple images page horizontally with a swipe. Tapping an
//  image opens a detail sheet that surfaces the source attribution; nothing
//  is overlaid on the image itself, to comply with partner-branding rules
//  (e.g. Spotify forbids its mark from appearing on art).
//

import SwiftUI

struct ArtistImageCarousel: View {
    let images: [ArtistImage]
    /// Full width available to the hero (the screen/window width).
    var availableWidth: CGFloat = 0
    /// Upper bound for the hero height — keeps tall portraits within the window.
    var maxHeight: CGFloat = 0
    /// Reports the image currently centered in the carousel so the host can
    /// render a matching license/attribution line beneath it. Defaults to a
    /// no-op binding for call sites (and previews) that don't need it.
    @Binding var currentImage: ArtistImage?

    init(
        images: [ArtistImage],
        availableWidth: CGFloat = 0,
        maxHeight: CGFloat = 0,
        currentImage: Binding<ArtistImage?> = .constant(nil)
    ) {
        self.images = images
        self.availableWidth = availableWidth
        self.maxHeight = maxHeight
        self._currentImage = currentImage
    }

    @State private var selectedImage: ArtistImage?

    // Fallbacks for before the host view has measured its viewport.
    private static let fallbackWidth: CGFloat = 393
    private static let fallbackMaxHeight: CGFloat = 600
    private static let defaultAspect: CGFloat = 3.0 / 4.0  // portrait

    /// One shared height for every page (stable paging). Sized so the tallest
    /// image renders full-width at its natural aspect ratio, then clamped to
    /// the window height. Images shorter/wider than this letterbox within it;
    /// none are cropped.
    private var carouselHeight: CGFloat {
        let width = availableWidth > 0 ? availableWidth : Self.fallbackWidth
        let cap = maxHeight > 0 ? maxHeight : Self.fallbackMaxHeight
        let tallest = images.map { width / aspect(of: $0) }.max() ?? width
        return min(tallest, cap)
    }

    /// Width-over-height aspect ratio from image metadata, defaulting to portrait.
    private func aspect(of image: ArtistImage) -> CGFloat {
        guard let w = image.width, let h = image.height, w > 0, h > 0 else {
            return Self.defaultAspect
        }
        return CGFloat(w) / CGFloat(h)
    }

    var body: some View {
        if images.isEmpty {
            EmptyView()
        } else {
            let carouselHeight = carouselHeight
            ScrollView(.horizontal, showsIndicators: false) {
                LazyHStack(spacing: 0) {
                    ForEach(images) { image in
                        ImageThumbnail(image: image, height: carouselHeight)
                            // Each page spans the full scroll-view width so a
                            // single image fills the screen and multiple images
                            // page one-per-swipe. The image itself fits (keeps
                            // aspect ratio) within that page, centered.
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
                        .padding(.bottom, ApproachNoteTheme.spacingXS)
                }
            }
            .scrollPosition(id: $scrolledImageID)
            .sheet(item: $selectedImage) { image in
                ImageDetailSheet(image: image)
            }
            .onAppear {
                if currentImage == nil { currentImage = images.first }
            }
            .onChange(of: scrolledImageID) { _, id in
                currentImage = images.first(where: { $0.id == id }) ?? images.first
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
        HStack(spacing: ApproachNoteTheme.spacingXS) {
            ForEach(0..<count, id: \.self) { index in
                Circle()
                    .fill(Color.white.opacity(index == current ? 0.95 : 0.45))
                    .frame(width: 7, height: 7)
            }
        }
        .padding(.horizontal, ApproachNoteTheme.spacingXS)
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
        // so no partner-branded watermark sits on top of the artwork. Keeps the
        // image's aspect ratio (no top/bottom cropping) and pins it to the top
        // of the hero, so a shorter image aligns with adjacent text (e.g. the
        // artist name) rather than floating in a frame sized to taller pages.
        Group {
            if let uiImage = uiImage {
                Image(uiImage: uiImage)
                    .resizable()
                    .aspectRatio(contentMode: .fit)
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
        .frame(height: height, alignment: .top)
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
                            let cleaned = cleanHTML(attribution)
                            if !cleaned.isEmpty {
                                InfoRow(title: "Attribution", value: cleaned)
                            }
                        }

                        if let width = image.width, let height = image.height {
                            InfoRow(title: "Dimensions", value: "\(width) × \(height) pixels")
                        }

                        if let sourcePageUrl = image.sourcePageUrl,
                           let url = URL(string: sourcePageUrl) {
                            Divider()
                                .padding(.vertical, ApproachNoteTheme.spacingXXS)
                            Link(destination: url) {
                                HStack(spacing: ApproachNoteTheme.spacingXS) {
                                    Text("View on \(sourceName)")
                                        .font(ApproachNoteTheme.subheadline())
                                    Image(systemName: "arrow.up.forward.square")
                                        .font(ApproachNoteTheme.caption())
                                }
                                .foregroundColor(ApproachNoteTheme.accent)
                            }
                        }
                    }
                    .frame(maxWidth: .infinity, alignment: .leading)
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
                    .fontWeight(.semibold)
                    .foregroundColor(.white)
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
        default: return license.capitalized
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
            Text(title.uppercased())
                .font(ApproachNoteTheme.caption2())
                .tracking(0.5)
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
