//
//  ArtistImageCarousel.swift
//  Approach Note
//
//  Horizontal scrolling carousel of artist images. Tapping an image
//  opens a detail sheet that surfaces the source attribution; nothing
//  is overlaid on the image itself, to comply with partner-branding
//  rules (e.g. Spotify forbids its mark from appearing on artwork).
//

import SwiftUI

struct ArtistImageCarousel: View {
    let images: [ArtistImage]
    @State private var selectedImage: ArtistImage?
    
    private let carouselHeight: CGFloat = 280
    
    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            // Header removed as per requirements
            
            if images.isEmpty {
                Text("No images available")
                    .foregroundColor(ApproachNoteTheme.smokeGray)
                    .padding()
            } else {
                // Carousel with border wrapper
                ScrollView(.horizontal, showsIndicators: false) {
                    HStack(spacing: 12) {
                        ForEach(images) { image in
                            ImageThumbnail(image: image)
                                .onTapGesture {
                                    selectedImage = image
                                }
                        }
                    }
                    .padding(.horizontal)
                    .padding(.vertical, 12)
                }
                .frame(height: carouselHeight + 24) // Account for vertical padding
                .background(
                    RoundedRectangle(cornerRadius: 16)
                        .strokeBorder(ApproachNoteTheme.smokeGray.opacity(0.3), lineWidth: 1.5)
                        .background(
                            RoundedRectangle(cornerRadius: 16)
                                .fill(Color.white.opacity(0.5))
                        )
                )
                .padding(.horizontal)
            }
        }
        .sheet(item: $selectedImage) { image in
            ImageDetailSheet(image: image)
        }
    }
}

// MARK: - Image Thumbnail

private struct ImageThumbnail: View {
    let image: ArtistImage
    @State private var uiImage: UIImage?
    @State private var isLoading = true
    
    private var cardWidth: CGFloat {
        guard let width = image.width, let height = image.height, height > 0 else {
            return 210 // Default width
        }
        let aspectRatio = CGFloat(width) / CGFloat(height)
        return 280 * aspectRatio
    }
    
    var body: some View {
        // Image only — source attribution lives in ImageDetailSheet (tap to open)
        // so no partner-branded watermark sits on top of the artwork.
        Group {
            if let uiImage = uiImage {
                Image(uiImage: uiImage)
                    .resizable()
                    .aspectRatio(contentMode: .fill)
                    .frame(width: cardWidth, height: 280)
                    .clipShape(RoundedRectangle(cornerRadius: 12))
            } else if isLoading {
                RoundedRectangle(cornerRadius: 12)
                    .fill(Color.gray.opacity(0.2))
                    .frame(width: cardWidth, height: 280)
                    .overlay(ProgressView().tint(ApproachNoteTheme.amber))
            } else {
                RoundedRectangle(cornerRadius: 12)
                    .fill(Color.gray.opacity(0.3))
                    .frame(width: cardWidth, height: 280)
                    .overlay(
                        Image(systemName: "photo")
                            .font(ApproachNoteTheme.largeTitle())
                            .foregroundColor(.gray)
                    )
            }
        }
        .frame(width: cardWidth, height: 280)
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
                VStack(spacing: 20) {
                    // Full image
                    if let uiImage = uiImage {
                        Image(uiImage: uiImage)
                            .resizable()
                            .aspectRatio(contentMode: .fit)
                            .cornerRadius(8)
                    } else {
                        ProgressView()
                            .frame(height: 300)
                            .tint(ApproachNoteTheme.amber)
                    }
                    
                    // Image info
                    VStack(alignment: .leading, spacing: 16) {
                        InfoRow(title: "Source", value: sourceName)
                        
                        if let license = image.licenseType {
                            InfoRow(title: "License", value: licenseName(license))
                        }
                        
                        if let attribution = image.attribution {
                            VStack(alignment: .leading, spacing: 4) {
                                Text("Attribution")
                                    .font(ApproachNoteTheme.caption())
                                    .foregroundColor(ApproachNoteTheme.smokeGray)
                                Text(cleanHTML(attribution))
                                    .font(ApproachNoteTheme.subheadline())
                                    .foregroundColor(ApproachNoteTheme.charcoal)
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
                                .foregroundColor(ApproachNoteTheme.brass)
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
                    .foregroundColor(ApproachNoteTheme.brass)
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
        VStack(alignment: .leading, spacing: 4) {
            Text(title)
                .font(ApproachNoteTheme.caption())
                .foregroundColor(ApproachNoteTheme.smokeGray)
            Text(value)
                .font(ApproachNoteTheme.subheadline())
                .foregroundColor(ApproachNoteTheme.charcoal)
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
