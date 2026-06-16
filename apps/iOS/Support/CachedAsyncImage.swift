//
//  CachedAsyncImage.swift
//  Approach Note
//
//  A drop-in replacement for AsyncImage that caches loaded images in memory
//  to prevent re-fetching when scrolling back to previously loaded images.
//

import SwiftUI

// MARK: - Image Cache

/// Shared in-memory cache for loaded images
final class ImageCache {
    static let shared = ImageCache()

    private let cache = NSCache<NSString, UIImage>()

    private init() {
        // Configure cache limits
        cache.countLimit = 200  // Max number of images
        cache.totalCostLimit = 100 * 1024 * 1024  // ~100MB
    }

    func get(forKey key: String) -> UIImage? {
        cache.object(forKey: key as NSString)
    }

    func set(_ image: UIImage, forKey key: String) {
        let cost = image.jpegData(compressionQuality: 1.0)?.count ?? 0
        cache.setObject(image, forKey: key as NSString, cost: cost)
    }
}

// MARK: - Cached Async Image

/// AsyncImage replacement with in-memory caching
struct CachedAsyncImage<Content: View, Placeholder: View>: View {
    let url: URL?
    let content: (Image) -> Content
    let placeholder: () -> Placeholder

    @State private var cachedImage: UIImage?
    @State private var isLoading = false

    // Screenshot mode swaps real (copyrighted) album art for generated covers
    // so App Store screenshots never show third-party covers (Guideline 5.2.1).
    @AppStorage(ScreenshotMode.defaultsKey) private var screenshotEnabled = false

    init(
        url: URL?,
        @ViewBuilder content: @escaping (Image) -> Content,
        @ViewBuilder placeholder: @escaping () -> Placeholder
    ) {
        self.url = url
        self.content = content
        self.placeholder = placeholder
    }

    var body: some View {
        Group {
            if let uiImage = cachedImage {
                content(Image(uiImage: uiImage))
            } else {
                placeholder()
                    .onAppear {
                        loadImage()
                    }
            }
        }
    }

    private func loadImage() {
        guard let url = url else { return }

        let key = url.absoluteString

        // Check cache first
        if let cached = ImageCache.shared.get(forKey: key) {
            cachedImage = cached
            return
        }

        // Screenshot mode: substitute generated cover art instead of fetching
        // the real (copyrighted) image. GenericCoverArt caches internally, so
        // we don't pollute the shared real-URL cache.
        if screenshotEnabled || ScreenshotMode.envEnabled {
            cachedImage = GenericCoverArt.image(seed: key)
            return
        }

        // Prevent duplicate loads
        guard !isLoading else { return }
        isLoading = true

        // Load from network
        Task {
            do {
                let (data, _) = try await URLSession.shared.data(from: url)
                if let uiImage = UIImage(data: data) {
                    // Cache the image
                    ImageCache.shared.set(uiImage, forKey: key)

                    await MainActor.run {
                        cachedImage = uiImage
                        isLoading = false
                    }
                }
            } catch {
                await MainActor.run {
                    isLoading = false
                }
            }
        }
    }
}

// MARK: - Convenience initializer matching AsyncImage style

extension CachedAsyncImage where Content == Image, Placeholder == Color {
    init(url: URL?) {
        self.init(
            url: url,
            content: { $0 },
            placeholder: { Color(.systemGray5) }
        )
    }
}
