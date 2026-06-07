//
//  YouTube.swift
//  Approach Note
//
//  Helpers for working with YouTube video URLs plus a reusable thumbnail
//  view used by backing tracks and solo transcriptions on iOS and Mac.
//

import SwiftUI
import os

// MARK: - URL Helpers

/// Pure helpers for deriving thumbnails and watch URLs from a stored
/// YouTube URL string. Handles the common URL shapes: `watch?v=`,
/// `youtu.be/<id>`, `embed/<id>`, and `shorts/<id>`.
enum YouTube {
    /// Extracts the video ID from a YouTube URL in any of the common forms.
    static func videoID(from urlString: String?) -> String? {
        guard let urlString,
              let components = URLComponents(string: urlString) else { return nil }

        // youtu.be/<id>
        if components.host?.contains("youtu.be") == true {
            let id = components.path.split(separator: "/").first.map(String.init)
            return (id?.isEmpty == false) ? id : nil
        }

        // youtube.com/watch?v=<id>
        if let v = components.queryItems?.first(where: { $0.name == "v" })?.value, !v.isEmpty {
            return v
        }

        // youtube.com/embed/<id>, /shorts/<id>, /v/<id>
        let parts = components.path.split(separator: "/").map(String.init)
        if let idx = parts.firstIndex(where: { $0 == "embed" || $0 == "shorts" || $0 == "v" }),
           idx + 1 < parts.count {
            return parts[idx + 1]
        }

        return nil
    }

    /// High-quality thumbnail URL for the given YouTube video URL.
    ///
    /// `hqdefault.jpg` is always available (unlike `maxresdefault`); it is
    /// served at 480×360 with letterbox bars, which callers crop by filling a
    /// 16:9 frame.
    static func thumbnailURL(from urlString: String?) -> URL? {
        guard let id = videoID(from: urlString) else { return nil }
        return URL(string: "https://img.youtube.com/vi/\(id)/hqdefault.jpg")
    }

    /// A canonical watch URL suitable for opening in the YouTube app or
    /// website. Opening this with `openURL` routes to the YouTube app when
    /// installed (which plays full screen) and otherwise the browser.
    static func watchURL(from urlString: String?) -> URL? {
        if let id = videoID(from: urlString) {
            return URL(string: "https://www.youtube.com/watch?v=\(id)")
        }
        guard let urlString else { return nil }
        return URL(string: urlString)
    }

    // MARK: - Title Lookup

    /// Fetches the human-readable video title via YouTube's public oEmbed
    /// endpoint (no API key required). Results are cached in memory for the
    /// lifetime of the process. Returns `nil` on any failure.
    static func title(for urlString: String?) async -> String? {
        guard let id = videoID(from: urlString) else { return nil }
        return await titleCache.title(for: id)
    }

    fileprivate static let titleCache = TitleCache()
}

// MARK: - Title Cache

/// In-memory cache that resolves YouTube video titles via oEmbed, so each
/// video is fetched at most once per process.
fileprivate actor TitleCache {
    private var cache: [String: String] = [:]

    func title(for videoID: String) async -> String? {
        if let cached = cache[videoID] { return cached }

        var components = URLComponents(string: "https://www.youtube.com/oembed")
        components?.queryItems = [
            URLQueryItem(name: "format", value: "json"),
            URLQueryItem(name: "url", value: "https://www.youtube.com/watch?v=\(videoID)")
        ]
        guard let url = components?.url else { return nil }

        do {
            let (data, response) = try await URLSession.shared.data(from: url)
            guard let http = response as? HTTPURLResponse, http.statusCode == 200 else { return nil }
            let title = try JSONDecoder().decode(OEmbedResponse.self, from: data).title
            cache[videoID] = title
            return title
        } catch {
            Log.network.debug("YouTube oEmbed lookup failed: \(error.localizedDescription, privacy: .public)")
            return nil
        }
    }

    private struct OEmbedResponse: Decodable {
        let title: String
    }
}

// MARK: - Title View

/// Shows the title of a YouTube video below a thumbnail. Prefers a curated
/// `storedTitle` when present; otherwise resolves the real video title via
/// oEmbed, showing `placeholder` until it loads (or if it can't be fetched).
struct YouTubeTitleView: View {
    let youtubeUrl: String?
    let storedTitle: String?
    var placeholder: String = "Video"

    @State private var fetchedTitle: String?

    private var displayTitle: String {
        if let storedTitle, !storedTitle.isEmpty { return storedTitle }
        if let fetchedTitle, !fetchedTitle.isEmpty { return fetchedTitle }
        return placeholder
    }

    var body: some View {
        Text(displayTitle)
            .font(ApproachNoteTheme.headline())
            .foregroundColor(ApproachNoteTheme.textPrimary)
            .lineLimit(2)
            .multilineTextAlignment(.leading)
            .task(id: youtubeUrl) {
                // Only fetch when there is no curated title to show.
                if let storedTitle, !storedTitle.isEmpty { return }
                fetchedTitle = await YouTube.title(for: youtubeUrl)
            }
    }
}

// MARK: - Thumbnail View

/// A 16:9 YouTube thumbnail with a play-button overlay. Falls back to a
/// placeholder when no video ID can be derived. Pass `maxWidth` to cap the
/// thumbnail size (Mac); leave `nil` to fill the available width (iOS).
struct YouTubeThumbnailView: View {
    let youtubeUrl: String?
    var maxWidth: CGFloat? = nil
    var cornerRadius: CGFloat = 8

    var body: some View {
        Group {
            if let url = YouTube.thumbnailURL(from: youtubeUrl) {
                thumbnail(url)
                    .overlay { playButton }
            } else {
                placeholder
            }
        }
        .frame(maxWidth: maxWidth ?? .infinity)
        .aspectRatio(16.0 / 9.0, contentMode: .fit)
        .clipShape(RoundedRectangle(cornerRadius: cornerRadius))
    }

    @ViewBuilder
    private func thumbnail(_ url: URL) -> some View {
        #if os(iOS)
        CachedAsyncImage(url: url) { image in
            image
                .resizable()
                .aspectRatio(contentMode: .fill)
        } placeholder: {
            placeholder
        }
        #else
        AsyncImage(url: url) { phase in
            switch phase {
            case .success(let image):
                image
                    .resizable()
                    .aspectRatio(contentMode: .fill)
            default:
                placeholder
            }
        }
        #endif
    }

    private var playButton: some View {
        Image(systemName: "play.fill")
            .font(.system(size: 22))
            .foregroundColor(.white)
            .padding(16)
            .background(.black.opacity(0.55), in: Circle())
    }

    private var placeholder: some View {
        ZStack {
            Rectangle()
                .fill(ApproachNoteTheme.accent.opacity(0.15))
            Image(systemName: "video.slash")
                .font(.system(size: 28))
                .foregroundColor(ApproachNoteTheme.accent)
        }
    }
}
