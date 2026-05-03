//
//  StreamingButtons.swift
//  Approach Note
//
//  Inline Spotify / Apple Music / YouTube link buttons for a recording (macOS)
//

import SwiftUI

// MARK: - Streaming Buttons

struct StreamingButtons: View {
    let recording: Recording

    /// Get Spotify URL from streamingLinks or legacy field
    private var spotifyUrl: String? {
        if let link = recording.streamingLinks?["spotify"], let url = link.bestPlaybackUrl {
            return url
        }
        return recording.bestSpotifyUrl
    }

    private var appleMusicUrl: String? {
        recording.streamingLinks?["apple_music"]?.bestPlaybackUrl
    }

    private var youtubeUrl: String? {
        recording.streamingLinks?["youtube"]?.bestPlaybackUrl
    }

    var body: some View {
        HStack(spacing: 8) {
            if let urlString = spotifyUrl, let url = URL(string: urlString) {
                Link(destination: url) {
                    StreamingIcon(service: .spotify, size: 22)
                }
                .buttonStyle(.plain)
                .help("Open in Spotify")
            }

            if let urlString = appleMusicUrl, let url = URL(string: urlString) {
                Link(destination: url) {
                    StreamingIcon(service: .appleMusic, size: 22)
                }
                .buttonStyle(.plain)
                .help("Open in Apple Music")
            }

            if let urlString = youtubeUrl, let url = URL(string: urlString) {
                Link(destination: url) {
                    StreamingIcon(service: .youtube, size: 22)
                }
                .buttonStyle(.plain)
                .help("Open in YouTube")
            }
        }
    }
}
