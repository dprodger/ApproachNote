//
//  MacAddStreamingLinkSheet.swift
//  Approach Note
//
//  Sheet for adding manual Spotify/Apple Music/YouTube streaming links
//

import SwiftUI

struct MacAddStreamingLinkSheet: View {
    let recordingId: String
    let releaseId: String
    let releaseTitle: String
    let onSuccess: () -> Void

    @Environment(\.dismiss) var dismiss
    @EnvironmentObject var authManager: AuthenticationManager

    @State private var urlInput: String = ""
    @State private var isSaving = false
    @State private var errorMessage: String?
    @State private var successMessage: String?

    private let contentService = ContentService()

    var body: some View {
        VStack(spacing: 0) {
            // Header
            headerView

            Divider()

            // Content
            ScrollView {
                VStack(alignment: .leading, spacing: 20) {
                    // Instructions
                    instructionsView

                    // URL Input
                    urlInputView

                    // Error/Success messages
                    if let error = errorMessage {
                        errorView(error)
                    }

                    if let success = successMessage {
                        successView(success)
                    }
                }
                .padding(20)
            }

            Divider()

            // Footer with buttons
            footerView
        }
        .frame(width: 500, height: 400)
        .background(ApproachNoteTheme.background)
    }

    // MARK: - Header

    private var headerView: some View {
        HStack {
            VStack(alignment: .leading, spacing: 4) {
                Text("Add Streaming Link")
                    .font(ApproachNoteTheme.title2())
                    .foregroundColor(ApproachNoteTheme.textPrimary)

                Text(releaseTitle)
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(ApproachNoteTheme.textSecondary)
                    .lineLimit(1)
            }

            Spacer()

            Button {
                dismiss()
            } label: {
                Image(systemName: "xmark.circle.fill")
                    .font(.title2)
                    .foregroundColor(ApproachNoteTheme.textSecondary)
            }
            .buttonStyle(.plain)
        }
        .padding()
    }

    // MARK: - Instructions

    private var instructionsView: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("Paste a streaming service URL")
                .font(ApproachNoteTheme.headline())
                .foregroundColor(ApproachNoteTheme.textPrimary)

            VStack(alignment: .leading, spacing: 8) {
                HStack(spacing: 8) {
                    Image(systemName: "checkmark.circle.fill")
                        .foregroundColor(.green)
                        .font(.caption)
                    Text("Spotify: https://open.spotify.com/track/...")
                        .font(ApproachNoteTheme.caption())
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                }

                HStack(spacing: 8) {
                    Image(systemName: "checkmark.circle.fill")
                        .foregroundColor(.green)
                        .font(.caption)
                    Text("Apple Music: https://music.apple.com/.../song/...")
                        .font(ApproachNoteTheme.caption())
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                }

                HStack(spacing: 8) {
                    Image(systemName: "checkmark.circle.fill")
                        .foregroundColor(.green)
                        .font(.caption)
                    Text("YouTube: https://youtube.com/watch?v=...")
                        .font(ApproachNoteTheme.caption())
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                }

                HStack(spacing: 8) {
                    Image(systemName: "info.circle")
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                        .font(.caption)
                    Text("This link will be preserved during automatic re-matching")
                        .font(ApproachNoteTheme.caption())
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                }
            }
            .padding(12)
            .background(ApproachNoteTheme.surface)
            .cornerRadius(8)
        }
    }

    // MARK: - URL Input

    private var urlInputView: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("Track URL or ID")
                .font(ApproachNoteTheme.subheadline(weight: .medium))
                .foregroundColor(ApproachNoteTheme.textPrimary)

            HStack {
                TextField("Paste URL here...", text: $urlInput)
                    .textFieldStyle(.roundedBorder)
                    .font(ApproachNoteTheme.body())
                    .disabled(isSaving)

                // Paste button
                Button {
                    if let clipboard = NSPasteboard.general.string(forType: .string) {
                        urlInput = clipboard
                    }
                } label: {
                    Image(systemName: "doc.on.clipboard")
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                }
                .buttonStyle(.plain)
                .help("Paste from clipboard")
                .disabled(isSaving)
            }

            // Service indicator based on input
            if !urlInput.isEmpty {
                serviceIndicator
            }
        }
    }

    @ViewBuilder
    private var serviceIndicator: some View {
        let detection = detectService(from: urlInput)
        HStack(spacing: 6) {
            switch detection {
            case .valid(let service):
                Image(systemName: "checkmark.circle.fill")
                    .foregroundColor(.green)
                Text("Detected: \(serviceName(for: service))")
                    .font(ApproachNoteTheme.caption())
                    .foregroundColor(.green)
            case .wrongType(let message):
                Image(systemName: "exclamationmark.triangle.fill")
                    .foregroundColor(.orange)
                Text(message)
                    .font(ApproachNoteTheme.caption())
                    .foregroundColor(.orange)
            case .unknown:
                if urlInput.count > 5 {
                    Image(systemName: "questionmark.circle")
                        .foregroundColor(.orange)
                    Text("Could not detect service - will validate on save")
                        .font(ApproachNoteTheme.caption())
                        .foregroundColor(.orange)
                }
            }
        }
    }

    private func serviceName(for service: String) -> String {
        switch service {
        case "spotify": return "Spotify"
        case "apple_music": return "Apple Music"
        case "youtube": return "YouTube"
        default: return service
        }
    }

    private enum ServiceDetection {
        case valid(String)
        case wrongType(String)  // e.g. "This is a Spotify album URL..."
        case unknown
    }

    /// Simple client-side service detection for UI feedback
    private func detectService(from input: String) -> ServiceDetection {
        let trimmed = input.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmed.contains("spotify.com/track/") || trimmed.hasPrefix("spotify:track:") {
            return .valid("spotify")
        }
        // YouTube URL detection
        if trimmed.contains("youtube.com/watch?v=") || trimmed.contains("youtu.be/") {
            return .valid("youtube")
        }
        // Detect wrong YouTube URL types
        if trimmed.contains("youtube.com/channel/") || trimmed.contains("youtube.com/playlist")
            || trimmed.contains("youtube.com/@") || trimmed.contains("youtube.com/c/") {
            return .wrongType("This is a YouTube channel/playlist URL. Please use a video URL instead.")
        }
        // Detect wrong Spotify URL types
        let spotifyTypes = ["album", "artist", "playlist", "episode", "show"]
        for type in spotifyTypes {
            if trimmed.contains("spotify.com/\(type)/") {
                return .wrongType("This is a Spotify \(type) URL. Please use a track URL instead.")
            }
        }
        if trimmed.contains("music.apple.com") {
            // Check for song URL (valid) vs album-only URL (wrong type)
            if trimmed.contains("/song/") || trimmed.contains("?i=") {
                return .valid("apple_music")
            }
            if trimmed.contains("/album/") {
                return .wrongType("This is an Apple Music album URL. Please use a song URL instead.")
            }
            return .valid("apple_music")
        }
        // Check for raw IDs
        let alphanumericPattern = try? NSRegularExpression(pattern: "^[a-zA-Z0-9]{22}$")
        if alphanumericPattern?.firstMatch(in: trimmed, range: NSRange(trimmed.startIndex..., in: trimmed)) != nil {
            return .valid("spotify")
        }
        let numericPattern = try? NSRegularExpression(pattern: "^\\d{9,12}$")
        if numericPattern?.firstMatch(in: trimmed, range: NSRange(trimmed.startIndex..., in: trimmed)) != nil {
            return .valid("apple_music")
        }
        return .unknown
    }

    // MARK: - Messages

    private func errorView(_ message: String) -> some View {
        HStack(spacing: 8) {
            Image(systemName: "exclamationmark.triangle.fill")
                .foregroundColor(.red)
            Text(message)
                .font(ApproachNoteTheme.subheadline())
                .foregroundColor(.red)
        }
        .padding(12)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(Color.red.opacity(0.1))
        .cornerRadius(8)
    }

    private func successView(_ message: String) -> some View {
        HStack(spacing: 8) {
            Image(systemName: "checkmark.circle.fill")
                .foregroundColor(.green)
            Text(message)
                .font(ApproachNoteTheme.subheadline())
                .foregroundColor(.green)
        }
        .padding(12)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(Color.green.opacity(0.1))
        .cornerRadius(8)
    }

    // MARK: - Footer

    private var footerView: some View {
        HStack {
            Spacer()

            Button("Cancel") {
                dismiss()
            }
            .keyboardShortcut(.cancelAction)

            Button {
                Task {
                    await saveLink()
                }
            } label: {
                if isSaving {
                    ProgressView()
                        .scaleEffect(0.7)
                        .frame(width: 60)
                } else {
                    Text("Save")
                }
            }
            .buttonStyle(.borderedProminent)
            .tint(ApproachNoteTheme.brand)
            .disabled(urlInput.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty || isSaving)
            .keyboardShortcut(.defaultAction)
        }
        .padding()
    }

    // MARK: - Actions

    private func saveLink() async {
        guard let token = authManager.getAccessToken() else {
            errorMessage = "Please sign in to add streaming links"
            return
        }

        let trimmedUrl = urlInput.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmedUrl.isEmpty else {
            errorMessage = "Please enter a URL"
            return
        }

        isSaving = true
        errorMessage = nil
        successMessage = nil

        let response = await contentService.addManualStreamingLink(
            recordingId: recordingId,
            releaseId: releaseId,
            url: trimmedUrl,
            authToken: token
        )

        await MainActor.run {
            isSaving = false

            if let response = response {
                if response.success {
                    let name = serviceName(for: response.service ?? "unknown")
                    successMessage = "\(name) link added successfully!"

                    // Dismiss after a short delay
                    DispatchQueue.main.asyncAfter(deadline: .now() + 1.0) {
                        onSuccess()
                        dismiss()
                    }
                } else {
                    errorMessage = response.error ?? "Failed to add link"
                }
            } else {
                errorMessage = "Network error. Please try again."
            }
        }
    }
}

#Preview {
    MacAddStreamingLinkSheet(
        recordingId: "test-recording",
        releaseId: "test-release",
        releaseTitle: "Kind of Blue",
        onSuccess: {}
    )
    .environmentObject(AuthenticationManager())
}
