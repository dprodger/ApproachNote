//
//  MacShareViewController.swift
//  ShareImporterMac
//
//  Safari Share Extension for importing artist, song, and YouTube data into Approach Note (macOS).
//

import Cocoa
import SwiftUI
import UniformTypeIdentifiers

// MARK: - Mac Share View Controller

class MacShareViewController: NSViewController {

    // MARK: - Properties
    private var artistData: ArtistData?
    private var existingArtist: ExistingArtist?
    private var songData: SongData?
    private var existingSong: ExistingSong?
    private var youtubeData: YouTubeData?
    private var isSongImport: Bool = false
    private var isYouTubeImport: Bool = false
    private let appGroupIdentifier = "group.com.approachnote.shared"

    // MARK: - Lifecycle
    override func loadView() {
        // Create a view with reasonable default size for extension popover
        self.view = NSView(frame: NSRect(x: 0, y: 0, width: 400, height: 450))
    }

    override func viewDidLoad() {
        super.viewDidLoad()

        // Bail out early if the user isn't signed in to the main app — avoids
        // dragging them through the whole import flow only to 401 at the end.
        guard isUserAuthenticated() else {
            NSLog("🔒 User not authenticated — showing login-required view")
            showLoginRequired()
            return
        }

        // First, detect if this is an artist or song page
        detectPageType()
    }

    private func isUserAuthenticated() -> Bool {
        UserDefaults(suiteName: appGroupIdentifier)?.bool(forKey: "isAuthenticated") ?? false
    }

    private func showLoginRequired() {
        let loginView = MacLoginRequiredView(
            onDismiss: { [weak self] in
                self?.cancelImport()
            }
        )
        replaceCurrentView(with: loginView)
    }

    // MARK: - Data Extraction

    private func detectPageType() {
        NSLog("🔍 Starting data extraction...")
        extractData()
    }

    private func extractData() {
        NSLog("🔍 Extracting page data...")

        // Get the extension context
        guard let extensionItem = extensionContext?.inputItems.first as? NSExtensionItem else {
            NSLog("❌ No extension item found")
            showError("No data available")
            return
        }

        guard let attachments = extensionItem.attachments, !attachments.isEmpty else {
            NSLog("❌ No attachments found")
            showError("No data available")
            return
        }

        NSLog("✓ Extension item found with %d attachment(s)", attachments.count)

        // Log type identifiers for debugging
        for (index, provider) in attachments.enumerated() {
            NSLog("📋 Attachment %d type identifiers: %{public}@", index, provider.registeredTypeIdentifiers.joined(separator: ", "))
        }

        // Try to find the best attachment to use
        // Priority: property-list (Safari JS), then URL, then plain-text
        let propertyListType = "com.apple.property-list"

        // First, look for property list in any attachment (Safari JavaScript preprocessing)
        if let itemProvider = attachments.first(where: { $0.hasItemConformingToTypeIdentifier(propertyListType) }) {
            NSLog("✓ Found attachment with property list type")
            loadPropertyList(from: itemProvider)
            return
        }

        // DEBUG: Show what types we got if no property list found
        NSLog("⚠️ No property list type found in any attachment")

        // Check for plain-text first (contains both title and URL from YouTube app)
        // Then fall back to URL only
        let plainTextProvider = attachments.first { $0.hasItemConformingToTypeIdentifier("public.plain-text") }
        let urlProvider = attachments.first { $0.hasItemConformingToTypeIdentifier("public.url") }

        // Try to extract title from extension item metadata
        let metadataTitle = self.extractTitleFromExtensionItem(extensionItem)
        if let title = metadataTitle {
            NSLog("📝 Found title from extension item metadata: %{public}@", title)
        }

        // Prefer plain-text if available (contains title + URL)
        if let plainTextProvider = plainTextProvider {
            NSLog("✓ Plain text type found, loading text (preferred for title extraction)...")

            plainTextProvider.loadItem(forTypeIdentifier: "public.plain-text", options: nil) { [weak self] (item, error) in
                guard let self = self else { return }

                if let error = error {
                    NSLog("❌ Error loading text: \(error.localizedDescription)")
                    // Fall back to URL if plain-text fails
                    if let urlProvider = urlProvider {
                        self.loadURLProvider(urlProvider, title: metadataTitle)
                    } else {
                        DispatchQueue.main.async {
                            self.showError("Failed to load shared content")
                        }
                    }
                    return
                }

                if let text = item as? String {
                    NSLog("✓ Got text: %{public}@", text)
                    // Try to extract URL and title from the text
                    if let (url, title) = self.extractURLAndTitle(from: text) {
                        let finalTitle = title ?? metadataTitle
                        NSLog("✅ Extracted URL: %{public}@, title: %{public}@", url.absoluteString, finalTitle ?? "(none)")
                        DispatchQueue.main.async {
                            self.processSharedURL(url, title: finalTitle)
                        }
                    } else {
                        NSLog("❌ No valid URL found in text, trying URL provider")
                        if let urlProvider = urlProvider {
                            self.loadURLProvider(urlProvider, title: metadataTitle)
                        } else {
                            DispatchQueue.main.async {
                                self.showError("No valid URL found in shared content")
                            }
                        }
                    }
                } else {
                    NSLog("❌ Item is not String: \(type(of: item))")
                    if let urlProvider = urlProvider {
                        self.loadURLProvider(urlProvider, title: metadataTitle)
                    } else {
                        DispatchQueue.main.async {
                            self.showError("Invalid content format")
                        }
                    }
                }
            }
        } else if let urlProvider = urlProvider {
            // No plain-text, fall back to URL only
            loadURLProvider(urlProvider, title: metadataTitle)
        } else {
            NSLog("❌ No supported type found. Available: \(attachments[0].registeredTypeIdentifiers)")
            showError("This extension only works with web pages and URLs")
        }
    }

    private func loadURLProvider(_ urlProvider: NSItemProvider, title: String?) {
        NSLog("✓ URL type found, loading URL...")

        urlProvider.loadItem(forTypeIdentifier: "public.url", options: nil) { [weak self] (item, error) in
            guard let self = self else { return }

            if let error = error {
                NSLog("❌ Error loading URL: \(error.localizedDescription)")
                DispatchQueue.main.async {
                    self.showError("Failed to load URL")
                }
                return
            }

            if let url = item as? URL {
                NSLog("✓ Got URL: %{public}@", url.absoluteString)
                DispatchQueue.main.async {
                    self.processSharedURL(url, title: title)
                }
            } else if let urlString = item as? String, let url = URL(string: urlString) {
                NSLog("✓ Got URL string: %{public}@", urlString)
                DispatchQueue.main.async {
                    self.processSharedURL(url, title: title)
                }
            } else {
                NSLog("❌ Item is not URL: \(type(of: item))")
                DispatchQueue.main.async {
                    self.showError("Invalid URL format")
                }
            }
        }
    }

    private func extractTitleFromExtensionItem(_ extensionItem: NSExtensionItem) -> String? {
        // Log available metadata for debugging
        NSLog("📋 Extension item metadata:")
        if let attributedContentText = extensionItem.attributedContentText {
            NSLog("   attributedContentText: %{public}@", attributedContentText.string)
        } else {
            NSLog("   attributedContentText: nil")
        }
        if let attributedTitle = extensionItem.attributedTitle {
            NSLog("   attributedTitle: %{public}@", attributedTitle.string)
        } else {
            NSLog("   attributedTitle: nil")
        }
        if let userInfo = extensionItem.userInfo {
            NSLog("   userInfo keys: %{public}@", "\(userInfo.keys)")
        }

        // Also check item provider suggested names
        if let attachments = extensionItem.attachments {
            for (index, provider) in attachments.enumerated() {
                if let suggestedName = provider.suggestedName {
                    NSLog("   attachment %d suggestedName: %{public}@", index, suggestedName)
                }
            }
        }

        // Try attributedTitle first (most reliable for title)
        if let attributedTitle = extensionItem.attributedTitle {
            let title = attributedTitle.string.trimmingCharacters(in: .whitespacesAndNewlines)
            if !title.isEmpty {
                return title
            }
        }

        // Try attributedContentText
        if let attributedContentText = extensionItem.attributedContentText {
            let text = attributedContentText.string.trimmingCharacters(in: .whitespacesAndNewlines)
            // The content text might contain the title followed by the URL
            // Extract just the title part (before any URL)
            if let (_, title) = extractURLAndTitle(from: text), let extractedTitle = title {
                return extractedTitle
            }
            // If no URL found, use the whole text if it's not too long
            if !text.isEmpty && text.count < 200 && !text.hasPrefix("http") {
                return text
            }
        }

        // Try item provider suggested names as last resort
        if let attachments = extensionItem.attachments {
            for provider in attachments {
                if let suggestedName = provider.suggestedName,
                   !suggestedName.isEmpty,
                   !suggestedName.hasPrefix("http") {
                    return suggestedName
                }
            }
        }

        return nil
    }

    private func loadPropertyList(from itemProvider: NSItemProvider) {
        let propertyListType = "com.apple.property-list"
        NSLog("✓ Loading property list...")

        itemProvider.loadItem(forTypeIdentifier: propertyListType, options: nil) { [weak self] (item, error) in
            guard let self = self else { return }

            if let error = error {
                NSLog("❌ Error loading item: \(error.localizedDescription)")
                DispatchQueue.main.async {
                    self.showError("Failed to load page data")
                }
                return
            }

            NSLog("✓ Item loaded successfully, type: \(type(of: item))")

            // The item should be a Dictionary containing the JavaScript preprocessing results
            if let dictionary = item as? [String: Any] {
                NSLog("✓ Got dictionary from item")

                // Check for the JavaScript preprocessing results key
                if let results = dictionary[NSExtensionJavaScriptPreprocessingResultsKey] as? [String: Any] {
                    NSLog("✓ Got JavaScript preprocessing results")

                    DispatchQueue.main.async {
                        self.processExtractedData(results)
                    }
                } else {
                    NSLog("❌ No JavaScript preprocessing results found in dictionary")
                    NSLog("   Dictionary keys: \(dictionary.keys)")
                    DispatchQueue.main.async {
                        self.showError("Could not extract data from page")
                    }
                }
            } else if let data = item as? Data {
                NSLog("✓ Got Data (\(data.count) bytes), attempting to deserialize...")
                do {
                    if let dict = try PropertyListSerialization.propertyList(from: data, options: [], format: nil) as? [String: Any] {
                        NSLog("✓ Successfully deserialized property list")
                        if let results = dict[NSExtensionJavaScriptPreprocessingResultsKey] as? [String: Any] {
                            NSLog("✓ Got JavaScript preprocessing results from deserialized data")
                            DispatchQueue.main.async {
                                self.processExtractedData(results)
                            }
                            return
                        } else {
                            NSLog("❌ No JS results key in deserialized dict. Keys: \(dict.keys)")
                        }
                    }
                } catch {
                    NSLog("❌ Failed to deserialize property list: \(error)")
                }
                DispatchQueue.main.async {
                    self.showError("Invalid data format")
                }
            } else {
                NSLog("❌ Item is not Dictionary or Data: \(type(of: item))")
                DispatchQueue.main.async {
                    self.showError("Invalid data format")
                }
            }
        }
    }

    private func extractURLAndTitle(from text: String) -> (URL, String?)? {
        // Try to find a YouTube URL in the text
        let patterns = [
            "https?://(?:www\\.)?youtube\\.com/watch\\?[^\\s]+",
            "https?://(?:www\\.)?youtube\\.com/shorts/[^\\s]+",
            "https?://youtu\\.be/[^\\s]+",
            "https?://(?:www\\.)?youtube\\.com/[^\\s]+"
        ]

        for pattern in patterns {
            if let regex = try? NSRegularExpression(pattern: pattern, options: .caseInsensitive) {
                let range = NSRange(text.startIndex..., in: text)
                if let match = regex.firstMatch(in: text, options: [], range: range) {
                    if let urlRange = Range(match.range, in: text) {
                        let urlString = String(text[urlRange])
                        if let url = URL(string: urlString) {
                            // Extract title from text before the URL
                            let beforeURL = String(text[..<urlRange.lowerBound])
                            let title = extractTitle(from: beforeURL)
                            return (url, title)
                        }
                    }
                }
            }
        }

        // Fallback: try the whole text as a URL (no title in this case)
        if let url = URL(string: text.trimmingCharacters(in: .whitespacesAndNewlines)) {
            return (url, nil)
        }
        return nil
    }

    private func extractTitle(from text: String) -> String? {
        // Clean up the text that appears before the URL
        var title = text
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .replacingOccurrences(of: "\n", with: " ")
            .trimmingCharacters(in: .whitespacesAndNewlines)

        // Remove common prefixes like "Check out this video:"
        let prefixesToRemove = [
            "Check out this video:",
            "Check out:",
            "Watch:",
            "Video:"
        ]
        for prefix in prefixesToRemove {
            if title.lowercased().hasPrefix(prefix.lowercased()) {
                title = String(title.dropFirst(prefix.count)).trimmingCharacters(in: .whitespacesAndNewlines)
            }
        }

        // Return nil if empty or too short
        guard title.count >= 2 else { return nil }

        NSLog("📝 Extracted title from shared text: %{public}@", title)
        return title
    }

    // MARK: - URL Processing

    private func processSharedURL(_ url: URL, title: String? = nil) {
        NSLog("🔗 Processing shared URL: %{public}@", url.absoluteString)
        if let title = title {
            NSLog("📝 With title: %{public}@", title)
        }

        // Check if this is a YouTube URL
        if isYouTubeURL(url) {
            processYouTubeURL(url, title: title)
        } else if isMusicBrainzURL(url) {
            // For MusicBrainz URLs, we can't extract data without JavaScript
            // Show an error asking to use Safari
            showError("Please share MusicBrainz pages from Safari to extract full data")
        } else {
            showError("This URL is not supported. Share from YouTube or MusicBrainz.")
        }
    }

    private func isYouTubeURL(_ url: URL) -> Bool {
        let host = url.host?.lowercased() ?? ""
        return host.contains("youtube.com") || host.contains("youtu.be")
    }

    private func isMusicBrainzURL(_ url: URL) -> Bool {
        let host = url.host?.lowercased() ?? ""
        return host.contains("musicbrainz.org")
    }

    private func processYouTubeURL(_ url: URL, title: String? = nil) {
        NSLog("🎬 Processing YouTube URL: %{public}@", url.absoluteString)
        if let title = title {
            NSLog("📝 With title: %{public}@", title)
        }

        // Extract video ID from URL
        guard let videoId = extractYouTubeVideoId(from: url) else {
            NSLog("❌ Could not extract video ID from URL")
            showError("Could not extract video ID from YouTube URL")
            return
        }

        NSLog("✓ Extracted video ID: %{public}@", videoId)

        // If we don't have a title, fetch it from YouTube oEmbed API
        if title == nil {
            NSLog("📡 No title provided, fetching from YouTube oEmbed API...")
            fetchYouTubeTitle(for: url, videoId: videoId)
        } else {
            createYouTubeData(videoId: videoId, title: title!, url: url, channelName: nil)
        }
    }

    private func fetchYouTubeTitle(for url: URL, videoId: String) {
        // YouTube oEmbed API - no auth required
        let oembedURLString = "https://www.youtube.com/oembed?url=\(url.absoluteString.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed) ?? url.absoluteString)&format=json"

        guard let oembedURL = URL(string: oembedURLString) else {
            NSLog("❌ Failed to create oEmbed URL")
            createYouTubeData(videoId: videoId, title: "YouTube Video", url: url, channelName: nil)
            return
        }

        NSLog("📡 Fetching: %{public}@", oembedURLString)

        Task { [weak self] in
            guard let self = self else { return }
            let (title, authorName) = await Self.fetchOEmbedMetadata(from: oembedURL)
            await MainActor.run {
                self.createYouTubeData(videoId: videoId, title: title, url: url, channelName: authorName)
            }
        }
    }

    private static func fetchOEmbedMetadata(from oembedURL: URL) async -> (title: String, author: String?) {
        do {
            let (data, _) = try await URLSession.shared.data(from: oembedURL)
            guard let json = try JSONSerialization.jsonObject(with: data) as? [String: Any] else {
                NSLog("❌ Failed to parse oEmbed JSON")
                return ("YouTube Video", nil)
            }
            let title = json["title"] as? String ?? "YouTube Video"
            let author = json["author_name"] as? String
            NSLog("✅ oEmbed response - title: %{public}@, author: %{public}@", title, author ?? "(none)")
            return (title, author)
        } catch {
            NSLog("❌ oEmbed fetch error: %{public}@", error.localizedDescription)
            return ("YouTube Video", nil)
        }
    }

    private func createYouTubeData(videoId: String, title: String, url: URL, channelName: String?) {
        let youtubeData = YouTubeData(
            videoId: videoId,
            title: title,
            url: url.absoluteString,
            channelName: channelName,
            description: nil,
            videoType: nil,
            songId: nil,
            recordingId: nil
        )

        self.youtubeData = youtubeData
        self.isYouTubeImport = true

        NSLog("✅ YouTube data created with title: %{public}@", title)

        // Show YouTube type selection view
        showYouTubeTypeSelectionView(with: youtubeData)
    }

    private func extractYouTubeVideoId(from url: URL) -> String? {
        // Handle youtu.be short URLs
        if url.host?.lowercased().contains("youtu.be") == true {
            // Format: https://youtu.be/VIDEO_ID or https://youtu.be/VIDEO_ID?...
            return url.pathComponents.last
        }

        // Handle youtube.com URLs
        if let queryItems = URLComponents(url: url, resolvingAgainstBaseURL: false)?.queryItems {
            // Standard watch URL: https://www.youtube.com/watch?v=VIDEO_ID
            if let videoId = queryItems.first(where: { $0.name == "v" })?.value {
                return videoId
            }
        }

        // Handle embed URLs: https://www.youtube.com/embed/VIDEO_ID
        if url.pathComponents.contains("embed"), let videoId = url.pathComponents.last {
            return videoId
        }

        // Handle shorts URLs: https://www.youtube.com/shorts/VIDEO_ID
        if url.pathComponents.contains("shorts"), let videoId = url.pathComponents.last {
            return videoId
        }

        return nil
    }

    // MARK: - Data Processing
    private func processExtractedData(_ data: [String: Any]) {
        NSLog("📄 Processing extracted data...")

        // Check for error from JavaScript
        if let error = data["error"] as? String {
            NSLog("❌ JavaScript error: \(error)")
            showError(error)
            return
        }

        // Detect page type based on the fields present in the JavaScript results
        if let pageType = data["pageType"] as? String, pageType == "youtube" {
            NSLog("📍 Detected: YouTube page")
            processYouTubeDataFromJS(data)
        } else if let _ = data["title"] as? String {
            NSLog("📍 Detected: Song/Work page (has 'title' field)")
            processSongData(data)
        } else if let _ = data["name"] as? String {
            NSLog("📍 Detected: Artist page (has 'name' field)")
            processArtistData(data)
        } else {
            NSLog("❌ Could not determine page type from JavaScript results")
            NSLog("   Available keys: \(data.keys.joined(separator: ", "))")
            showError("Could not extract data from this page")
        }
    }

    private func processArtistData(_ data: [String: Any]) {
        NSLog("🎵 Processing artist data...")

        // Extract ONLY minimal artist information from the JavaScript results
        let name = data["name"] as? String ?? ""
        let musicbrainzId = data["musicbrainzId"] as? String ?? ""
        let wikipediaUrl = data["wikipediaUrl"] as? String
        let sourceUrl = data["url"] as? String

        let artistData = ArtistData(
            name: name,
            musicbrainzId: musicbrainzId,
            wikipediaUrl: wikipediaUrl,
            sourceUrl: sourceUrl
        )

        NSLog("   Artist: \(artistData.name)")
        NSLog("   MusicBrainz ID: \(artistData.musicbrainzId)")

        // Validate that we got at least a name and ID
        guard !artistData.name.isEmpty, !artistData.musicbrainzId.isEmpty else {
            NSLog("❌ Missing required fields - name: '\(artistData.name)', id: '\(artistData.musicbrainzId)'")
            showError("Could not extract artist information from this page")
            return
        }

        self.artistData = artistData
        NSLog("✅ Artist data validation passed")

        // Check if artist already exists in database
        checkArtistExistence(artistData)
    }

    // MARK: - Database Checking
    private func checkArtistExistence(_ artistData: ArtistData) {
        NSLog("🔍 Checking if artist exists in database...")

        // Show loading indicator
        showLoadingView()

        Task {
            do {
                let result = try await ShareDatabaseService.shared.checkArtistExists(
                    name: artistData.name,
                    musicbrainzId: artistData.musicbrainzId
                )

                await MainActor.run {
                    NSLog("✅ Database check complete")
                    self.handleArtistMatchResult(result, artistData: artistData)
                }
            } catch {
                await MainActor.run {
                    NSLog("⚠️ Database check failed: \(error.localizedDescription)")
                    NSLog("   Proceeding with import anyway...")
                    // If database check fails, just proceed with normal import
                    self.showConfirmationView(with: artistData)
                }
            }
        }
    }

    private func handleArtistMatchResult(_ result: ArtistMatchResult, artistData: ArtistData) {
        switch result {
        case .notFound:
            NSLog("ℹ️ Artist not found - showing normal import")
            showConfirmationView(with: artistData)

        case .exactMatch(let existingArtist):
            NSLog("ℹ️ Exact match found - artist already exists")
            self.existingArtist = existingArtist
            showExactMatchView(artistData: artistData, existingArtist: existingArtist)

        case .nameMatchNoMbid(let existingArtist):
            NSLog("ℹ️ Name match with blank MusicBrainz ID")
            self.existingArtist = existingArtist
            showNameMatchNoMbidView(artistData: artistData, existingArtist: existingArtist)

        case .nameMatchDifferentMbid(let existingArtist):
            NSLog("ℹ️ Name match with different MusicBrainz ID")
            self.existingArtist = existingArtist
            showNameMatchDifferentMbidView(artistData: artistData, existingArtist: existingArtist)
        }
    }

    // MARK: - Song Processing

    private func processSongData(_ data: [String: Any]) {
        NSLog("🎵 Processing song data...")
        NSLog("   Data keys: \(data.keys.joined(separator: ", "))")

        // Check if there's an error from JavaScript
        if let error = data["error"] as? String {
            NSLog("❌ JavaScript error: \(error)")
            showError(error)
            return
        }

        // Extract required fields
        guard let title = data["title"] as? String,
              !title.isEmpty else {
            NSLog("❌ Missing or empty title")
            showError("Could not extract song title from page")
            return
        }

        guard let musicbrainzId = data["musicbrainzId"] as? String,
              !musicbrainzId.isEmpty else {
            NSLog("❌ Missing or empty MusicBrainz ID")
            showError("Could not extract MusicBrainz ID from page")
            return
        }

        NSLog("   Title: \(title)")
        NSLog("   MusicBrainz ID: \(musicbrainzId)")

        // Extract optional fields
        let composers = data["composers"] as? [String]
        let workType = data["workType"] as? String
        let key = data["key"] as? String
        let annotation = data["annotation"] as? String
        let wikipediaUrl = data["wikipediaUrl"] as? String
        let sourceUrl = data["url"] as? String

        // Log optional fields if present
        if let composers = composers, !composers.isEmpty {
            NSLog("   Composers: \(composers.joined(separator: ", "))")
        }
        if let workType = workType {
            NSLog("   Work Type: \(workType)")
        }

        // Create SongData
        let songData = SongData(
            title: title,
            musicbrainzId: musicbrainzId,
            composers: composers,
            workType: workType,
            key: key,
            annotation: annotation,
            wikipediaUrl: wikipediaUrl,
            sourceUrl: sourceUrl
        )

        self.songData = songData

        NSLog("✅ Song data validation passed")

        // Check if song already exists in database
        showLoadingView()
        checkSongMatch(songData: songData)
    }

    private func checkSongMatch(songData: SongData) {
        Task {
            do {
                NSLog("🔍 Checking if song exists in database...")
                let result = try await ShareDatabaseService.shared.checkSongExists(
                    title: songData.title,
                    musicbrainzId: songData.musicbrainzId
                )

                DispatchQueue.main.async {
                    self.processSongMatch(result: result, songData: songData)
                }
            } catch {
                NSLog("❌ Error checking song: \(error)")
                DispatchQueue.main.async {
                    // If database check fails, just proceed with import
                    NSLog("⚠️ Database check failed, proceeding with import anyway...")
                    self.showSongConfirmationView(with: songData)
                }
            }
        }
    }

    private func processSongMatch(result: SongMatchResult, songData: SongData) {
        switch result {
        case .notFound:
            NSLog("✓ Song not found in database - showing import view")
            showSongConfirmationView(with: songData)

        case .exactMatch(let existingSong):
            NSLog("⚠️ Exact match found - song already exists")
            self.existingSong = existingSong
            showSongExactMatchView(songData: songData, existingSong: existingSong)

        case .titleMatchNoMbid(let existingSong):
            NSLog("⚠️ Title match with no MusicBrainz ID")
            self.existingSong = existingSong
            showSongTitleMatchNoMbidView(songData: songData, existingSong: existingSong)

        case .titleMatchDifferentMbid(let existingSong):
            NSLog("⚠️ Title match with different MusicBrainz ID")
            self.existingSong = existingSong
            showSongTitleMatchDifferentMbidView(songData: songData, existingSong: existingSong)
        }
    }

    // MARK: - YouTube Processing

    private func processYouTubeDataFromJS(_ data: [String: Any]) {
        NSLog("🎬 Processing YouTube data...")
        NSLog("   Data keys: \(data.keys.joined(separator: ", "))")

        // Extract required fields
        guard let videoId = data["videoId"] as? String, !videoId.isEmpty else {
            NSLog("❌ Missing or empty video ID")
            showError("Could not extract video ID from YouTube page")
            return
        }

        guard let title = data["title"] as? String, !title.isEmpty else {
            NSLog("❌ Missing or empty title")
            showError("Could not extract video title from YouTube page")
            return
        }

        guard let url = data["url"] as? String else {
            NSLog("❌ Missing URL")
            showError("Could not get YouTube URL")
            return
        }

        NSLog("   Video ID: \(videoId)")
        NSLog("   Title: \(title)")

        // Extract optional fields
        let channelName = data["channelName"] as? String
        let description = data["description"] as? String

        // Create YouTubeData
        let youtubeData = YouTubeData(
            videoId: videoId,
            title: title,
            url: url,
            channelName: channelName,
            description: description,
            videoType: nil,
            songId: nil,
            recordingId: nil
        )

        self.youtubeData = youtubeData
        self.isYouTubeImport = true

        NSLog("✅ YouTube data validation passed")

        // Show YouTube type selection view
        showYouTubeTypeSelectionView(with: youtubeData)
    }

    // MARK: - UI Views

    private func showLoadingView() {
        let loadingView = MacLoadingView()
        replaceCurrentView(with: loadingView)
    }

    private func showConfirmationView(with artistData: ArtistData) {
        NSLog("🎨 Showing confirmation view")

        let confirmationView = MacArtistImportConfirmationView(
            artistData: artistData,
            onImport: { [weak self] in
                self?.importArtist()
            },
            onCancel: { [weak self] in
                self?.cancelImport()
            }
        )

        replaceCurrentView(with: confirmationView)
    }

    private func showExactMatchView(artistData: ArtistData, existingArtist: ExistingArtist) {
        NSLog("🎨 Showing exact match view")

        let view = MacArtistExactMatchView(
            artistData: artistData,
            existingArtist: existingArtist,
            onOpenInApp: { [weak self] in
                self?.openArtistInApp(artistId: existingArtist.id)
            },
            onCancel: { [weak self] in
                self?.cancelImport()
            }
        )

        replaceCurrentView(with: view)
    }

    private func showNameMatchNoMbidView(artistData: ArtistData, existingArtist: ExistingArtist) {
        NSLog("🎨 Showing name match (no MBID) view")

        let view = MacArtistNameMatchNoMbidView(
            artistData: artistData,
            existingArtist: existingArtist,
            onAssociate: { [weak self] in
                self?.showNotImplementedAlert("Associate MusicBrainz ID")
            },
            onCancel: { [weak self] in
                self?.cancelImport()
            }
        )

        replaceCurrentView(with: view)
    }

    private func showNameMatchDifferentMbidView(artistData: ArtistData, existingArtist: ExistingArtist) {
        NSLog("🎨 Showing name match (different MBID) view")

        let view = MacArtistNameMatchDifferentMbidView(
            artistData: artistData,
            existingArtist: existingArtist,
            onOverwrite: { [weak self] in
                self?.showNotImplementedAlert("Overwrite MusicBrainz ID")
            },
            onCancel: { [weak self] in
                self?.cancelImport()
            }
        )

        replaceCurrentView(with: view)
    }

    private func showSongConfirmationView(with songData: SongData) {
        NSLog("🎨 Showing song confirmation view")

        let confirmationView = MacSongImportConfirmationView(
            songData: songData,
            onImport: { [weak self] in
                self?.importSong()
            },
            onCancel: { [weak self] in
                self?.cancelImport()
            }
        )

        replaceCurrentView(with: confirmationView)
    }

    private func showSongExactMatchView(songData: SongData, existingSong: ExistingSong) {
        NSLog("🎨 Showing song exact match view")

        let view = MacSongExactMatchView(
            songData: songData,
            existingSong: existingSong,
            onOpenInApp: { [weak self] in
                self?.openSongInApp(songId: existingSong.id)
            },
            onCancel: { [weak self] in
                self?.cancelImport()
            }
        )

        replaceCurrentView(with: view)
    }

    private func showSongTitleMatchNoMbidView(songData: SongData, existingSong: ExistingSong) {
        NSLog("🎨 Showing song title match (no MBID) view")

        let view = MacSongTitleMatchNoMbidView(
            songData: songData,
            existingSong: existingSong,
            onAssociate: { [weak self] in
                self?.showNotImplementedAlert("Associate MusicBrainz ID to song")
            },
            onCancel: { [weak self] in
                self?.cancelImport()
            }
        )

        replaceCurrentView(with: view)
    }

    private func showSongTitleMatchDifferentMbidView(songData: SongData, existingSong: ExistingSong) {
        NSLog("🎨 Showing song title match (different MBID) view")

        let view = MacSongTitleMatchDifferentMbidView(
            songData: songData,
            existingSong: existingSong,
            onOverwrite: { [weak self] in
                self?.showNotImplementedAlert("Overwrite MusicBrainz ID")
            },
            onCancel: { [weak self] in
                self?.cancelImport()
            }
        )

        replaceCurrentView(with: view)
    }

    // MARK: - YouTube UI Views

    private func showYouTubeTypeSelectionView(with youtubeData: YouTubeData) {
        NSLog("🎨 Showing YouTube type selection view")

        let view = MacYouTubeTypeSelectionView(
            youtubeData: youtubeData,
            onSelectType: { [weak self] selectedType in
                self?.handleYouTubeTypeSelected(youtubeData: youtubeData, type: selectedType)
            },
            onCancel: { [weak self] in
                self?.cancelImport()
            }
        )

        replaceCurrentView(with: view)
    }

    private func handleYouTubeTypeSelected(youtubeData: YouTubeData, type: YouTubeVideoType) {
        NSLog("🎬 YouTube type selected: \(type.rawValue)")

        // Update youtubeData with selected type
        var updatedData = youtubeData
        updatedData.videoType = type

        self.youtubeData = updatedData

        // Save to shared container and open main app for song selection
        SharedYouTubeData.saveSharedData(updatedData, appGroup: appGroupIdentifier)

        // Open main app to continue import (song selection happens there)
        openMainApp(path: "import-youtube")
    }

    private func showError(_ message: String) {
        NSLog("⚠️ Showing error: \(message)")

        let errorView = MacErrorView(
            message: message,
            onDismiss: { [weak self] in
                self?.cancelImport()
            }
        )

        replaceCurrentView(with: errorView)
    }

    private func replaceCurrentView<Content: View>(with swiftUIView: Content) {
        // Remove all existing subviews
        view.subviews.forEach { $0.removeFromSuperview() }

        // Create hosting view for SwiftUI
        let hostingView = NSHostingView(rootView: swiftUIView)
        hostingView.translatesAutoresizingMaskIntoConstraints = false

        view.addSubview(hostingView)

        NSLayoutConstraint.activate([
            hostingView.topAnchor.constraint(equalTo: view.topAnchor),
            hostingView.bottomAnchor.constraint(equalTo: view.bottomAnchor),
            hostingView.leadingAnchor.constraint(equalTo: view.leadingAnchor),
            hostingView.trailingAnchor.constraint(equalTo: view.trailingAnchor)
        ])
    }

    // MARK: - Actions

    private func importArtist() {
        NSLog("🎯 importArtist() called")

        guard let artistData = artistData else {
            showError("No artist data to import")
            return
        }

        NSLog("💾 Saving artist data to shared container")
        SharedArtistData.saveSharedData(artistData, appGroup: appGroupIdentifier)
        NSLog("✅ Data saved successfully")

        // Try to open the main app directly
        openMainApp(path: "import-artist")
    }

    private func openMainApp(path: String) {
        NSLog("🔗 Opening main app with path: %@", path)

        guard let url = URL(string: "approachnote://\(path)") else {
            NSLog("❌ Invalid URL scheme")
            fallbackToManualOpen(path: path)
            return
        }

        NSLog("🔗 Attempting to open URL: %@", url.absoluteString)

        // Check if the app is already running
        let runningApps = NSWorkspace.shared.runningApplications.filter {
            $0.bundleIdentifier == "com.approachnote.mac"
        }

        if let runningApp = runningApps.first {
            NSLog("📱 App is already running, activating existing instance")
            runningApp.activate()

            // Open the URL to trigger the deep link handler
            let configuration = NSWorkspace.OpenConfiguration()
            configuration.activates = false // Don't create new window, just handle URL
            NSWorkspace.shared.open(url, configuration: configuration) { [weak self] _, error in
                if let error = error {
                    NSLog("⚠️ URL open error (app already active): \(error.localizedDescription)")
                } else {
                    NSLog("✅ URL sent to running app")
                }
                DispatchQueue.main.asyncAfter(deadline: .now() + 0.3) {
                    self?.extensionContext?.completeRequest(returningItems: nil, completionHandler: nil)
                }
            }
        } else {
            NSLog("📱 App not running, launching with URL")
            // App not running, launch it with the URL
            let configuration = NSWorkspace.OpenConfiguration()
            configuration.activates = true
            NSWorkspace.shared.open(url, configuration: configuration) { [weak self] app, error in
                if let _ = app {
                    NSLog("✅ Successfully launched main app")
                    DispatchQueue.main.asyncAfter(deadline: .now() + 0.3) {
                        self?.extensionContext?.completeRequest(returningItems: nil, completionHandler: nil)
                    }
                } else {
                    NSLog("❌ Failed to open URL: \(error?.localizedDescription ?? "unknown error")")
                    DispatchQueue.main.async {
                        self?.fallbackToManualOpen(path: path)
                    }
                }
            }
        }
    }

    private func fallbackToManualOpen(path: String) {
        NSLog("⚠️ Using fallback manual open")

        DispatchQueue.main.async { [weak self] in
            let alert = NSAlert()
            alert.messageText = "Data Saved"
            alert.informativeText = "Open ApproachNote to complete the import."
            alert.alertStyle = .informational
            alert.addButton(withTitle: "OK")

            alert.runModal()

            self?.extensionContext?.completeRequest(returningItems: nil, completionHandler: nil)
        }
    }

    private func openArtistInApp(artistId: String) {
        NSLog("🔗 Opening artist in app: \(artistId)")
        openMainApp(path: "artist/\(artistId)")
    }

    private func importSong() {
        NSLog("💾 importSong() called")

        guard let songData = songData else {
            showError("No song data to import")
            return
        }

        NSLog("💾 Saving song data to shared container: %@", songData.title)
        SharedSongData.saveSharedData(songData, appGroup: appGroupIdentifier)
        NSLog("✅ Data saved successfully, opening main app")

        // Try to open the main app directly
        openMainApp(path: "import-song")
    }

    private func openSongInApp(songId: String) {
        NSLog("🔗 Opening song in app: \(songId)")
        openMainApp(path: "song/\(songId)")
    }

    private func showNotImplementedAlert(_ feature: String) {
        let alertView = MacNotImplementedView(
            feature: feature,
            onDismiss: { [weak self] in
                self?.cancelImport()
            }
        )
        replaceCurrentView(with: alertView)
    }

    private func cancelImport() {
        NSLog("❌ User cancelled import")
        extensionContext?.completeRequest(returningItems: nil, completionHandler: nil)
    }

    private func showSuccessAndClose() {
        NSLog("✅ Showing success message")

        // Show brief success message
        let successView = MacSuccessView()
        replaceCurrentView(with: successView)

        // Close after a brief delay
        DispatchQueue.main.asyncAfter(deadline: .now() + 1.0) { [weak self] in
            NSLog("👋 Closing extension")
            self?.extensionContext?.completeRequest(returningItems: nil, completionHandler: nil)
        }
    }
}
