//
//  ExternalReferencesPanel.swift
//  Approach Note
//
//  Reusable panel for displaying external references (Wikipedia, Jazz Standards, MusicBrainz)
//

import SwiftUI

struct ExternalReferencesPanel: View {
    let externalReferences: [String: String]?
    let wikipediaUrl: String?
    let musicbrainzId: String?
    let musicbrainzType: MusicBrainzType
    let entityType: String
    let entityId: String
    let entityName: String
    var showsBackground: Bool = true

    @State private var reportingInfo: ReportingInfo?
    @State private var longPressOccurred = false
    @State private var showingSubmissionAlert = false
    @State private var submissionAlertMessage = ""
    @Environment(\.openURL) var openURL
    
    struct ReportingInfo: Identifiable {
        let id = UUID()
        let source: String
        let url: String
    }
    
    enum MusicBrainzType {
        case work      // For songs
        case artist    // For performers
        case recording // For recordings
    }
    
    // NEW: Initializer for songs with dedicated wikipedia and musicbrainz fields
    init(wikipediaUrl: String?, musicbrainzId: String?, externalReferences: [String: String]?,
         entityId: String, entityName: String, showsBackground: Bool = true) {
        // Use dedicated fields first, fall back to external_references if needed
        var references = externalReferences ?? [:]
        if let wikipedia = wikipediaUrl {
            references["wikipedia"] = wikipedia
        }
        if let musicbrainz = musicbrainzId {
            references["musicbrainz"] = musicbrainz
        }

        self.externalReferences = references
        self.musicbrainzId = musicbrainzId
        self.musicbrainzType = .work
        self.wikipediaUrl = wikipediaUrl
        self.entityType = "song"
        self.entityId = entityId
        self.entityName = entityName
        self.showsBackground = showsBackground
    }
    
    // Initializer for artists with dedicated wikipedia and musicbrainz fields
    init(wikipediaUrl: String?, musicbrainzId: String?, externalLinks: [String: String]?,
         entityId: String, entityName: String, isArtist: Bool, showsBackground: Bool = true) {
        // Use dedicated fields first, fall back to external_links if needed
        var references = externalLinks ?? [:]
        if let wikipedia = wikipediaUrl {
            references["wikipedia"] = wikipedia
        }
        if let musicbrainz = musicbrainzId {
            references["musicbrainz"] = musicbrainz
        }

        self.externalReferences = references
        self.musicbrainzId = musicbrainzId
        self.musicbrainzType = .artist
        self.wikipediaUrl = wikipediaUrl
        self.entityType = "performer"
        self.entityId = entityId
        self.entityName = entityName
        self.showsBackground = showsBackground
    }
    
    // DEPRECATED: Legacy initializer for songs (for backward compatibility)
    // This is kept for any existing code that hasn't been updated yet
    init(externalReferences: [String: String]?, musicbrainzId: String?, entityId: String, entityName: String) {
        self.externalReferences = externalReferences
        self.musicbrainzId = musicbrainzId
        self.musicbrainzType = .work
        self.wikipediaUrl = nil as String?
        self.entityType = "song"
        self.entityId = entityId
        self.entityName = entityName
    }
    
    // Convenience initializer for artists (musicbrainz in externalLinks)
    init(externalLinks: [String: String]?, entityId: String, entityName: String) {
        self.externalReferences = externalLinks
        self.musicbrainzId = externalLinks?["musicbrainz"]
        self.musicbrainzType = .artist
        self.wikipediaUrl = nil as String?
        self.entityType = "performer"
        self.entityId = entityId
        self.entityName = entityName
    }
    
    // Convenience initializer for recordings (musicbrainz only)
    init(musicbrainzId: String?, recordingId: String, albumTitle: String) {
        self.externalReferences = nil
        self.musicbrainzId = musicbrainzId
        self.musicbrainzType = .recording
        self.wikipediaUrl = nil as String?
        self.entityType = "recording"
        self.entityId = recordingId
        self.entityName = albumTitle
    }
    
    var wikipediaURL: String? {
        return wikipediaUrl
    }
    
    var jazzStandardsURL: String? {
        externalReferences?["jazzstandards"]
    }
    
    var musicbrainzURL: String? {
        guard let musicbrainzId = musicbrainzId else { return nil }
        
        switch musicbrainzType {
        case .work:
            return "https://musicbrainz.org/work/\(musicbrainzId)"
        case .artist:
            return "https://musicbrainz.org/artist/\(musicbrainzId)"
        case .recording:
            return "https://musicbrainz.org/recording/\(musicbrainzId)"
        }
    }
    
    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("Learn More:")
                .font(ApproachNoteTheme.body(weight: .semibold))
                .bodyLineSpacing()
                .foregroundColor(ApproachNoteTheme.textPrimary)
            
            VStack(spacing: 10) {
                // Wikipedia
                if let wikipediaURL = wikipediaURL {
                    ExternalLinkButton(
                        label: "Wikipedia",
                        url: wikipediaURL,
                        onLongPress: {
                            reportingInfo = ReportingInfo(source: "Wikipedia", url: wikipediaURL)
                        }
                    )
                }

                // Jazz Standards
                if let jazzStandardsURL = jazzStandardsURL {
                    ExternalLinkButton(
                        label: "Jazz Standards",
                        url: jazzStandardsURL,
                        onLongPress: {
                            reportingInfo = ReportingInfo(source: "JazzStandards.com", url: jazzStandardsURL)
                        }
                    )
                }

                // MusicBrainz
                if let musicbrainzURL = musicbrainzURL {
                    ExternalLinkButton(
                        label: "MusicBrainz",
                        url: musicbrainzURL,
                        onLongPress: {
                            reportingInfo = ReportingInfo(source: "MusicBrainz", url: musicbrainzURL)
                        }
                    )
                }
            }
        }
        .padding(showsBackground ? EdgeInsets(top: 16, leading: 16, bottom: 16, trailing: 16)
                                 : EdgeInsets())
        .background(showsBackground ? ApproachNoteTheme.surface : Color.clear)
        .cornerRadius(showsBackground ? 10 : 0)
        .sheet(item: $reportingInfo) { info in
            ReportLinkIssueView(
                entityType: entityType,
                entityId: entityId,
                entityName: entityName,
                externalSource: info.source,
                externalUrl: info.url,
                onSubmit: { explanation in
                    submitLinkReport(
                        entityType: entityType,
                        entityId: entityId,
                        entityName: entityName,
                        externalSource: info.source,
                        externalUrl: info.url,
                        explanation: explanation
                    )
                    reportingInfo = nil
                },
                onCancel: {
                    reportingInfo = nil
                }
            )
        }
        .alert("Report Submitted", isPresented: $showingSubmissionAlert) {
            Button("OK", role: .cancel) { }
        } message: {
            Text(submissionAlertMessage)
        }
    }
    
    // MARK: - Submit link report to API
    private func submitLinkReport(entityType: String, entityId: String, entityName: String, externalSource: String, externalUrl: String, explanation: String) {
        Task {
            do {
                let success = try await ContentService.submitContentReport(
                    entityType: entityType,
                    entityId: entityId,
                    entityName: entityName,
                    externalSource: externalSource,
                    externalUrl: externalUrl,
                    explanation: explanation
                )

                if success {
                    submissionAlertMessage = "Thank you for your report. We will review it shortly."
                } else {
                    submissionAlertMessage = "Failed to submit report. Please try again later."
                }
                showingSubmissionAlert = true

            } catch {
                submissionAlertMessage = "Failed to submit report: \(error.localizedDescription)"
                showingSubmissionAlert = true
            }
        }
    }
}

// MARK: - External Link Button

struct ExternalLinkButton: View {
    let label: String
    let url: String
    let onLongPress: () -> Void

    @Environment(\.openURL) var openURL

    var body: some View {
        ApproachNoteButton(
            label,
            style: .secondary,
            trailingSystemImage: "arrow.up.right.square"
        ) {
            if let url = URL(string: url) {
                openURL(url)
            }
        }
        .simultaneousGesture(
            LongPressGesture(minimumDuration: 0.5)
                .onEnded { _ in
                    onLongPress()
                }
        )
    }
}

