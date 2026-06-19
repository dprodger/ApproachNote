import Foundation
import Combine
import os

// MARK: - MusicBrainz Service

@MainActor
class MusicBrainzService: ObservableObject {

    /// Search MusicBrainz for works (songs) by title
    func searchMusicBrainzWorks(query: String) async -> [MusicBrainzWork] {
        let startTime = Date()

        let encodedQuery = query.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed) ?? query
        let url = URL.api(path: "/musicbrainz/works/search?q=\(encodedQuery)")

        do {
            let (data, response) = try await URLSession.shared.data(from: url)

            guard let httpResponse = response as? HTTPURLResponse else {
                return []
            }

            APIClient.logRequest("GET /musicbrainz/works/search", startTime: startTime)

            if httpResponse.statusCode == 200 {
                let searchResponse = try JSONDecoder().decode(MusicBrainzSearchResponse.self, from: data)
                if APIClient.diagnosticsEnabled {
                    Log.network.debug("Found \(searchResponse.results.count, privacy: .public) MusicBrainz works")
                }
                return searchResponse.results
            } else {
                Log.network.error("Error searching MusicBrainz: HTTP \(httpResponse.statusCode, privacy: .public)")
                return []
            }
        } catch {
            Log.network.error("Error searching MusicBrainz: \(error)")
            return []
        }
    }

    /// Submit a request to add a song from MusicBrainz. Unlike import, this
    /// records a pending request for an admin to review — nothing is added to
    /// the catalog until it's approved.
    func submitSongRequest(work: MusicBrainzWork, authToken: String) async -> SongRequestResult {
        let startTime = Date()

        let url = URL.api(path: "/musicbrainz/request")

        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("Bearer \(authToken)", forHTTPHeaderField: "Authorization")
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")

        var body: [String: Any] = [
            "musicbrainz_id": work.id,
            "title": work.title
        ]
        if let composers = work.composers, !composers.isEmpty {
            body["composer"] = composers.joined(separator: ", ")
        }

        do {
            request.httpBody = try JSONSerialization.data(withJSONObject: body)

            let (data, response) = try await URLSession.shared.data(for: request)

            guard let httpResponse = response as? HTTPURLResponse else {
                return .failed(message: "Couldn't submit your request. Please try again.")
            }

            APIClient.logRequest("POST /musicbrainz/request", startTime: startTime)

            switch httpResponse.statusCode {
            case 201:
                let decoded = try? JSONDecoder().decode(SongRequestResponse.self, from: data)
                if APIClient.diagnosticsEnabled {
                    Log.network.info("Song request submitted: \(work.title, privacy: .private)")
                }
                return .submitted(message: decoded?.message ?? "Your request has been submitted for review.")
            case 409:
                let decoded = try? JSONDecoder().decode(SongRequestErrorResponse.self, from: data)
                Log.network.warning("Song request not needed: HTTP 409")
                return .alreadyKnown(message: decoded?.error ?? "This song has already been requested.")
            default:
                Log.network.error("Error submitting song request: HTTP \(httpResponse.statusCode, privacy: .public)")
                return .failed(message: "Couldn't submit your request. Please try again.")
            }
        } catch {
            Log.network.error("Error submitting song request: \(error)")
            return .failed(message: "Couldn't submit your request. Please try again.")
        }
    }
}
