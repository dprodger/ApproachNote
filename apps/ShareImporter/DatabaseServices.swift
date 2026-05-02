//
//  DatabaseServices.swift
//  Approach Note
//
//  Created by Dave Rodger on 11/1/25.
//

import Foundation

// Hardcoded because the share extension target can't link apps/Shared/Services/APIClient.swift.
// Keep in sync with APIClient.baseURL and APIClient.apiVersionPath.
private let apiBaseURL = "https://api.approachnote.com"
private let apiVersionPath = "/v1"
private let requestTimeout: TimeInterval = 10.0

final class ShareDatabaseService {
    static let shared = ShareDatabaseService()

    private let decoder = JSONDecoder()

    private init() {}

    /// Check if an artist exists in the database by name and/or MusicBrainz ID
    func checkArtistExists(name: String, musicbrainzId: String) async throws -> ArtistMatchResult {
        guard let encodedName = name.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed) else {
            throw URLError(.badURL)
        }

        let results: [ExistingArtist]
        do {
            results = try await get(path: "/performers/search?name=\(encodedName)")
        } catch ShareAPIError.notFound {
            return .notFound
        }

        guard let matchingArtist = results.first(where: { $0.name.lowercased() == name.lowercased() }) else {
            return .notFound
        }

        guard let existingMbid = matchingArtist.musicbrainzId else {
            return .nameMatchNoMbid(existingArtist: matchingArtist)
        }

        return existingMbid == musicbrainzId
            ? .exactMatch(existingArtist: matchingArtist)
            : .nameMatchDifferentMbid(existingArtist: matchingArtist)
    }

    /// Check if a song exists in the database by title and/or MusicBrainz ID
    func checkSongExists(title: String, musicbrainzId: String) async throws -> SongMatchResult {
        guard let encodedTitle = title.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed) else {
            throw URLError(.badURL)
        }

        let results: [ExistingSong]
        do {
            results = try await get(path: "/songs?search=\(encodedTitle)")
        } catch ShareAPIError.notFound {
            return .notFound
        }

        guard let matchingSong = results.first(where: { $0.title.lowercased() == title.lowercased() }) else {
            return .notFound
        }

        guard let existingMbid = matchingSong.musicbrainzId else {
            return .titleMatchNoMbid(existingSong: matchingSong)
        }

        return existingMbid == musicbrainzId
            ? .exactMatch(existingSong: matchingSong)
            : .titleMatchDifferentMbid(existingSong: matchingSong)
    }

    private func get<T: Decodable>(path: String) async throws -> T {
        guard let url = URL(string: apiBaseURL + apiVersionPath + path) else {
            throw URLError(.badURL)
        }

        var request = URLRequest(url: url)
        request.httpMethod = "GET"
        request.setValue("application/json", forHTTPHeaderField: "Accept")
        request.timeoutInterval = requestTimeout

        let (data, response) = try await URLSession.shared.data(for: request)

        guard let httpResponse = response as? HTTPURLResponse else {
            throw URLError(.badServerResponse)
        }

        if httpResponse.statusCode == 404 {
            throw ShareAPIError.notFound
        }

        guard httpResponse.statusCode == 200 else {
            throw URLError(.badServerResponse)
        }

        return try decoder.decode(T.self, from: data)
    }
}

private enum ShareAPIError: Error {
    case notFound
}
