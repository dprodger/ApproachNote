//
//  AuthenticationManager.swift
//  Approach Note
//
//  Created by Dave Rodger on 11/14/25.
//  Manages user authentication state and operations
//

import SwiftUI
import Combine
import os
import AuthenticationServices
#if canImport(GoogleSignIn)
import GoogleSignIn
#endif
#if canImport(AppKit)
import AppKit
#endif
#if canImport(UIKit)
import UIKit
#endif

@MainActor
class AuthenticationManager: ObservableObject {
    // MARK: - Published Properties
    
    @Published var isAuthenticated = false {
        didSet {
            SharedAuthState.isAuthenticated = isAuthenticated
            if !isAuthenticated {
                SharedAuthState.userDisplayName = nil
            }
        }
    }
    @Published var currentUser: User? {
        didSet {
            SharedAuthState.userDisplayName = currentUser?.displayName
        }
    }
    @Published var isLoading = false
    @Published var errorMessage: String?
    
    // MARK: - Private Properties

    private var accessToken: String?
    private let keychainHelper = KeychainHelper.shared

    // Token refresh serialization - prevents race conditions when multiple
    // requests fail with 401 and try to refresh simultaneously
    private var isRefreshingToken = false
    private var refreshWaiters: [CheckedContinuation<Bool, Never>] = []
    
    // MARK: - Initialization
    
    init() {
        // Try to load tokens from Keychain on init
        loadTokensFromKeychain()
    }
    
    // MARK: - Token Management
    
    private func loadTokensFromKeychain() {
        if let accessToken = keychainHelper.load(forKey: "access_token"),
           let _ = keychainHelper.load(forKey: "refresh_token") {
            self.accessToken = accessToken
            
            // Validate token by fetching current user
            Task {
                await fetchCurrentUser()
            }
        }
    }
    
    private func saveTokens(accessToken: String, refreshToken: String) {
        self.accessToken = accessToken
        _ = keychainHelper.save(accessToken, forKey: "access_token")
        _ = keychainHelper.save(refreshToken, forKey: "refresh_token")
    }
    
    /// Refresh the access token using the stored refresh token
    /// This method is serialized - if a refresh is already in progress,
    /// callers will wait for the result instead of starting a new refresh
    private func refreshAccessToken() async -> Bool {
        // If a refresh is already in progress, wait for its result
        if isRefreshingToken {
            Log.auth.debug("Token refresh already in progress - waiting for result")
            return await withCheckedContinuation { continuation in
                refreshWaiters.append(continuation)
            }
        }

        // Mark that we're now refreshing
        isRefreshingToken = true

        // Perform the actual refresh
        let result = await performTokenRefresh()

        // Notify all waiters of the result
        let waiters = refreshWaiters
        refreshWaiters.removeAll()
        isRefreshingToken = false

        for waiter in waiters {
            waiter.resume(returning: result)
        }

        return result
    }

    /// Actually performs the token refresh network request
    private func performTokenRefresh() async -> Bool {
        guard let refreshToken = keychainHelper.load(forKey: "refresh_token") else {
            Log.auth.error("No refresh token available")
            return false
        }

        let url = URL.api(path: "/auth/refresh-token")
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")

        let body: [String: String] = ["refresh_token": refreshToken]
        request.httpBody = try? JSONSerialization.data(withJSONObject: body)

        do {
            let (data, response) = try await URLSession.shared.data(for: request)

            guard let httpResponse = response as? HTTPURLResponse else {
                throw URLError(.badServerResponse)
            }

            if httpResponse.statusCode == 200 {
                struct RefreshResponse: Codable {
                    let accessToken: String
                    let refreshToken: String

                    enum CodingKeys: String, CodingKey {
                        case accessToken = "access_token"
                        case refreshToken = "refresh_token"
                    }
                }

                let refreshResponse = try JSONDecoder().decode(RefreshResponse.self, from: data)

                // Save new tokens
                saveTokens(accessToken: refreshResponse.accessToken,
                          refreshToken: refreshResponse.refreshToken)

                return true
            } else {
                Log.auth.error("Token refresh failed with status: \(httpResponse.statusCode)")
                return false
            }
        } catch {
            Log.auth.error("Token refresh error: \(error.localizedDescription)")
            return false
        }
    }

    private func clearTokens() {
        self.accessToken = nil
        keychainHelper.clearAll()
    }
    
    // MARK: - Authentication Methods
    
    /// Register a new user account
    func register(email: String, password: String, displayName: String) async -> Bool {
        isLoading = true
        errorMessage = nil
        
        let url = URL.api(path: "/auth/register")
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        
        let body: [String: Any] = [
            "email": email,
            "password": password,
            "display_name": displayName
        ]
        
        request.httpBody = try? JSONSerialization.data(withJSONObject: body)
        
        do {
            let (data, response) = try await URLSession.shared.data(for: request)
            
            guard let httpResponse = response as? HTTPURLResponse else {
                throw URLError(.badServerResponse)
            }
            
            if httpResponse.statusCode == 201 {
                let authResponse = try JSONDecoder().decode(AuthResponse.self, from: data)
                
                saveTokens(accessToken: authResponse.accessToken,
                          refreshToken: authResponse.refreshToken)
                currentUser = authResponse.user
                isAuthenticated = true
                isLoading = false
                
                Log.auth.info("Registration successful: \(authResponse.user.email, privacy: .private)")
                return true
            } else {
                // Try to parse error message
                if let errorResponse = try? JSONDecoder().decode(AuthError.self, from: data) {
                    errorMessage = errorResponse.error
                } else {
                    errorMessage = "Registration failed"
                }
                
                let msg = errorMessage ?? "Unknown error"
                Log.auth.error("Registration failed: \(msg)")
                isLoading = false
                return false
            }
        } catch {
            errorMessage = "Registration failed: \(error.localizedDescription)"
            Log.auth.error("Registration error: \(error.localizedDescription)")
            isLoading = false
            return false
        }
    }
    
    /// Login with email and password
    func login(email: String, password: String) async -> Bool {
        isLoading = true
        errorMessage = nil
        
        let url = URL.api(path: "/auth/login")
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        
        let body: [String: Any] = [
            "email": email,
            "password": password
        ]
        
        request.httpBody = try? JSONSerialization.data(withJSONObject: body)
        
        do {
            let (data, response) = try await URLSession.shared.data(for: request)
            
            guard let httpResponse = response as? HTTPURLResponse else {
                throw URLError(.badServerResponse)
            }
            
            if httpResponse.statusCode == 200 {
                let authResponse = try JSONDecoder().decode(AuthResponse.self, from: data)
                
                saveTokens(accessToken: authResponse.accessToken,
                          refreshToken: authResponse.refreshToken)
                currentUser = authResponse.user
                isAuthenticated = true
                isLoading = false
                
                Log.auth.info("Login successful: \(authResponse.user.email, privacy: .private)")
                return true
            } else {
                // Try to parse error message
                if let errorResponse = try? JSONDecoder().decode(AuthError.self, from: data) {
                    errorMessage = errorResponse.error
                } else {
                    errorMessage = "Login failed"
                }
                
                let msg = errorMessage ?? "Unknown error"
                Log.auth.error("Login failed: \(msg)")
                isLoading = false
                return false
            }
        } catch {
            errorMessage = "Login failed: \(error.localizedDescription)"
            Log.auth.error("Login error: \(error.localizedDescription)")
            isLoading = false
            return false
        }
    }
    
    /// Logout and clear all tokens
    func logout() {
        let userEmail = currentUser?.email ?? "unknown"
        Log.auth.info("Logging out user: \(userEmail, privacy: .private)")
        
        // Optional: Call logout endpoint to invalidate refresh token on backend
        if let token = accessToken,
           let refreshToken = keychainHelper.load(forKey: "refresh_token") {
            Task {
                await callLogoutEndpoint(accessToken: token, refreshToken: refreshToken)
            }
        }
        
        clearTokens()
        currentUser = nil
        isAuthenticated = false
        errorMessage = nil
    }
    
    /// Fetch current user info to validate token
    private func fetchCurrentUser() async {
        guard let token = accessToken else { return }

        let url = URL.api(path: "/auth/me")
        var request = URLRequest(url: url)
        request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")

        do {
            let (data, response) = try await URLSession.shared.data(for: request)

            guard let httpResponse = response as? HTTPURLResponse else {
                throw URLError(.badServerResponse)
            }

            if httpResponse.statusCode == 200 {
                let user = try JSONDecoder().decode(User.self, from: data)
                currentUser = user
                isAuthenticated = true
            } else if httpResponse.statusCode == 401 {
                // Access token expired — try refreshing before giving up
                Log.auth.warning("Access token expired, attempting refresh")
                let refreshed = await refreshAccessToken()
                if refreshed {
                    // Retry with the new access token
                    await fetchCurrentUser()
                } else {
                    Log.auth.error("Token refresh failed, clearing authentication")
                    clearTokens()
                    isAuthenticated = false
                }
            } else {
                Log.auth.warning("Unexpected status \(httpResponse.statusCode), clearing authentication")
                clearTokens()
                isAuthenticated = false
            }
        } catch {
            Log.auth.warning("Token validation failed: \(error.localizedDescription)")
            clearTokens()
            isAuthenticated = false
        }
    }
    
    /// Call logout endpoint on backend (best effort)
    private func callLogoutEndpoint(accessToken: String, refreshToken: String) async {
        let url = URL.api(path: "/auth/logout")
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        
        let body: [String: String] = ["refresh_token": refreshToken]
        request.httpBody = try? JSONSerialization.data(withJSONObject: body)
        
        do {
            _ = try await URLSession.shared.data(for: request)
            Log.auth.info("Backend logout successful")
        } catch {
            // Don't worry if it fails - tokens already cleared locally
            Log.auth.warning("Backend logout failed (non-critical): \(error.localizedDescription)")
        }
    }
    
    // MARK: - Password Reset
    
    /// Request a password reset email
    func requestPasswordReset(email: String) async -> Bool {
        isLoading = true
        errorMessage = nil
        
        let url = URL.api(path: "/auth/forgot-password")
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        
        let body: [String: String] = ["email": email]
        request.httpBody = try? JSONSerialization.data(withJSONObject: body)
        
        do {
            let (data, response) = try await URLSession.shared.data(for: request)
            
            guard let httpResponse = response as? HTTPURLResponse else {
                throw URLError(.badServerResponse)
            }
            
            isLoading = false
            
            if httpResponse.statusCode == 200 {
                Log.auth.info("Password reset email sent to: \(email, privacy: .private)")
                return true
            } else {
                if let errorResponse = try? JSONDecoder().decode(AuthError.self, from: data) {
                    errorMessage = errorResponse.error
                } else {
                    errorMessage = "Failed to send reset email"
                }
                let msg = errorMessage ?? "Unknown error"
                Log.auth.error("Password reset failed: \(msg)")
                return false
            }
        } catch {
            errorMessage = "Failed to send reset email: \(error.localizedDescription)"
            Log.auth.error("Password reset error: \(error.localizedDescription)")
            isLoading = false
            return false
        }
    }
    
    // MARK: - Authenticated API Requests
    
    /// Make an authenticated API request with automatic token inclusion
    // ENHANCED VERSION: Add this to AuthenticationManager.swift
    // This version supports GET, POST, DELETE with optional body data

    /// Make an authenticated API request with automatic token refresh on 401
    /// - Parameters:
    ///   - url: The URL to request
    ///   - method: HTTP method (GET, POST, DELETE, etc.)
    ///   - body: Optional request body data
    ///   - contentType: Optional content type header
    ///   - maxRetries: Maximum retry attempts (default 1)
    /// - Returns: Response data
    /// - Throws: URLError if authentication fails or network error occurs
    func makeAuthenticatedRequest(
        url: URL,
        method: String = "GET",
        body: Data? = nil,
        contentType: String? = "application/json",
        maxRetries: Int = 1
    ) async throws -> Data {
        guard let token = accessToken else {
            throw URLError(.userAuthenticationRequired)
        }
        
        var request = URLRequest(url: url)
        request.httpMethod = method
        request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        
        if let contentType = contentType {
            request.setValue(contentType, forHTTPHeaderField: "Content-Type")
        }
        
        if let body = body {
            request.httpBody = body
        }
        
        let (data, response) = try await URLSession.shared.data(for: request)
        
        guard let httpResponse = response as? HTTPURLResponse else {
            throw URLError(.badServerResponse)
        }
        
        // Check for redirect - Authorization header gets stripped on cross-origin redirects
        if let responseURL = httpResponse.url, responseURL != url {
            Log.auth.debug("Redirect detected: \(url.absoluteString, privacy: .private) -> \(responseURL.absoluteString, privacy: .private)")
            if httpResponse.statusCode == 401 {
                Log.auth.warning("401 after redirect - Authorization header was likely stripped!")
                Log.auth.debug("Fix: Update baseURL to use the final URL directly (avoid redirects)")
            }
        }

        // If unauthorized, try to refresh token and retry
        if httpResponse.statusCode == 401 {
            Log.auth.warning("401 Unauthorized - attempting token refresh")
            
            // Try to refresh token
            let refreshed = await refreshAccessToken()
            
            if refreshed && maxRetries > 0 {
                // Retry the request with new token
                Log.auth.debug("Retrying request with refreshed token")
                return try await makeAuthenticatedRequest(
                    url: url,
                    method: method,
                    body: body,
                    contentType: contentType,
                    maxRetries: maxRetries - 1
                )
            } else {
                // Refresh failed or no retries left, clear tokens
                Log.auth.error("Token refresh failed - clearing authentication")
                await MainActor.run {
                    clearTokens()
                    isAuthenticated = false
                }
                throw URLError(.userAuthenticationRequired)
            }
        }
        
        // For non-200 responses, throw an error with status code
        if httpResponse.statusCode >= 400 {
            let errorMessage = String(data: data, encoding: .utf8) ?? "HTTP \(httpResponse.statusCode)"
            Log.auth.error("API error \(httpResponse.statusCode): \(errorMessage)")
            throw NSError(
                domain: "APIError",
                code: httpResponse.statusCode,
                userInfo: [NSLocalizedDescriptionKey: errorMessage]
            )
        }
        
        return data
    }
    
    /// Get current access token (for manual requests)
    func getAccessToken() -> String? {
        return accessToken
    }
    
    /// Reset password using token from email
    func resetPassword(token: String, newPassword: String) async -> Bool {
        isLoading = true
        errorMessage = nil
        
        let url = URL.api(path: "/auth/reset-password")
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        
        let body: [String: String] = [
            "token": token,
            "password": newPassword
        ]
        request.httpBody = try? JSONSerialization.data(withJSONObject: body)
        
        do {
            let (data, response) = try await URLSession.shared.data(for: request)
            
            guard let httpResponse = response as? HTTPURLResponse else {
                throw URLError(.badServerResponse)
            }
            
            isLoading = false
            
            if httpResponse.statusCode == 200 {
                Log.auth.info("Password reset successful")
                return true
            } else {
                if let errorResponse = try? JSONDecoder().decode(AuthError.self, from: data) {
                    errorMessage = errorResponse.error
                } else {
                    errorMessage = "Password reset failed"
                }
                let msg = errorMessage ?? "Unknown error"
                Log.auth.error("Password reset failed: \(msg)")
                return false
            }
        } catch {
            errorMessage = "Password reset failed: \(error.localizedDescription)"
            Log.auth.error("Password reset error: \(error.localizedDescription)")
            isLoading = false
            return false
        }
    }
    
    #if canImport(GoogleSignIn)
    @MainActor
    func signInWithGoogle() async -> Bool {
        isLoading = true
        errorMessage = nil

        // Get the GIDClientID from Info.plist
        guard let clientID = Bundle.main.object(forInfoDictionaryKey: "GIDClientID") as? String else {
            errorMessage = "Google Client ID not configured"
            isLoading = false
            return false
        }

        // Configure Google Sign In
        let config = GIDConfiguration(clientID: clientID)
        GIDSignIn.sharedInstance.configuration = config

        #if os(iOS)
        // iOS: Get the presenting view controller
        guard let windowScene = UIApplication.shared.connectedScenes.first as? UIWindowScene,
              let rootViewController = windowScene.windows.first?.rootViewController else {
            errorMessage = "Unable to find window"
            isLoading = false
            return false
        }

        do {
            // Start Google Sign In on iOS
            let result = try await GIDSignIn.sharedInstance.signIn(
                withPresenting: rootViewController
            )

            // Get ID token
            guard let idToken = result.user.idToken?.tokenString else {
                errorMessage = "Failed to get ID token from Google"
                isLoading = false
                return false
            }

            // Send ID token to backend
            let success = await authenticateWithGoogle(idToken: idToken)
            isLoading = false
            return success

        } catch {
            errorMessage = "Google Sign In failed: \(error.localizedDescription)"
            isLoading = false
            return false
        }

        #elseif os(macOS)
        // macOS: Get the presenting window
        guard let window = NSApplication.shared.keyWindow else {
            errorMessage = "Unable to find window"
            isLoading = false
            return false
        }

        do {
            // Start Google Sign In on macOS
            let result = try await GIDSignIn.sharedInstance.signIn(
                withPresenting: window
            )

            // Get ID token
            guard let idToken = result.user.idToken?.tokenString else {
                errorMessage = "Failed to get ID token from Google"
                isLoading = false
                return false
            }

            // Send ID token to backend
            let success = await authenticateWithGoogle(idToken: idToken)
            isLoading = false
            return success

        } catch {
            errorMessage = "Google Sign In failed: \(error.localizedDescription)"
            isLoading = false
            return false
        }
        #endif
    }
    #else
    @MainActor
    func signInWithGoogle() async -> Bool {
        // GoogleSignIn not available
        errorMessage = "Google Sign-In is not available"
        return false
    }
    #endif
    
    private func authenticateWithGoogle(idToken: String) async -> Bool {
        let url = URL.api(path: "/auth/google")
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        
        let body: [String: Any] = ["id_token": idToken]
        request.httpBody = try? JSONSerialization.data(withJSONObject: body)
        
        do {
            let (data, response) = try await URLSession.shared.data(for: request)
            
            // Check HTTP status
            guard let httpResponse = response as? HTTPURLResponse else {
                errorMessage = "Invalid response from server"
                return false
            }
            
            if httpResponse.statusCode != 200 {
                // Try to parse error message
                if let errorResponse = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
                   let error = errorResponse["error"] as? String {
                    errorMessage = error
                } else {
                    errorMessage = "Authentication failed with status \(httpResponse.statusCode)"
                }
                return false
            }
            
            // Parse successful response
            let authResponse = try JSONDecoder().decode(AuthResponse.self, from: data)
            
            // Save tokens
            saveTokens(accessToken: authResponse.accessToken,
                      refreshToken: authResponse.refreshToken)
            
            // Update user state
            currentUser = authResponse.user
            isAuthenticated = true
            
            return true
        } catch {
            errorMessage = "Authentication failed: \(error.localizedDescription)"
            return false
        }
    }

    // MARK: - Sign in with Apple

    /// Handle the completion of a `SignInWithAppleButton` tap.
    ///
    /// Extracts the Apple identity token (and fullName, which Apple only
    /// provides on the very first sign-in for an app), and forwards them to
    /// the backend via `/auth/apple`. Subsequent sign-ins for the same user
    /// will have `fullName == nil` — that's expected.
    @MainActor
    func signInWithApple(_ result: Result<ASAuthorization, Error>) async -> Bool {
        isLoading = true
        errorMessage = nil
        defer { isLoading = false }

        switch result {
        case .failure(let error):
            // ASAuthorizationError.canceled is user-initiated and not an error
            // we want to surface.
            if (error as NSError).code == ASAuthorizationError.canceled.rawValue {
                return false
            }
            Log.auth.error("Apple sign-in failed: \(error.localizedDescription, privacy: .public)")
            errorMessage = "Apple Sign In failed: \(error.localizedDescription)"
            return false

        case .success(let authorization):
            guard let credential = authorization.credential as? ASAuthorizationAppleIDCredential else {
                errorMessage = "Unexpected credential type from Apple"
                return false
            }
            guard let tokenData = credential.identityToken,
                  let identityToken = String(data: tokenData, encoding: .utf8) else {
                errorMessage = "Failed to read Apple identity token"
                return false
            }

            // `fullName` and `email` are only populated on the very first
            // authorization for this app + Apple ID pair. On re-auth they're
            // nil and we must not overwrite what the backend already stored.
            // Apple sometimes omits email from the JWT even on first auth, so
            // the credential is the authoritative source — forward it so the
            // backend can populate users.email when creating the row.
            let fullName: String? = {
                guard let name = credential.fullName else { return nil }
                let formatter = PersonNameComponentsFormatter()
                formatter.style = .long
                let formatted = formatter.string(from: name).trimmingCharacters(in: .whitespaces)
                return formatted.isEmpty ? nil : formatted
            }()

            return await authenticateWithApple(
                identityToken: identityToken,
                fullName: fullName,
                email: credential.email
            )
        }
    }

    private func authenticateWithApple(identityToken: String, fullName: String?, email: String?) async -> Bool {
        let url = URL.api(path: "/auth/apple")
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")

        var body: [String: Any] = ["identity_token": identityToken]
        if let fullName {
            body["full_name"] = fullName
        }
        if let email {
            body["email"] = email
        }
        request.httpBody = try? JSONSerialization.data(withJSONObject: body)

        do {
            let (data, response) = try await URLSession.shared.data(for: request)

            guard let httpResponse = response as? HTTPURLResponse else {
                errorMessage = "Invalid response from server"
                return false
            }

            if httpResponse.statusCode != 200 {
                if let errorResponse = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
                   let error = errorResponse["error"] as? String {
                    errorMessage = error
                } else {
                    errorMessage = "Authentication failed with status \(httpResponse.statusCode)"
                }
                return false
            }

            let authResponse = try JSONDecoder().decode(AuthResponse.self, from: data)
            saveTokens(accessToken: authResponse.accessToken,
                       refreshToken: authResponse.refreshToken)
            currentUser = authResponse.user
            isAuthenticated = true
            return true
        } catch {
            errorMessage = "Authentication failed: \(error.localizedDescription)"
            return false
        }
    }

}
