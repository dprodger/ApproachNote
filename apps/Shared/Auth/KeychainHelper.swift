//
//  KeychainHelper.swift
//  Approach Note
//
//  Created by Dave Rodger on 11/14/25.
//  Secure storage for authentication tokens using iOS Keychain
//

import Foundation
import os
import Security

/// Helper class for securely storing and retrieving tokens from Keychain
class KeychainHelper {
    static let shared = KeychainHelper()

    /// Service identifier for keychain items (required for sandboxed Mac apps)
    private let service = "com.approachnote"

    private init() {}
    
    // MARK: - Public Methods
    
    /// Save a token value to Keychain
    /// - Parameters:
    ///   - value: The token string to save
    ///   - key: The key to store it under (e.g., "access_token")
    /// - Returns: True if save was successful
    func save(_ value: String, forKey key: String) -> Bool {
        guard let data = value.data(using: .utf8) else {
            Log.auth.error("Keychain: Failed to encode value as UTF-8")
            return false
        }
        
        // Delete any existing item first to avoid duplicates
        delete(forKey: key)
        
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: key,
            kSecValueData as String: data,
            kSecAttrAccessible as String: kSecAttrAccessibleWhenUnlocked,
            // Use the data-protection keychain (entitlement-gated) instead of the
            // legacy file-based keychain (code-signature-gated). Without this, a
            // standalone-signed Mac build is treated as a different app than the
            // one that stored the token, triggering a "wants to use confidential
            // information" prompt. No-op on iOS, where it's already the default.
            kSecUseDataProtectionKeychain as String: true
        ]
        
        let status = SecItemAdd(query as CFDictionary, nil)
        
        if status == errSecSuccess {
            return true
        } else {
            Log.auth.error("Keychain: Failed to save '\(key, privacy: .private)' with status: \(Int(status))")
            return false
        }
    }
    
    /// Load a token value from Keychain
    /// - Parameter key: The key to retrieve (e.g., "access_token")
    /// - Returns: The token string if found, nil otherwise
    func load(forKey key: String) -> String? {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: key,
            kSecReturnData as String: true,
            kSecMatchLimit as String: kSecMatchLimitOne,
            kSecUseDataProtectionKeychain as String: true
        ]
        
        var result: AnyObject?
        let status = SecItemCopyMatching(query as CFDictionary, &result)
        
        guard status == errSecSuccess,
              let data = result as? Data,
              let value = String(data: data, encoding: .utf8) else {
            if status != errSecItemNotFound {
                Log.auth.error("Keychain: Failed to load '\(key, privacy: .private)' with status: \(Int(status))")
            }
            return nil
        }

        return value
    }
    
    /// Delete a token from Keychain
    /// - Parameter key: The key to delete
    /// - Returns: True if delete was successful (or item didn't exist)
    @discardableResult
    func delete(forKey key: String) -> Bool {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: key,
            kSecUseDataProtectionKeychain as String: true
        ]

        let status = SecItemDelete(query as CFDictionary)
        
        // Success if deleted or item didn't exist
        let success = status == errSecSuccess || status == errSecItemNotFound
        
        if !success {
            Log.auth.error("Keychain: Failed to delete '\(key, privacy: .private)' with status: \(Int(status))")
        }

        return success
    }
    
    /// Clear all authentication tokens (used on logout)
    func clearAll() {
        Log.auth.info("Keychain: Clearing all tokens")
        delete(forKey: "access_token")
        delete(forKey: "refresh_token")
    }
}
