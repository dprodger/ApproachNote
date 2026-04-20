//
//  RegisterView.swift
//  Approach Note
//
//  Created by Dave Rodger on 11/14/25.
//  Registration screen for creating new accounts
//

import SwiftUI

struct RegisterView: View {
    @EnvironmentObject var authManager: AuthenticationManager
    @Environment(\.dismiss) var dismiss
    
    @State private var email = ""
    @State private var password = ""
    @State private var confirmPassword = ""
    @State private var displayName = ""
    @State private var agreedToTerms = false
    
    var passwordsMatch: Bool {
        password == confirmPassword && !password.isEmpty
    }
    
    var isFormValid: Bool {
        !email.isEmpty && 
        !password.isEmpty && 
        password.count >= 8 &&
        passwordsMatch && 
        agreedToTerms
    }
    
    var body: some View {
        NavigationView {
            ScrollView {
                VStack(spacing: 24) {
                    // Header
                    VStack(spacing: 8) {
                        Text("Create Account")
                            .font(.largeTitle)
                            .fontWeight(.bold)
                            .foregroundColor(ApproachNoteTheme.charcoal)
                        
                        Text("Join ApproachNote")
                            .font(.subheadline)
                            .foregroundColor(.secondary)
                    }
                    .padding(.top, 40)
                    
                    // Display name
                    VStack(alignment: .leading, spacing: 8) {
                        Text("Display Name")
                            .font(.subheadline)
                            .foregroundColor(.secondary)
                        
                        TextField("What should we call you?", text: $displayName)
                            .padding()
                            .background(Color(.systemGray6))
                            .cornerRadius(10)
                    }
                    
                    // Email
                    VStack(alignment: .leading, spacing: 8) {
                        Text("Email")
                            .font(.subheadline)
                            .foregroundColor(.secondary)
                        
                        TextField("your@email.com", text: $email)
                            .textInputAutocapitalization(.never)
                            .keyboardType(.emailAddress)
                            .autocorrectionDisabled()
                            .padding()
                            .background(Color(.systemGray6))
                            .cornerRadius(10)
                    }
                    
                    // Password
                    VStack(alignment: .leading, spacing: 8) {
                        Text("Password")
                            .font(.subheadline)
                            .foregroundColor(.secondary)
                        
                        SecureField("At least 8 characters", text: $password)
                            .padding()
                            .background(Color(.systemGray6))
                            .cornerRadius(10)
                        
                        if !password.isEmpty && password.count < 8 {
                            Text("Password must be at least 8 characters")
                                .font(.caption)
                                .foregroundColor(.red)
                        }
                    }
                    
                    // Confirm password
                    VStack(alignment: .leading, spacing: 8) {
                        Text("Confirm Password")
                            .font(.subheadline)
                            .foregroundColor(.secondary)
                        
                        SecureField("Re-enter password", text: $confirmPassword)
                            .padding()
                            .background(Color(.systemGray6))
                            .cornerRadius(10)
                        
                        if !confirmPassword.isEmpty && !passwordsMatch {
                            Text("Passwords do not match")
                                .font(.caption)
                                .foregroundColor(.red)
                        }
                    }
                    
                    // Terms agreement
                    Toggle(isOn: $agreedToTerms) {
                        Text("I agree to the Terms of Service and Privacy Policy")
                            .font(.subheadline)
                            .foregroundColor(.secondary)
                    }
                    .tint(ApproachNoteTheme.burgundy)
                    
                    // Error message
                    if let error = authManager.errorMessage {
                        Text(error)
                            .font(.subheadline)
                            .foregroundColor(.red)
                            .multilineTextAlignment(.center)
                            .padding(.horizontal)
                    }
                    
                    // Register button
                    Button(action: {
                        Task {
                            let success = await authManager.register(
                                email: email.trimmingCharacters(in: .whitespacesAndNewlines),
                                password: password.trimmingCharacters(in: .whitespacesAndNewlines),
                                displayName: (displayName.isEmpty ? email : displayName).trimmingCharacters(in: .whitespacesAndNewlines)
                            )
                            if success {
                                dismiss()
                            }
                        }
                    }) {
                        if authManager.isLoading {
                            ProgressView()
                                .progressViewStyle(CircularProgressViewStyle(tint: .white))
                        } else {
                            Text("Create Account")
                                .fontWeight(.semibold)
                        }
                    }
                    .frame(maxWidth: .infinity)
                    .padding()
                    .background(isFormValid ? ApproachNoteTheme.burgundy : Color.gray)
                    .foregroundColor(.white)
                    .cornerRadius(10)
                    .disabled(!isFormValid || authManager.isLoading)
                    
                    Spacer()
                }
                .padding()
            }
            .navigationTitle("")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .navigationBarTrailing) {
                    Button("Close") {
                        dismiss()
                    }
                    .foregroundColor(ApproachNoteTheme.charcoal)
                }
            }
        }
    }
}

#Preview {
    RegisterView()
        .environmentObject(AuthenticationManager())
}
