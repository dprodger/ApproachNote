//
//  RegisterView.swift
//  Approach Note
//
//  Created by Dave Rodger on 11/14/25.
//  Registration screen for creating new accounts
//

import SwiftUI
import PostHog

struct RegisterView: View {
    @EnvironmentObject var authManager: AuthenticationManager
    @Environment(\.dismiss) var dismiss
    
    @State private var email = ""
    @State private var password = ""
    @State private var confirmPassword = ""
    @State private var displayName = ""
    @State private var agreedToTerms = false
    @State private var revealPasswords = false
    
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
                VStack(spacing: ApproachNoteTheme.spacingXL) {
                    // Header
                    VStack(spacing: ApproachNoteTheme.spacingXS) {
                        Text("Create Account")
                            .font(.largeTitle)
                            .fontWeight(.bold)
                            .foregroundColor(ApproachNoteTheme.textPrimary)
                        
                        Text("Join ApproachNote")
                            .font(.subheadline)
                            .foregroundColor(.secondary)
                    }
                    .padding(.top, 40)
                    
                    // Display name
                    VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXS) {
                        Text("Display Name")
                            .font(.subheadline)
                            .foregroundColor(.secondary)
                        
                        TextField("What should we call you?", text: $displayName)
                            .padding()
                            .background(Color(.systemGray6))
                            .cornerRadius(10)
                    }
                    
                    // Email
                    VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXS) {
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
                    VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXS) {
                        Text("Password")
                            .font(.subheadline)
                            .foregroundColor(.secondary)
                        
                        revealablePasswordField("At least 8 characters", text: $password)

                        if !password.isEmpty && password.count < 8 {
                            Text("Password must be at least 8 characters")
                                .font(.caption)
                                .foregroundColor(.red)
                        }
                    }
                    
                    // Confirm password
                    VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXS) {
                        Text("Confirm Password")
                            .font(.subheadline)
                            .foregroundColor(.secondary)
                        
                        revealablePasswordField("Re-enter password", text: $confirmPassword)

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
                    .tint(ApproachNoteTheme.brand)
                    
                    // Error message
                    if let error = authManager.errorMessage {
                        Text(error)
                            .font(.subheadline)
                            .foregroundColor(.red)
                            .multilineTextAlignment(.center)
                            .padding(.horizontal)
                    }
                    
                    // Register button
                    ApproachNoteButton(
                        "Create Account",
                        isLoading: authManager.isLoading,
                        action: {
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
                        }
                    )
                    .disabled(!isFormValid)
                    
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
                    .foregroundColor(ApproachNoteTheme.textPrimary)
                }
            }
        }
        .postHogMask()
    }

    /// Password entry that toggles between obscured (`SecureField`) and plain
    /// (`TextField`) text. The eye button is shared state, so tapping it on
    /// either field reveals both — handy for confirming the two entries match.
    @ViewBuilder
    private func revealablePasswordField(_ placeholder: String, text: Binding<String>) -> some View {
        HStack {
            Group {
                if revealPasswords {
                    TextField(placeholder, text: text)
                } else {
                    SecureField(placeholder, text: text)
                }
            }
            .textInputAutocapitalization(.never)
            .autocorrectionDisabled()

            Button {
                revealPasswords.toggle()
            } label: {
                Image(systemName: revealPasswords ? "eye.slash" : "eye")
                    .foregroundColor(.secondary)
            }
            .accessibilityLabel(revealPasswords ? "Hide passwords" : "Show passwords")
        }
        .padding()
        .background(Color(.systemGray6))
        .cornerRadius(10)
    }
}

#Preview {
    RegisterView()
        .environmentObject(AuthenticationManager())
}
