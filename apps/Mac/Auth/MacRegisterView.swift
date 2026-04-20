//
//  MacRegisterView.swift
//  Approach Note
//
//  Registration view for macOS
//

import SwiftUI

struct MacRegisterView: View {
    @EnvironmentObject var authManager: AuthenticationManager
    @Environment(\.dismiss) var dismiss

    @State private var email = ""
    @State private var password = ""
    @State private var confirmPassword = ""
    @State private var displayName = ""
    @State private var agreedToTerms = false

    private var passwordsMatch: Bool {
        password == confirmPassword && !password.isEmpty
    }

    private var isFormValid: Bool {
        !email.isEmpty &&
        !password.isEmpty &&
        password.count >= 8 &&
        passwordsMatch &&
        agreedToTerms
    }

    var body: some View {
        VStack(spacing: 20) {
            // Header
            VStack(spacing: 8) {
                Text("Create Account")
                    .font(ApproachNoteTheme.title())
                    .foregroundColor(ApproachNoteTheme.charcoal)

                Text("Join ApproachNote")
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(.secondary)
            }
            .padding(.top, 20)

            // Display name
            VStack(alignment: .leading, spacing: 6) {
                Text("Display Name")
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(.secondary)

                TextField("What should we call you?", text: $displayName)
                    .textFieldStyle(.roundedBorder)
            }

            // Email
            VStack(alignment: .leading, spacing: 6) {
                Text("Email")
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(.secondary)

                TextField("your@email.com", text: $email)
                    .textFieldStyle(.roundedBorder)
                    .textContentType(.emailAddress)
                    .disableAutocorrection(true)
            }

            // Password
            VStack(alignment: .leading, spacing: 6) {
                Text("Password")
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(.secondary)

                SecureField("At least 8 characters", text: $password)
                    .textFieldStyle(.roundedBorder)

                if !password.isEmpty && password.count < 8 {
                    Text("Password must be at least 8 characters")
                        .font(ApproachNoteTheme.caption())
                        .foregroundColor(.red)
                }
            }

            // Confirm password
            VStack(alignment: .leading, spacing: 6) {
                Text("Confirm Password")
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(.secondary)

                SecureField("Re-enter password", text: $confirmPassword)
                    .textFieldStyle(.roundedBorder)

                if !confirmPassword.isEmpty && !passwordsMatch {
                    Text("Passwords do not match")
                        .font(ApproachNoteTheme.caption())
                        .foregroundColor(.red)
                }
            }

            // Terms agreement
            Toggle(isOn: $agreedToTerms) {
                Text("I agree to the Terms of Service and Privacy Policy")
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(.secondary)
            }
            .toggleStyle(.checkbox)

            // Error message
            if let error = authManager.errorMessage {
                Text(error)
                    .font(ApproachNoteTheme.caption())
                    .foregroundColor(.red)
                    .multilineTextAlignment(.center)
                    .padding(.horizontal)
            }

            // Register button
            Button(action: register) {
                if authManager.isLoading {
                    ProgressView()
                        .controlSize(.small)
                        .frame(maxWidth: .infinity)
                } else {
                    Text("Create Account")
                        .frame(maxWidth: .infinity)
                }
            }
            .buttonStyle(.borderedProminent)
            .tint(ApproachNoteTheme.burgundy)
            .controlSize(.large)
            .disabled(!isFormValid || authManager.isLoading)

            // Cancel button
            Button("Cancel") {
                dismiss()
            }
            .buttonStyle(.bordered)
            .controlSize(.large)

            Spacer()
        }
        .padding(24)
        .frame(minWidth: 350, maxWidth: 450, minHeight: 500)
    }

    private func register() {
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
}

#Preview {
    MacRegisterView()
        .environmentObject(AuthenticationManager())
}
