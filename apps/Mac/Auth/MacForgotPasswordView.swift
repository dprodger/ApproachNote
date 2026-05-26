//
//  MacForgotPasswordView.swift
//  Approach Note
//
//  Password reset request view for macOS
//

import SwiftUI

struct MacForgotPasswordView: View {
    @EnvironmentObject var authManager: AuthenticationManager
    @Environment(\.dismiss) var dismiss

    @State private var email = ""
    @State private var resetEmailSent = false

    var body: some View {
        VStack(spacing: 20) {
            if resetEmailSent {
                // Success state
                successView
            } else {
                // Request form
                requestFormView
            }

            Spacer()
        }
        .padding(24)
        .frame(minWidth: 350, maxWidth: 400, minHeight: 300)
    }

    @ViewBuilder
    private var successView: some View {
        VStack(spacing: 16) {
            Image(systemName: "envelope.circle.fill")
                .font(.system(size: 60))
                .foregroundColor(ApproachNoteTheme.brand)

            Text("Check Your Email")
                .font(ApproachNoteTheme.title2())
                .foregroundColor(ApproachNoteTheme.textPrimary)

            Text("We've sent password reset instructions to:")
                .font(ApproachNoteTheme.subheadline())
                .foregroundColor(.secondary)
                .multilineTextAlignment(.center)

            Text(email)
                .font(ApproachNoteTheme.subheadline(weight: .semibold))
                .foregroundColor(ApproachNoteTheme.textPrimary)

            Text("Please check your email and follow the link to reset your password.")
                .font(ApproachNoteTheme.subheadline())
                .foregroundColor(.secondary)
                .multilineTextAlignment(.center)
                .padding(.top, 8)

            Button("Done") {
                dismiss()
            }
            .buttonStyle(.borderedProminent)
            .tint(ApproachNoteTheme.brand)
            .controlSize(.large)
            .padding(.top, 16)
        }
        .padding(.top, 40)
    }

    @ViewBuilder
    private var requestFormView: some View {
        VStack(spacing: 20) {
            // Header
            VStack(spacing: 8) {
                Text("Reset Password")
                    .font(ApproachNoteTheme.title())
                    .foregroundColor(ApproachNoteTheme.textPrimary)

                Text("Enter your email address and we'll send you instructions to reset your password.")
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(.secondary)
                    .multilineTextAlignment(.center)
            }
            .padding(.top, 20)

            // Email field
            VStack(alignment: .leading, spacing: 6) {
                Text("Email")
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(.secondary)

                TextField("your@email.com", text: $email)
                    .textFieldStyle(.roundedBorder)
                    .textContentType(.emailAddress)
                    .disableAutocorrection(true)
            }

            // Error message
            if let error = authManager.errorMessage {
                Text(error)
                    .font(ApproachNoteTheme.caption())
                    .foregroundColor(.red)
                    .multilineTextAlignment(.center)
                    .padding(.horizontal)
            }

            // Send button
            Button(action: sendResetLink) {
                if authManager.isLoading {
                    ProgressView()
                        .controlSize(.small)
                        .frame(maxWidth: .infinity)
                } else {
                    Text("Send Reset Link")
                        .frame(maxWidth: .infinity)
                }
            }
            .buttonStyle(.borderedProminent)
            .tint(ApproachNoteTheme.brand)
            .controlSize(.large)
            .disabled(email.isEmpty || authManager.isLoading)

            // Back button
            Button("Cancel") {
                dismiss()
            }
            .buttonStyle(.bordered)
            .controlSize(.large)
        }
    }

    private func sendResetLink() {
        Task {
            let success = await authManager.requestPasswordReset(
                email: email.trimmingCharacters(in: .whitespacesAndNewlines)
            )
            if success {
                resetEmailSent = true
            }
        }
    }
}

#Preview {
    MacForgotPasswordView()
        .environmentObject(AuthenticationManager())
}
