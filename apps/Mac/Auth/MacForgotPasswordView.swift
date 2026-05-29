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
        VStack(spacing: ApproachNoteTheme.spacingLG) {
            if resetEmailSent {
                // Success state
                successView
            } else {
                // Request form
                requestFormView
            }

            Spacer()
        }
        .padding(ApproachNoteTheme.spacingXL)
        .frame(minWidth: 350, maxWidth: 400, minHeight: 300)
    }

    @ViewBuilder
    private var successView: some View {
        VStack(spacing: ApproachNoteTheme.spacingMD) {
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
                .padding(.top, ApproachNoteTheme.spacingXS)

            ApproachNoteButton("Done") {
                dismiss()
            }
            .padding(.top, ApproachNoteTheme.spacingMD)
        }
        .padding(.top, 40)
    }

    @ViewBuilder
    private var requestFormView: some View {
        VStack(spacing: ApproachNoteTheme.spacingLG) {
            // Header
            VStack(spacing: ApproachNoteTheme.spacingXS) {
                Text("Reset Password")
                    .font(ApproachNoteTheme.title())
                    .foregroundColor(ApproachNoteTheme.textPrimary)

                Text("Enter your email address and we'll send you instructions to reset your password.")
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(.secondary)
                    .multilineTextAlignment(.center)
            }
            .padding(.top, ApproachNoteTheme.spacingLG)

            // Email field
            VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXS) {
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
            ApproachNoteButton(
                "Send Reset Link",
                isLoading: authManager.isLoading,
                action: sendResetLink
            )
            .disabled(email.isEmpty)

            // Back button
            ApproachNoteButton("Cancel", style: .secondary) {
                dismiss()
            }
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
