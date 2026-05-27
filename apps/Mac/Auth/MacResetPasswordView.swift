//
//  MacResetPasswordView.swift
//  Approach Note
//
//  Password reset view (with token from deep link) for macOS
//

import SwiftUI

struct MacResetPasswordView: View {
    @EnvironmentObject var authManager: AuthenticationManager
    @Environment(\.dismiss) var dismiss

    let token: String

    @State private var newPassword = ""
    @State private var confirmPassword = ""
    @State private var resetComplete = false

    private var passwordsMatch: Bool {
        newPassword == confirmPassword && !newPassword.isEmpty
    }

    private var isFormValid: Bool {
        !newPassword.isEmpty &&
        newPassword.count >= 8 &&
        passwordsMatch
    }

    var body: some View {
        VStack(spacing: 20) {
            if resetComplete {
                // Success state
                successView
            } else {
                // Reset form
                resetFormView
            }

            Spacer()
        }
        .padding(24)
        .frame(minWidth: 350, maxWidth: 400, minHeight: 350)
    }

    @ViewBuilder
    private var successView: some View {
        VStack(spacing: 16) {
            Image(systemName: "checkmark.circle.fill")
                .font(.system(size: 60))
                .foregroundColor(.green)

            Text("Password Reset Complete")
                .font(ApproachNoteTheme.title2())
                .foregroundColor(ApproachNoteTheme.textPrimary)

            Text("Your password has been successfully reset. You can now sign in with your new password.")
                .font(ApproachNoteTheme.subheadline())
                .foregroundColor(.secondary)
                .multilineTextAlignment(.center)

            ApproachNoteButton("Done") {
                dismiss()
            }
            .padding(.top, 16)
        }
        .padding(.top, 40)
    }

    @ViewBuilder
    private var resetFormView: some View {
        VStack(spacing: 20) {
            // Header
            VStack(spacing: 8) {
                Text("Set New Password")
                    .font(ApproachNoteTheme.title())
                    .foregroundColor(ApproachNoteTheme.textPrimary)

                Text("Enter your new password below.")
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(.secondary)
            }
            .padding(.top, 20)

            // New password
            VStack(alignment: .leading, spacing: 6) {
                Text("New Password")
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(.secondary)

                SecureField("At least 8 characters", text: $newPassword)
                    .textFieldStyle(.roundedBorder)

                if !newPassword.isEmpty && newPassword.count < 8 {
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
                } else if !confirmPassword.isEmpty && passwordsMatch {
                    HStack {
                        Image(systemName: "checkmark.circle.fill")
                            .foregroundColor(.green)
                        Text("Passwords match")
                            .foregroundColor(.green)
                    }
                    .font(ApproachNoteTheme.caption())
                }
            }

            // Error message
            if let error = authManager.errorMessage {
                Text(error)
                    .font(ApproachNoteTheme.caption())
                    .foregroundColor(.red)
                    .multilineTextAlignment(.center)
                    .padding(.horizontal)
            }

            // Reset button
            ApproachNoteButton(
                "Reset Password",
                isLoading: authManager.isLoading,
                action: resetPassword
            )
            .disabled(!isFormValid)

            // Cancel button
            ApproachNoteButton("Cancel", style: .secondary) {
                dismiss()
            }
        }
    }

    private func resetPassword() {
        Task {
            let success = await authManager.resetPassword(
                token: token,
                newPassword: newPassword.trimmingCharacters(in: .whitespacesAndNewlines)
            )
            if success {
                resetComplete = true
            }
        }
    }
}

#Preview {
    MacResetPasswordView(token: "preview-token")
        .environmentObject(AuthenticationManager())
}
