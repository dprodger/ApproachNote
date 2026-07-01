//
//  ForgotPasswordView.swift
//  Approach Note
//
//  Created by Dave Rodger on 11/14/25.
//  Password reset request screen
//

import SwiftUI

struct ForgotPasswordView: View {
    @EnvironmentObject var authManager: AuthenticationManager
    @Environment(\.dismiss) var dismiss

    @State private var email = ""

    var body: some View {
        NavigationView {
            ScrollView {
                VStack(spacing: ApproachNoteTheme.spacingXL) {
                    if authManager.passwordResetEmailSent {
                        // Success state
                        VStack(spacing: ApproachNoteTheme.spacingMD) {
                            Image(systemName: "envelope.circle.fill")
                                .font(.system(size: 60))
                                .foregroundColor(ApproachNoteTheme.brand)
                            
                            Text("Check Your Email")
                                .font(.title2)
                                .fontWeight(.bold)
                                .foregroundColor(ApproachNoteTheme.textPrimary)
                            
                            Text("We've sent password reset instructions to:")
                                .font(.subheadline)
                                .foregroundColor(.secondary)
                                .multilineTextAlignment(.center)
                            
                            Text(authManager.passwordResetEmailAddress)
                                .font(.subheadline)
                                .fontWeight(.semibold)
                                .foregroundColor(ApproachNoteTheme.textPrimary)
                            
                            Text("Please check your email and follow the link to reset your password.")
                                .font(.subheadline)
                                .foregroundColor(.secondary)
                                .multilineTextAlignment(.center)
                                .padding(.top, ApproachNoteTheme.spacingXS)
                            
                            ApproachNoteButton("Done") {
                                dismiss()
                            }
                            .padding(.top, ApproachNoteTheme.spacingMD)
                        }
                        .padding(.top, 60)
                    } else {
                        // Request form
                        VStack(spacing: ApproachNoteTheme.spacingXS) {
                            Text("Reset Password")
                                .font(.largeTitle)
                                .fontWeight(.bold)
                                .foregroundColor(ApproachNoteTheme.textPrimary)
                            
                            Text("Enter your email address and we'll send you instructions to reset your password.")
                                .font(.subheadline)
                                .foregroundColor(.secondary)
                                .multilineTextAlignment(.center)
                        }
                        .padding(.top, 40)
                        
                        // Email field
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
                        
                        // Error message
                        if let error = authManager.errorMessage {
                            Text(error)
                                .font(.subheadline)
                                .foregroundColor(.red)
                                .multilineTextAlignment(.center)
                                .padding(.horizontal)
                        }
                        
                        // Send button
                        ApproachNoteButton(
                            "Send Reset Link",
                            isLoading: authManager.isLoading,
                            action: {
                                Task {
                                    // Success is driven by authManager.passwordResetEmailSent
                                    // (@Published, set on the main actor), so the confirmation
                                    // state survives the sheet's re-renders.
                                    _ = await authManager.requestPasswordReset(
                                        email: email.trimmingCharacters(in: .whitespacesAndNewlines)
                                    )
                                }
                            }
                        )
                        .disabled(email.isEmpty)
                        
                        // Back to login
                        Button(action: {
                            dismiss()
                        }) {
                            Text("Back to Sign In")
                                .font(.subheadline)
                                .foregroundColor(ApproachNoteTheme.brand)
                        }
                        .padding(.top, ApproachNoteTheme.spacingXS)
                    }
                    
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
        // No password field here — only the email address, which we capture in
        // session replay so we can see the reset flow.
    }
}

#Preview {
    ForgotPasswordView()
        .environmentObject(AuthenticationManager())
}
