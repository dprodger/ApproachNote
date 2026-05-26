//
//  ResetPasswordView.swift
//  Approach Note
//
//  Created by Dave Rodger on 11/18/25.
//  Password reset screen with token
//

import SwiftUI
import PostHog

struct ResetPasswordView: View {
    @EnvironmentObject var authManager: AuthenticationManager
    @Environment(\.dismiss) var dismiss
    
    let token: String
    
    @State private var newPassword = ""
    @State private var confirmPassword = ""
    @State private var passwordsMatch = true
    @State private var resetSuccess = false
    
    var body: some View {
        NavigationView {
            ScrollView {
                VStack(spacing: 24) {
                    if resetSuccess {
                        // Success state
                        VStack(spacing: 16) {
                            Image(systemName: "checkmark.circle.fill")
                                .font(.system(size: 60))
                                .foregroundColor(.green)
                            
                            Text("Password Reset!")
                                .font(.title2)
                                .fontWeight(.bold)
                                .foregroundColor(ApproachNoteTheme.textPrimary)
                            
                            Text("Your password has been successfully reset. You can now sign in with your new password.")
                                .font(.subheadline)
                                .foregroundColor(.secondary)
                                .multilineTextAlignment(.center)
                            
                            Button(action: {
                                dismiss()
                            }) {
                                Text("Go to Sign In")
                                    .fontWeight(.semibold)
                                    .frame(maxWidth: .infinity)
                                    .padding()
                                    .background(ApproachNoteTheme.brand)
                                    .foregroundColor(.white)
                                    .cornerRadius(10)
                            }
                            .padding(.top, 16)
                        }
                        .padding(.top, 60)
                    } else {
                        // Reset form
                        VStack(spacing: 8) {
                            Text("Create New Password")
                                .font(.largeTitle)
                                .fontWeight(.bold)
                                .foregroundColor(ApproachNoteTheme.textPrimary)
                            
                            Text("Enter your new password below.")
                                .font(.subheadline)
                                .foregroundColor(.secondary)
                                .multilineTextAlignment(.center)
                        }
                        .padding(.top, 40)
                        
                        // New password field
                        VStack(alignment: .leading, spacing: 8) {
                            Text("New Password")
                                .font(.subheadline)
                                .foregroundColor(.secondary)
                            
                            SecureField("At least 8 characters", text: $newPassword)
                                .textInputAutocapitalization(.never)
                                .autocorrectionDisabled()
                                .padding()
                                .background(Color(.systemGray6))
                                .cornerRadius(10)
                                .onChange(of: newPassword) {
                                    checkPasswordsMatch()
                                }
                        }
                        
                        // Confirm password field
                        VStack(alignment: .leading, spacing: 8) {
                            Text("Confirm Password")
                                .font(.subheadline)
                                .foregroundColor(.secondary)
                            
                            SecureField("Re-enter password", text: $confirmPassword)
                                .textInputAutocapitalization(.never)
                                .autocorrectionDisabled()
                                .padding()
                                .background(Color(.systemGray6))
                                .cornerRadius(10)
                                .onChange(of: confirmPassword) {
                                    checkPasswordsMatch()
                                }
                            
                            if !passwordsMatch {
                                Text("Passwords do not match")
                                    .font(.caption)
                                    .foregroundColor(.red)
                            }
                        }
                        
                        // Password requirements
                        VStack(alignment: .leading, spacing: 4) {
                            Text("Password must:")
                                .font(.caption)
                                .foregroundColor(.secondary)
                            Text("• Be at least 8 characters long")
                                .font(.caption)
                                .foregroundColor(newPassword.count >= 8 ? .green : .secondary)
                        }
                        .frame(maxWidth: .infinity, alignment: .leading)
                        .padding(.top, 4)
                        
                        // Error message
                        if let error = authManager.errorMessage {
                            Text(error)
                                .font(.subheadline)
                                .foregroundColor(.red)
                                .multilineTextAlignment(.center)
                                .padding(.horizontal)
                        }
                        
                        // Reset button
                        Button(action: {
                            Task {
                                let success = await authManager.resetPassword(
                                    token: token,
                                    newPassword: newPassword
                                )
                                if success {
                                    resetSuccess = true
                                }
                            }
                        }) {
                            if authManager.isLoading {
                                ProgressView()
                                    .progressViewStyle(CircularProgressViewStyle(tint: .white))
                            } else {
                                Text("Reset Password")
                                    .fontWeight(.semibold)
                            }
                        }
                        .frame(maxWidth: .infinity)
                        .padding()
                        .background(isFormValid ? ApproachNoteTheme.brand : Color.gray)
                        .foregroundColor(.white)
                        .cornerRadius(10)
                        .disabled(!isFormValid || authManager.isLoading)
                        
                        Spacer()
                    }
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

    private func checkPasswordsMatch() {
        passwordsMatch = confirmPassword.isEmpty || newPassword == confirmPassword
    }
    
    private var isFormValid: Bool {
        !newPassword.isEmpty &&
        newPassword.count >= 8 &&
        newPassword == confirmPassword
    }
}

#Preview {
    ResetPasswordView(token: "sample_token")
        .environmentObject(AuthenticationManager())
}
