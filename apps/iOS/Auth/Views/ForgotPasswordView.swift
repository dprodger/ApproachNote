//
//  ForgotPasswordView.swift
//  Approach Note
//
//  Created by Dave Rodger on 11/14/25.
//  Password reset request screen
//

import SwiftUI
import PostHog

struct ForgotPasswordView: View {
    @EnvironmentObject var authManager: AuthenticationManager
    @Environment(\.dismiss) var dismiss
    
    @State private var email = ""
    @State private var resetEmailSent = false
    
    var body: some View {
        NavigationView {
            ScrollView {
                VStack(spacing: 24) {
                    if resetEmailSent {
                        // Success state
                        VStack(spacing: 16) {
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
                            
                            Text(email)
                                .font(.subheadline)
                                .fontWeight(.semibold)
                                .foregroundColor(ApproachNoteTheme.textPrimary)
                            
                            Text("Please check your email and follow the link to reset your password.")
                                .font(.subheadline)
                                .foregroundColor(.secondary)
                                .multilineTextAlignment(.center)
                                .padding(.top, 8)
                            
                            Button(action: {
                                dismiss()
                            }) {
                                Text("Done")
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
                        // Request form
                        VStack(spacing: 8) {
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
                        
                        // Error message
                        if let error = authManager.errorMessage {
                            Text(error)
                                .font(.subheadline)
                                .foregroundColor(.red)
                                .multilineTextAlignment(.center)
                                .padding(.horizontal)
                        }
                        
                        // Send button
                        Button(action: {
                            Task {
                                let success = await authManager.requestPasswordReset(
                                    email: email.trimmingCharacters(in: .whitespacesAndNewlines)
                                )
                                if success {
                                    resetEmailSent = true
                                }
                            }
                        }) {
                            if authManager.isLoading {
                                ProgressView()
                                    .progressViewStyle(CircularProgressViewStyle(tint: .white))
                            } else {
                                Text("Send Reset Link")
                                    .fontWeight(.semibold)
                            }
                        }
                        .frame(maxWidth: .infinity)
                        .padding()
                        .background(email.isEmpty ? Color.gray : ApproachNoteTheme.brand)
                        .foregroundColor(.white)
                        .cornerRadius(10)
                        .disabled(email.isEmpty || authManager.isLoading)
                        
                        // Back to login
                        Button(action: {
                            dismiss()
                        }) {
                            Text("Back to Sign In")
                                .font(.subheadline)
                                .foregroundColor(ApproachNoteTheme.brand)
                        }
                        .padding(.top, 8)
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
        .postHogMask()
    }
}

#Preview {
    ForgotPasswordView()
        .environmentObject(AuthenticationManager())
}
