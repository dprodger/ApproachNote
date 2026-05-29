//
//  MacLoginView.swift
//  Approach Note
//
//  Login view for macOS with email/password and Google Sign-In support
//

import SwiftUI
import AuthenticationServices
#if canImport(GoogleSignIn)
import GoogleSignIn
import GoogleSignInSwift
#endif

struct MacLoginView: View {
    @EnvironmentObject var authManager: AuthenticationManager
    @Environment(\.dismiss) var dismiss

    @StateObject private var viewModel = LoginViewModel()

    /// Whether this view is presented inline (in Settings) vs as a sheet
    var isInline: Bool = false

    var body: some View {
        VStack(spacing: ApproachNoteTheme.spacingLG) {
            // Header
            VStack(spacing: ApproachNoteTheme.spacingXS) {
                Text("Welcome Back")
                    .font(ApproachNoteTheme.title())
                    .foregroundColor(ApproachNoteTheme.textPrimary)

                Text("Sign in to access your repertoire")
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(.secondary)
            }
            .padding(.top, isInline ? 0 : 20)

            // Google Sign-In button — pre-rendered asset per Google's
            // identity branding guidelines
            // (developers.google.com/identity/branding-guidelines).
            // Native asset size 185x44; Apple button below matches.
            #if canImport(GoogleSignIn)
            BrandedGoogleSignInButton(action: signInWithGoogle)
                .frame(width: 168, height: 40)
                .disabled(authManager.isLoading)
            #else
            // Fallback for when GoogleSignIn is not available
            Button(action: {
                authManager.errorMessage = "Google Sign-In is not available on this platform"
            }) {
                HStack {
                    Image(systemName: "globe")
                    Text("Sign in with Google")
                }
                .frame(maxWidth: .infinity)
            }
            .buttonStyle(.bordered)
            .controlSize(.large)
            .disabled(true)
            #endif

            // Sign in with Apple button — sized to match the Google asset.
            SignInWithAppleButton(
                .signIn,
                onRequest: { request in
                    request.requestedScopes = [.fullName, .email]
                },
                onCompletion: { result in
                    Task {
                        let success = await authManager.signInWithApple(result)
                        if success {
                            dismiss()
                        }
                    }
                }
            )
            .signInWithAppleButtonStyle(.black)
            .frame(width: 168, height: 40)
            .cornerRadius(8)
            .disabled(authManager.isLoading)

            // Divider
            HStack {
                Rectangle()
                    .frame(height: 1)
                    .foregroundColor(.gray.opacity(0.3))
                Text("or")
                    .foregroundColor(.secondary)
                    .font(ApproachNoteTheme.caption())
                    .padding(.horizontal, ApproachNoteTheme.spacingXS)
                Rectangle()
                    .frame(height: 1)
                    .foregroundColor(.gray.opacity(0.3))
            }
            .padding(.vertical, ApproachNoteTheme.spacingXS)

            // Email field
            VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXS) {
                Text("Email")
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(.secondary)

                TextField("your@email.com", text: $viewModel.email)
                    .textFieldStyle(.roundedBorder)
                    .textContentType(.emailAddress)
                    .disableAutocorrection(true)
            }

            // Password field
            VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXS) {
                Text("Password")
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(.secondary)

                SecureField("Enter password", text: $viewModel.password)
                    .textFieldStyle(.roundedBorder)
            }

            // Forgot password link
            HStack {
                Spacer()
                Button("Forgot password?") {
                    viewModel.showingForgotPassword = true
                }
                .buttonStyle(.link)
                .foregroundColor(ApproachNoteTheme.brand)
                .font(ApproachNoteTheme.subheadline())
            }

            // Error message
            if let error = authManager.errorMessage {
                Text(error)
                    .font(ApproachNoteTheme.caption())
                    .foregroundColor(.red)
                    .multilineTextAlignment(.center)
                    .padding(.horizontal)
            }

            // Sign In button
            ApproachNoteButton(
                "Sign In",
                isLoading: authManager.isLoading,
                action: signIn
            )
            .disabled(!viewModel.canSubmit)

            // Divider
            HStack {
                Rectangle()
                    .frame(height: 1)
                    .foregroundColor(.gray.opacity(0.3))
                Text("or")
                    .foregroundColor(.secondary)
                    .font(ApproachNoteTheme.caption())
                Rectangle()
                    .frame(height: 1)
                    .foregroundColor(.gray.opacity(0.3))
            }
            .padding(.vertical, ApproachNoteTheme.spacingXS)

            // Create account button
            ApproachNoteButton("Create Account", style: .secondary) {
                viewModel.showingRegister = true
            }

            if !isInline {
                Spacer()
            }
        }
        .padding(isInline ? 0 : 24)
        .frame(minWidth: 300, maxWidth: 400)
        .sheet(isPresented: $viewModel.showingRegister) {
            MacRegisterView()
                .environmentObject(authManager)
        }
        .sheet(isPresented: $viewModel.showingForgotPassword) {
            MacForgotPasswordView()
                .environmentObject(authManager)
        }
        .onChange(of: authManager.isAuthenticated) { _, isAuthenticated in
            if isAuthenticated && !isInline {
                dismiss()
            }
        }
    }

    private func signIn() {
        Task {
            let success = await viewModel.signIn(using: authManager)
            if success && !isInline {
                dismiss()
            }
        }
    }

    private func signInWithGoogle() {
        Task {
            let success = await viewModel.signInWithGoogle(using: authManager)
            if success && !isInline {
                dismiss()
            }
        }
    }
}

#Preview {
    MacLoginView()
        .environmentObject(AuthenticationManager())
        .frame(width: 400, height: 500)
}
