//
//  LoginFormBody.swift
//  Approach Note
//
//  Shared login form used by both LoginView (sheet) and RepertoireLoginPromptView
//  (inline). Owns the email/password fields, all three sign-in buttons
//  (email+password, Google, Apple), the Forgot Password link, the Create
//  Account button, and the associated sheets. Callers supply the surrounding
//  chrome (NavigationView, headers, etc.) and an optional `onAuthenticated`
//  closure used to dismiss the presenting view when appropriate.
//

import SwiftUI
import AuthenticationServices
import PostHog

struct LoginFormBody: View {
    @EnvironmentObject var authManager: AuthenticationManager
    @StateObject private var viewModel = LoginViewModel()

    /// Invoked after a successful authentication (any provider). Modal
    /// presenters pass `{ dismiss() }` here; inline presenters can leave it
    /// nil since their parent reacts to `authManager.isAuthenticated`.
    var onAuthenticated: (() -> Void)? = nil

    var body: some View {
        VStack(spacing: 24) {

            // Google + Apple sign-in buttons. Pinned to the Google asset's
            // 185:44 aspect ratio (rendered at 210x50) so the pair stacks
            // uniformly. Google asset per branding guidelines:
            // developers.google.com/identity/branding-guidelines.
            BrandedGoogleSignInButton(action: {
                Task {
                    let success = await viewModel.signInWithGoogle(using: authManager)
                    if success { onAuthenticated?() }
                }
            })
            .frame(width: 210, height: 50)
            .disabled(authManager.isLoading)

            SignInWithAppleButton(
                .signIn,
                onRequest: { request in
                    request.requestedScopes = [.fullName, .email]
                },
                onCompletion: { result in
                    Task {
                        let success = await authManager.signInWithApple(result)
                        if success { onAuthenticated?() }
                    }
                }
            )
            .signInWithAppleButtonStyle(.black)
            .frame(width: 210, height: 50)
            .cornerRadius(10)
            .disabled(authManager.isLoading)

            // Divider
            orDivider
                .padding(.vertical, 16)
            
            // Email field
            VStack(alignment: .leading, spacing: 8) {
                Text("Email")
                    .font(.subheadline)
                    .foregroundColor(.secondary)

                TextField("your@email.com", text: $viewModel.email)
                    .textInputAutocapitalization(.never)
                    .keyboardType(.emailAddress)
                    .autocorrectionDisabled()
                    .padding()
                    .background(Color(.systemGray6))
                    .cornerRadius(10)
            }

            // Password field
            VStack(alignment: .leading, spacing: 8) {
                Text("Password")
                    .font(.subheadline)
                    .foregroundColor(.secondary)

                SecureField("Enter password", text: $viewModel.password)
                    .padding()
                    .background(Color(.systemGray6))
                    .cornerRadius(10)
            }


            // Forgot password
            HStack {
                Spacer()
                Button("Forgot password?") {
                    viewModel.showingForgotPassword = true
                }
                .font(.subheadline)
                .foregroundColor(ApproachNoteTheme.burgundy)
            }

            // Error message
            if let error = authManager.errorMessage {
                Text(error)
                    .font(.subheadline)
                    .foregroundColor(.red)
                    .multilineTextAlignment(.center)
                    .padding(.horizontal)
            }

            // Email/password Sign In button
            Button(action: {
                Task {
                    let success = await viewModel.signIn(using: authManager)
                    if success { onAuthenticated?() }
                }
            }) {
                if authManager.isLoading {
                    ProgressView()
                        .progressViewStyle(CircularProgressViewStyle(tint: .white))
                } else {
                    Text("Sign In")
                        .fontWeight(.semibold)
                }
            }
            .frame(maxWidth: .infinity)
            .padding()
            .background(viewModel.canSubmit ? ApproachNoteTheme.burgundy : Color.gray)
            .foregroundColor(.white)
            .cornerRadius(10)
            .disabled(!viewModel.canSubmit || authManager.isLoading)

            // Divider
            orDivider
                .padding(.vertical, 8)

            // Create account button
            Button(action: {
                viewModel.showingRegister = true
            }) {
                Text("Create Account")
                    .fontWeight(.semibold)
                    .frame(maxWidth: .infinity)
                    .padding()
                    .background(Color(.systemGray6))
                    .foregroundColor(ApproachNoteTheme.charcoal)
                    .cornerRadius(10)
            }
        }
        .sheet(isPresented: $viewModel.showingRegister) {
            RegisterView()
        }
        .sheet(isPresented: $viewModel.showingForgotPassword) {
            ForgotPasswordView()
        }
        .postHogMask()
    }

    private var orDivider: some View {
        HStack {
            Rectangle()
                .frame(height: 1)
                .foregroundColor(.gray.opacity(0.3))
            Text("or")
                .foregroundColor(.secondary)
                .padding(.horizontal, 8)
            Rectangle()
                .frame(height: 1)
                .foregroundColor(.gray.opacity(0.3))
        }
    }
}

#Preview {
    LoginFormBody()
        .environmentObject(AuthenticationManager())
        .padding()
}
