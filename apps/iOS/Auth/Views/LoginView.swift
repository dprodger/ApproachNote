//
//  LoginView.swift
//  Approach Note
//
//  Created by Dave Rodger on 11/14/25.
//  Sheet-style login entry point (used from flows like YouTube import).
//  The form itself lives in LoginFormBody; this view supplies the
//  NavigationView chrome, the generic "Welcome Back" header, and the
//  Close toolbar button.
//

import SwiftUI

struct LoginView: View {
    @EnvironmentObject var authManager: AuthenticationManager
    @Environment(\.dismiss) var dismiss

    var body: some View {
        NavigationView {
            ScrollView {
                VStack(spacing: ApproachNoteTheme.spacingXL) {
                    // Header
                    VStack(spacing: ApproachNoteTheme.spacingXS) {
                        Text("Welcome Back")
                            .font(.largeTitle)
                            .fontWeight(.bold)
                            .foregroundColor(ApproachNoteTheme.textPrimary)

                        Text("Sign in to access your repertoire")
                            .font(.subheadline)
                            .foregroundColor(.secondary)
                    }
                    .padding(.top, 40)

                    LoginFormBody(onAuthenticated: { dismiss() })

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
            .onChange(of: authManager.isAuthenticated) {
                if authManager.isAuthenticated {
                    dismiss()
                }
            }
        }
    }
}

#Preview {
    LoginView()
        .environmentObject(AuthenticationManager())
}
