//
//  RepertoireLoginPromptView.swift
//  Approach Note
//
//  Created by Dave Rodger on 11/14/25.
//  Inline login prompt shown when unauthenticated users access the
//  Repertoire tab. Reuses LoginFormBody for the form itself so the
//  auth surface stays in one place.
//

import SwiftUI

struct RepertoireLoginPromptView: View {
    @EnvironmentObject var authManager: AuthenticationManager

    var body: some View {
        ScrollView {
            VStack(spacing: ApproachNoteTheme.spacingXL) {
                // Header
                VStack(spacing: ApproachNoteTheme.spacingSM) {
                    Text("Sign up or Sign In")
                        .font(.title)
                        .fontWeight(.bold)
                        .foregroundColor(ApproachNoteTheme.textPrimary)

                    Text("To use the repertoire feature, you need to have an account. Create one now, or sign in below.")
                        .font(.subheadline)
                        .foregroundColor(.secondary)
                        .multilineTextAlignment(.center)
                }
                .padding(.top, 32)

                // Inline presenter — the parent reacts to
                // authManager.isAuthenticated flipping, so we don't need
                // an onAuthenticated dismiss callback.
                LoginFormBody()

                Spacer()
            }
            .padding(.horizontal, 32)
        }
    }
}

#Preview {
    RepertoireLoginPromptView()
        .environmentObject(AuthenticationManager())
}
