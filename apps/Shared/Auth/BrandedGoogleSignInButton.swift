//
//  BrandedGoogleSignInButton.swift
//  Approach Note
//
//  Sign in with Google button using Google's pre-rendered button asset
//  ("GoogleGLogo" in each target's Assets.xcassets). Renders the asset
//  at its natural aspect ratio so the on-disk button stays pixel-accurate
//  with Google's identity branding guidelines
//  (developers.google.com/identity/branding-guidelines).
//
//  Used in preference to GoogleSignInSwift's bundled `GoogleSignInButton`,
//  which still ships the older Material-style asset.
//

import SwiftUI

struct BrandedGoogleSignInButton: View {
    let action: () -> Void

    var body: some View {
        Button(action: action) {
            Image("GoogleGLogo")
                .resizable()
                .aspectRatio(contentMode: .fit)
        }
        .buttonStyle(.plain)
    }
}

#Preview {
    BrandedGoogleSignInButton(action: {})
        .frame(height: 50)
        .padding()
        .background(Color(white: 0.95))
}
