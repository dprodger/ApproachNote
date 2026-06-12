//
//  NoAlbumArtPlaceholder.swift
//  Approach Note
//
//  The single, standard placeholder for the no-album-art case: a plain
//  outlined card with a centered two-line label. Shared by iOS and Mac,
//  used everywhere a recording is missing cover art (shelf cards, hero
//  artwork) so the treatment stays consistent instead of a mix of
//  disc/note glyphs.
//

import SwiftUI

/// Outlined "Album art / not available" placeholder. Fills its container,
/// so each call site sets the size (shelf card vs. hero artwork) and
/// passes the corner radius that matches the real artwork it stands in for.
struct NoAlbumArtPlaceholder: View {
    var cornerRadius: CGFloat = 12

    var body: some View {
        ZStack {
            RoundedRectangle(cornerRadius: cornerRadius, style: .continuous)
                .fill(ApproachNoteTheme.surface)

            Text("Album art\nnot available")
                .font(ApproachNoteTheme.caption())
                .foregroundColor(ApproachNoteTheme.textSecondary)
                .multilineTextAlignment(.center)
                .lineSpacing(2)
                .minimumScaleFactor(0.7)
                .padding(ApproachNoteTheme.spacingXS)
        }
        // strokeBorder draws fully inside the shape, so an outer .cornerRadius
        // clip at a call site won't shave the hairline.
        .overlay(
            RoundedRectangle(cornerRadius: cornerRadius, style: .continuous)
                .strokeBorder(ApproachNoteTheme.surfaceMuted, lineWidth: 1)
        )
    }
}

#Preview {
    HStack(spacing: 16) {
        NoAlbumArtPlaceholder(cornerRadius: 8)
            .frame(width: 150, height: 150)
        NoAlbumArtPlaceholder()
            .frame(width: 250, height: 250)
    }
    .padding()
    .background(ApproachNoteTheme.background)
}
