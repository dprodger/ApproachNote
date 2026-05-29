//
//  MacResearchStatusBanner.swift
//  Approach Note
//
//  Visual indicator showing research queue status for a song (macOS version)
//

import SwiftUI

/// A banner showing the research status of a song with hover-to-reveal helper text
struct MacResearchStatusBanner: View {
    let icon: String
    let iconColor: Color
    let title: String
    let message: String
    let helperText: String
    let isAnimating: Bool

    @State private var isHovering = false

    var body: some View {
        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXXS) {
            HStack(spacing: ApproachNoteTheme.spacingXS) {
                // Animated or static icon
                if isAnimating {
                    Image(systemName: icon)
                        .font(.system(size: 16))
                        .foregroundColor(iconColor)
                        .symbolEffect(.pulse, options: .repeating)
                } else {
                    Image(systemName: icon)
                        .font(.system(size: 16))
                        .foregroundColor(iconColor)
                }

                VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXXS) {
                    Text(title)
                        .font(ApproachNoteTheme.subheadline())
                        .fontWeight(.semibold)
                        .foregroundColor(ApproachNoteTheme.textPrimary)
                    Text(message)
                        .font(ApproachNoteTheme.caption())
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                }

                Spacer()

                // Info icon to indicate hoverable
                Image(systemName: "info.circle")
                    .font(.system(size: 12))
                    .foregroundColor(ApproachNoteTheme.textSecondary)
            }
            .padding(ApproachNoteTheme.spacingXS)
            .background(iconColor.opacity(0.1))
            .cornerRadius(6)
            .onHover { hovering in
                isHovering = hovering
            }
            .help(helperText)

            // Show helper text below when hovering (in addition to system tooltip)
            if isHovering {
                Text(helperText)
                    .font(ApproachNoteTheme.caption())
                    .foregroundColor(ApproachNoteTheme.textSecondary)
                    .padding(.horizontal, ApproachNoteTheme.spacingXS)
                    .transition(.opacity)
            }
        }
        .padding(.top, ApproachNoteTheme.spacingXS)
        .animation(.easeInOut(duration: 0.15), value: isHovering)
    }
}

#Preview("Currently Researching") {
    VStack {
        MacResearchStatusBanner(
            icon: "waveform.circle.fill",
            iconColor: ApproachNoteTheme.brand,
            title: "Researching Now",
            message: "Importing MusicBrainz recordings (3/10)",
            helperText: "We're scouring the internet to learn more about this song... Check back in a while to see what we've found.",
            isAnimating: true
        )
        .padding()
        .frame(width: 400)

        Spacer()
    }
}

#Preview("In Queue") {
    VStack {
        MacResearchStatusBanner(
            icon: "clock.fill",
            iconColor: ApproachNoteTheme.accent,
            title: "In Research Queue",
            message: "Position 3 in queue",
            helperText: "This song is in the queue to get researched... Check back in a while to see what we've found.",
            isAnimating: false
        )
        .padding()
        .frame(width: 400)

        Spacer()
    }
}
