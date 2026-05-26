//
//  ResearchStatusBanner.swift
//  Approach Note
//
//  Visual indicator showing research queue status for a song
//

import SwiftUI

/// A banner showing the research status of a song with tap-to-reveal helper text
struct ResearchStatusBanner: View {
    let icon: String
    let iconColor: Color
    let title: String
    let message: String
    let helperText: String
    let isAnimating: Bool

    @State private var showHelperText = false

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            // Main banner - tappable
            Button(action: {
                withAnimation(.easeInOut(duration: 0.2)) {
                    showHelperText.toggle()
                }
            }) {
                HStack(spacing: 10) {
                    // Animated or static icon
                    if isAnimating {
                        Image(systemName: icon)
                            .font(.system(size: 18))
                            .foregroundColor(iconColor)
                            .symbolEffect(.pulse, options: .repeating)
                    } else {
                        Image(systemName: icon)
                            .font(.system(size: 18))
                            .foregroundColor(iconColor)
                    }

                    VStack(alignment: .leading, spacing: 2) {
                        Text(title)
                            .font(ApproachNoteTheme.subheadline())
                            .fontWeight(.semibold)
                            .foregroundColor(ApproachNoteTheme.textPrimary)
                        Text(message)
                            .font(ApproachNoteTheme.caption())
                            .foregroundColor(ApproachNoteTheme.textSecondary)
                    }

                    Spacer()

                    // Chevron to indicate expandable
                    Image(systemName: showHelperText ? "chevron.up" : "chevron.down")
                        .font(.system(size: 12))
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                }
                .padding(12)
                .background(iconColor.opacity(0.1))
                .cornerRadius(8)
            }
            .buttonStyle(.plain)

            // Expandable helper text
            if showHelperText {
                Text(helperText)
                    .font(ApproachNoteTheme.caption())
                    .foregroundColor(ApproachNoteTheme.textSecondary)
                    .padding(.horizontal, 12)
                    .padding(.bottom, 4)
                    .transition(.opacity.combined(with: .move(edge: .top)))
            }
        }
        .padding(.top, 8)
    }
}

#Preview("Currently Researching") {
    VStack {
        ResearchStatusBanner(
            icon: "waveform.circle.fill",
            iconColor: ApproachNoteTheme.brand,
            title: "Researching Now",
            message: "Importing MusicBrainz recordings (3/10)",
            helperText: "We're scouring the internet to learn more about this song... Check back in a while to see what we've found.",
            isAnimating: true
        )
        .padding()

        Spacer()
    }
}

#Preview("In Queue") {
    VStack {
        ResearchStatusBanner(
            icon: "clock.fill",
            iconColor: ApproachNoteTheme.accent,
            title: "In Research Queue",
            message: "Position 3 in queue",
            helperText: "This song is in the queue to get researched... Check back in a while to see what we've found.",
            isAnimating: false
        )
        .padding()

        Spacer()
    }
}
