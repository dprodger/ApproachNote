//
//  MacOnboardingView.swift
//  Approach Note
//
//  Onboarding flow for first-time Mac users explaining Songs, Recordings, and Releases
//

import SwiftUI

struct MacOnboardingView: View {
    @Binding var isPresented: Bool
    @State private var currentPage = 0

    private let totalPages = 5

    var body: some View {
        VStack(spacing: 0) {
            // Page content
            Group {
                switch currentPage {
                case 0:
                    OnboardingWelcomePage()
                case 1:
                    OnboardingSongPage()
                case 2:
                    OnboardingRecordingPage()
                case 3:
                    OnboardingReleasesPage()
                case 4:
                    OnboardingCompletionPage(onFinish: { isPresented = false })
                default:
                    EmptyView()
                }
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity)

            Divider()

            // Navigation controls
            HStack {
                // Back button
                Button(action: {
                    withAnimation {
                        currentPage -= 1
                    }
                }) {
                    HStack(spacing: 4) {
                        Image(systemName: "chevron.left")
                        Text("Back")
                    }
                }
                .buttonStyle(.plain)
                .foregroundColor(ApproachNoteTheme.textSecondary)
                .opacity(currentPage > 0 ? 1 : 0)
                .disabled(currentPage == 0)

                Spacer()

                // Page indicators
                HStack(spacing: 8) {
                    ForEach(0..<totalPages, id: \.self) { index in
                        Circle()
                            .fill(index == currentPage ? ApproachNoteTheme.brand : ApproachNoteTheme.textSecondary.opacity(0.4))
                            .frame(width: 8, height: 8)
                    }
                }

                Spacer()

                // Next/Skip button
                if currentPage < totalPages - 1 {
                    Button(action: {
                        withAnimation {
                            currentPage += 1
                        }
                    }) {
                        HStack(spacing: 4) {
                            Text("Next")
                            Image(systemName: "chevron.right")
                        }
                    }
                    .buttonStyle(.plain)
                    .foregroundColor(ApproachNoteTheme.brand)
                    .fontWeight(.semibold)
                } else {
                    // Invisible placeholder to balance layout
                    Text("Next")
                        .opacity(0)
                }
            }
            .padding(.horizontal, 24)
            .padding(.vertical, 16)
            .background(ApproachNoteTheme.surface)
        }
        .frame(width: 650, height: 580)
        .background(ApproachNoteTheme.background)
        .toolbar {
            ToolbarItem(placement: .cancellationAction) {
                Button("Skip") {
                    isPresented = false
                }
            }
        }
    }
}

// MARK: - Preview

#Preview {
    MacOnboardingView(isPresented: .constant(true))
}
