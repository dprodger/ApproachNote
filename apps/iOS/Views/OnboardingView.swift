//
//  OnboardingView.swift
//  Approach Note
//
//  Created by Dave Rodger on 12/4/25.
//  Onboarding flow for first-time users explaining Songs, Recordings, and Releases
//

import SwiftUI

struct OnboardingView: View {
    @Binding var isPresented: Bool
    @State private var currentPage = 0

    private let totalPages = 5

    var body: some View {
        ZStack {
            // Background
            ApproachNoteTheme.background
                .ignoresSafeArea()

            VStack(spacing: 0) {
                // Page content
                TabView(selection: $currentPage) {
                    OnboardingWelcomePage()
                        .tag(0)

                    OnboardingSongPage()
                        .tag(1)

                    OnboardingRecordingPage()
                        .tag(2)

                    OnboardingReleasesPage()
                        .tag(3)

                    OnboardingCompletionPage(onFinish: { isPresented = false })
                        .tag(4)
                }
                .tabViewStyle(.page(indexDisplayMode: .never))
                .animation(.easeInOut, value: currentPage)

                // Navigation controls
                VStack(spacing: ApproachNoteTheme.spacingMD) {
                    // Page indicators
                    HStack(spacing: ApproachNoteTheme.spacingXS) {
                        ForEach(0..<totalPages, id: \.self) { index in
                            Circle()
                                .fill(index == currentPage ? ApproachNoteTheme.brand : ApproachNoteTheme.textSecondary.opacity(0.4))
                                .frame(width: 8, height: 8)
                        }
                    }
                    .padding(.bottom, ApproachNoteTheme.spacingXS)

                    // Navigation buttons
                    HStack {
                        if currentPage > 0 {
                            Button(action: {
                                withAnimation {
                                    currentPage -= 1
                                }
                            }) {
                                HStack {
                                    Image(systemName: "chevron.left")
                                    Text("Back")
                                }
                                .foregroundColor(ApproachNoteTheme.textSecondary)
                            }
                        } else {
                            Spacer()
                                .frame(width: 80)
                        }

                        Spacer()

                        if currentPage < totalPages - 1 {
                            Button(action: {
                                withAnimation {
                                    currentPage += 1
                                }
                            }) {
                                HStack {
                                    Text("Next")
                                    Image(systemName: "chevron.right")
                                }
                                .foregroundColor(ApproachNoteTheme.brand)
                                .fontWeight(.semibold)
                            }
                        } else {
                            Spacer()
                                .frame(width: 80)
                        }
                    }
                    .padding(.horizontal, 32)
                }
                .padding(.bottom, 40)
            }

            // Skip button (top right)
            VStack {
                HStack {
                    Spacer()
                    Button("Skip") {
                        isPresented = false
                    }
                    .foregroundColor(ApproachNoteTheme.textSecondary)
                    .padding()
                }
                Spacer()
            }
        }
    }
}

// MARK: - Preview

#Preview {
    OnboardingView(isPresented: .constant(true))
}
