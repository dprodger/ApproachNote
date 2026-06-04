//
//  AboutView.swift
//  Approach Note
//
//  About screen with splash screen background and visible navigation bar
//

import SwiftUI

struct AboutView: View {
    @State private var queueSize: Int = 0
    @State private var currentSongName: String? = nil
    @State private var progress: ResearchProgress? = nil
    @State private var isLoadingQueue: Bool = true
    @State private var isRefreshing: Bool = false
    @State private var rotationAngle: Double = 0
    @State private var showingOnboarding: Bool = false

    let researchService = ResearchService()

    // Get version from build settings
    private var appVersion: String {
        let version = Bundle.main.infoDictionary?["CFBundleShortVersionString"] as? String ?? "1.0"
        let build = Bundle.main.infoDictionary?["CFBundleVersion"] as? String ?? "1"
        return "Version \(version) (\(build))"
    }
    
    var body: some View {
        ZStack {
            // Background image
            Image("LaunchImage")
                .resizable()
                .scaledToFill()
                .ignoresSafeArea()
            
            // Vignette gradient overlay - darker at top and bottom for toolbar visibility
            LinearGradient(
                gradient: Gradient(colors: [
                    Color.black.opacity(0.75),  // Darker at top for navigation bar
                    Color.black.opacity(0.3),   // Lighter in middle
                    Color.black.opacity(0.85)   // Darkest at bottom for tab bar
                ]),
                startPoint: .top,
                endPoint: .bottom
            )
            .ignoresSafeArea()
            
            // Content
            VStack(spacing: ApproachNoteTheme.spacingLG) {
                Spacer()

                Image("horizontal-white_1")

                Text("Your comprehensive guide to jazz recordings")
                    .font(ApproachNoteTheme.title3())
                    .foregroundColor(.white)
                    .multilineTextAlignment(.center)
                    .padding(.horizontal, 40)
                    .shadow(color: .black.opacity(0.7), radius: 5, x: 0, y: 2)
                    .minimumScaleFactor(0.8)
                
                Spacer()
                
                VStack(spacing: ApproachNoteTheme.spacingSM) {
                    Text("Explore thousands of jazz standards")
                        .font(ApproachNoteTheme.body())
                        .bodyLineSpacing()
                        .foregroundColor(.white)

                    Text("Discover legendary artists and recordings")
                        .font(ApproachNoteTheme.body())
                        .bodyLineSpacing()
                        .foregroundColor(.white)

                    Text("Build your jazz knowledge")
                        .font(ApproachNoteTheme.body())
                        .bodyLineSpacing()
                        .foregroundColor(.white)
                }
                .padding(.horizontal, 40)
                .shadow(color: .black.opacity(0.7), radius: 5, x: 0, y: 2)
                
                Spacer()
                
                // View Tutorial Button
                ApproachNoteButton("View Tutorial", leadingSystemImage: "book.fill") {
                    showingOnboarding = true
                }
                .padding(.horizontal, 40)
                
                Spacer()
                
                // Research Queue Status
                VStack(spacing: ApproachNoteTheme.spacingXS) {
                    if isLoadingQueue && !isRefreshing {
                        ProgressView()
                            .tint(.white)
                    } else {
                        HStack(spacing: ApproachNoteTheme.spacingXS) {
                            Image(systemName: currentSongName != nil ? "arrow.triangle.2.circlepath" : "clock")
                                .foregroundColor(.white.opacity(0.9))
                                .font(ApproachNoteTheme.body())
                                .bodyLineSpacing()
                                .rotationEffect(.degrees(isRefreshing ? rotationAngle : 0))

                            Text("Research Queue: \(queueSize)")
                                .font(ApproachNoteTheme.body())
                                .bodyLineSpacing()
                                .foregroundColor(.white.opacity(0.9))

                            if isRefreshing {
                                ProgressView()
                                    .tint(.white)
                                    .scaleEffect(0.7)
                            }
                        }

                        if let songName = currentSongName {
                            Text("Processing: \(songName)")
                                .font(ApproachNoteTheme.caption())
                                .foregroundColor(.white.opacity(0.9))
                                .fontWeight(.medium)
                                .lineLimit(1)
                                .truncationMode(.tail)

                            // Progress indicator
                            if let progress = progress {
                                VStack(spacing: ApproachNoteTheme.spacingXXS) {
                                    // Phase label with progress count
                                    HStack(spacing: ApproachNoteTheme.spacingXXS) {
                                        Text(progress.phaseLabel)
                                            .font(ApproachNoteTheme.caption())
                                            .foregroundColor(.white.opacity(0.7))
                                            .lineLimit(1)

                                        Text("\(progress.current)/\(progress.total)")
                                            .font(ApproachNoteTheme.caption())
                                            .foregroundColor(.white.opacity(0.9))
                                            .fontWeight(.medium)
                                    }

                                    // Progress bar
                                    ZStack(alignment: .leading) {
                                        // Background track
                                        RoundedRectangle(cornerRadius: 2)
                                            .fill(Color.white.opacity(0.2))
                                            .frame(height: 4)

                                        // Progress fill
                                        RoundedRectangle(cornerRadius: 2)
                                            .fill(Color.white.opacity(0.8))
                                            .frame(width: 200 * progress.progressFraction, height: 4)
                                    }
                                    .frame(width: 200, height: 4)
                                }
                                .padding(.top, ApproachNoteTheme.spacingXXS)
                            }
                        }

                        // Tap to refresh hint
                        Text("Tap to refresh")
                            .font(ApproachNoteTheme.caption())
                            .foregroundColor(.white.opacity(0.5))
                            .padding(.top, 2)
                    }
                }
                .padding(.vertical, ApproachNoteTheme.spacingMD)
                .padding(.horizontal, ApproachNoteTheme.spacingXL)
                .frame(maxWidth: 300)
                .background(
                    RoundedRectangle(cornerRadius: 12)
                        .fill(Color.black.opacity(0.3))
                )
                .overlay(
                    RoundedRectangle(cornerRadius: 12)
                        .stroke(Color.white.opacity(0.2), lineWidth: 1)
                )
                .shadow(color: .black.opacity(0.5), radius: 5, x: 0, y: 2)
                .onTapGesture {
                    Task {
                        await refreshQueueStatus()
                    }
                }
                
                Spacer()
                
                Text(appVersion)
                    .font(ApproachNoteTheme.caption())
                    .foregroundColor(.white.opacity(0.8))

                Text("Written by Dave Rodger")
                    .font(ApproachNoteTheme.caption())
                    .foregroundColor(.white.opacity(0.8))
                    .padding(.bottom, ApproachNoteTheme.spacingXS)

                HStack(spacing: ApproachNoteTheme.spacingXS) {
                    Link("Terms", destination: URL(string: "https://approachnote.com/terms")!)
                    Text("·")
                    Link("Privacy", destination: URL(string: "https://approachnote.com/privacy")!)
                    Text("·")
                    Link("approachnote.com", destination: URL(string: "https://www.approachnote.com")!)
                }
                .font(ApproachNoteTheme.caption())
                .foregroundColor(.white.opacity(0.8))
                .tint(.white.opacity(0.8))
                .padding(.bottom, 40)
            }
            .dynamicTypeSize(...DynamicTypeSize.large)
        }
        .navigationBarTitleDisplayMode(.inline)
        .toolbarBackground(ApproachNoteTheme.brand, for: .navigationBar)
        .toolbarBackground(.visible, for: .navigationBar)
        .toolbarColorScheme(.dark, for: .navigationBar)
        .task {
            await loadQueueStatus()
        }
        .fullScreenCover(isPresented: $showingOnboarding) {
            OnboardingView(isPresented: $showingOnboarding)
        }
    }
    
    private func loadQueueStatus() async {
        if let status = await researchService.fetchQueueStatus() {
            queueSize = status.queueSize
            currentSongName = status.currentSong?.songName
            progress = status.progress
        }
        isLoadingQueue = false
    }
    
    private func refreshQueueStatus() async {
        guard !isRefreshing else { return }
        
        isRefreshing = true
        
        // Start rotation animation
        withAnimation(.linear(duration: 1).repeatForever(autoreverses: false)) {
            rotationAngle = 360
        }
        
        if let status = await researchService.fetchQueueStatus() {
            queueSize = status.queueSize
            currentSongName = status.currentSong?.songName
            progress = status.progress
        }
        
        // Stop animation
        withAnimation(.linear(duration: 0.1)) {
            rotationAngle = 0
        }
        isRefreshing = false
    }
}

#Preview {
    NavigationStack {
        AboutView()
    }
}
