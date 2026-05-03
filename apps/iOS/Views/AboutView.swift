//
//  AboutView.swift
//  Approach Note
//
//  About screen with splash screen background and visible navigation bar
//

import SwiftUI

struct AboutView: View {
    @State private var queueSize: Int = 0
    @State private var workerActive: Bool = false
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
            VStack(spacing: 20) {
                Spacer()

                Text("ApproachNote")
                    .font(ApproachNoteTheme.largeTitle(size: 48))
                    .foregroundColor(.white)
                    .shadow(color: .black.opacity(0.7), radius: 10, x: 0, y: 5)
                    .minimumScaleFactor(0.7)
                    .lineLimit(1)

                Text("Your comprehensive guide to jazz recordings")
                    .font(ApproachNoteTheme.title3())
                    .foregroundColor(.white)
                    .multilineTextAlignment(.center)
                    .padding(.horizontal, 40)
                    .shadow(color: .black.opacity(0.7), radius: 5, x: 0, y: 2)
                    .minimumScaleFactor(0.8)
                
                Spacer()
                
                VStack(spacing: 12) {
                    Text("Explore thousands of jazz standards")
                        .font(ApproachNoteTheme.body())
                        .foregroundColor(.white)

                    Text("Discover legendary artists and recordings")
                        .font(ApproachNoteTheme.body())
                        .foregroundColor(.white)

                    Text("Build your jazz knowledge")
                        .font(ApproachNoteTheme.body())
                        .foregroundColor(.white)
                }
                .padding(.horizontal, 40)
                .shadow(color: .black.opacity(0.7), radius: 5, x: 0, y: 2)
                
                Spacer()
                
                // View Tutorial Button
                Button(action: {
                    showingOnboarding = true
                }) {
                    HStack {
                        Image(systemName: "book.fill")
                        Text("View Tutorial")
                    }
                    .font(ApproachNoteTheme.headline())
                    .foregroundColor(ApproachNoteTheme.burgundy)
                    .padding(.horizontal, 24)
                    .padding(.vertical, 12)
                    .background(
                        RoundedRectangle(cornerRadius: 10)
                            .fill(Color.white.opacity(0.95))
                    )
                    .shadow(color: .black.opacity(0.3), radius: 4, x: 0, y: 2)
                }
                
                Spacer()
                
                // Research Queue Status
                VStack(spacing: 8) {
                    if isLoadingQueue && !isRefreshing {
                        ProgressView()
                            .tint(.white)
                    } else {
                        HStack(spacing: 6) {
                            Image(systemName: workerActive ? "arrow.triangle.2.circlepath" : "clock")
                                .foregroundColor(.white.opacity(0.9))
                                .font(ApproachNoteTheme.body())
                                .rotationEffect(.degrees(isRefreshing ? rotationAngle : 0))

                            Text("Research Queue: \(queueSize)")
                                .font(ApproachNoteTheme.body())
                                .foregroundColor(.white.opacity(0.9))

                            if isRefreshing {
                                ProgressView()
                                    .tint(.white)
                                    .scaleEffect(0.7)
                            }
                        }

                        if workerActive {
                            if let songName = currentSongName {
                                Text("Processing: \(songName)")
                                    .font(ApproachNoteTheme.caption())
                                    .foregroundColor(.white.opacity(0.9))
                                    .fontWeight(.medium)
                                    .lineLimit(1)
                                    .truncationMode(.tail)
                            } else {
                                Text("Processing...")
                                    .font(ApproachNoteTheme.caption())
                                    .foregroundColor(.white.opacity(0.7))
                                    .italic()
                            }

                            // Progress indicator
                            if let progress = progress {
                                VStack(spacing: 4) {
                                    // Phase label with progress count
                                    HStack(spacing: 4) {
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
                                .padding(.top, 4)
                            }
                        }

                        // Tap to refresh hint
                        Text("Tap to refresh")
                            .font(ApproachNoteTheme.caption())
                            .foregroundColor(.white.opacity(0.5))
                            .padding(.top, 2)
                    }
                }
                .padding(.vertical, 16)
                .padding(.horizontal, 24)
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
                    .padding(.bottom, 10)

                HStack(spacing: 10) {
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
        .toolbarBackground(ApproachNoteTheme.burgundy, for: .navigationBar)
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
            workerActive = status.workerActive
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
            workerActive = status.workerActive
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
