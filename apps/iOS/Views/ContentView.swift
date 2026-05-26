// ContentView.swift
// Main tab-based navigation view for the iOS app

import SwiftUI

struct ContentView: View {
    @EnvironmentObject var authManager: AuthenticationManager

    var body: some View {
        TabView {
            SongsListView()
                .tabItem {
                    Label("Songs", systemImage: "music.note.list")
                }

            ArtistsListView()
                .tabItem {
                    Label("Artists", systemImage: "person.2.fill")
                }

            RecordingsListView()
                .tabItem {
                    Label("Recordings", systemImage: "opticaldisc")
                }

            SettingsView()
                .environmentObject(authManager)
                .tabItem {
                    Label("Settings", systemImage: "gearshape.fill")
                }

            AboutView()
                .tabItem {
                    Label("About", systemImage: "info.circle")
                }

        }
        .onAppear {
            // Set up tab bar appearance with opaque background
            let appearance = UITabBarAppearance()
            appearance.configureWithOpaqueBackground()
            appearance.backgroundColor = UIColor(ApproachNoteTheme.background)

            // Set unselected item color (light gray)
            appearance.stackedLayoutAppearance.normal.iconColor = UIColor.lightGray
            appearance.stackedLayoutAppearance.normal.titleTextAttributes = [.foregroundColor: UIColor.lightGray]

            // Set selected item color (burgundy)
            appearance.stackedLayoutAppearance.selected.iconColor = UIColor(ApproachNoteTheme.brand)
            appearance.stackedLayoutAppearance.selected.titleTextAttributes = [.foregroundColor: UIColor(ApproachNoteTheme.brand)]

            UITabBar.appearance().standardAppearance = appearance
            UITabBar.appearance().scrollEdgeAppearance = appearance
        }
        .tint(ApproachNoteTheme.brand) // Sets the active tab color
    }
}

#Preview {
    ContentView()
        .environmentObject(AuthenticationManager())
        .environmentObject(RepertoireManager())
        .environmentObject(FavoritesManager())
}
