// ContentView.swift
// Main tab-based navigation view for the iOS app

import SwiftUI

struct ContentView: View {
    @EnvironmentObject var authManager: AuthenticationManager

    /// iPad floats the tab bar with a light selection pill; iPhone draws it as a
    /// solid bar. The selected-tab colour flips between the two (see below).
    static var isPad: Bool { UIDevice.current.userInterfaceIdiom == .pad }

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
            // Opaque brand tab bar so the bar reads as a solid brand band
            // (matching the brand list/detail headers) rather than cream or glass.
            let appearance = UITabBarAppearance()
            appearance.configureWithOpaqueBackground()
            appearance.backgroundColor = UIColor(ApproachNoteTheme.brand)

            // Selected-state colour differs by idiom: on iPad the tab bar floats
            // with a light selection pill, so the selected item must be DARK
            // (brand) to read; on iPhone the selected item sits directly on the
            // opaque brand bar, so it must be WHITE. Unselected is dimmed white,
            // legible on the brand bar.
            let normal = UIColor(ApproachNoteTheme.textOnDark).withAlphaComponent(0.6)
            let selected = ContentView.isPad
                ? UIColor(ApproachNoteTheme.brand)
                : UIColor(ApproachNoteTheme.textOnDark)
            for item in [appearance.stackedLayoutAppearance,
                         appearance.inlineLayoutAppearance,
                         appearance.compactInlineLayoutAppearance] {
                item.normal.iconColor = normal
                item.normal.titleTextAttributes = [.foregroundColor: normal]
                item.selected.iconColor = selected
                item.selected.titleTextAttributes = [.foregroundColor: selected]
            }

            UITabBar.appearance().standardAppearance = appearance
            UITabBar.appearance().scrollEdgeAppearance = appearance
        }
        // Selected tab tint: dark (brand) on iPad's light pill, white on the
        // iPhone brand bar. Matches the appearance proxy above.
        .tint(ContentView.isPad ? ApproachNoteTheme.brand : ApproachNoteTheme.textOnDark)
    }
}

#Preview {
    ContentView()
        .environmentObject(AuthenticationManager())
        .environmentObject(RepertoireManager())
        .environmentObject(FavoritesManager())
}
