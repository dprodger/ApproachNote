//
//  ApproachNoteTheme.swift
//  Approach Note
//
//  Centralized color theme inspired by jazz nightclubs and speakeasies
//

import SwiftUI
#if os(iOS)
import UIKit
#endif

struct ApproachNoteTheme {
    // MARK: - Typography

    /// Font family for headings (largeTitle, title, title2, title3, headline)
    /// Options: "Libre Franklin", "Futura", "Avenir", "Helvetica Neue", "Gill Sans", "Optima", "Baskerville"
    static let headingFontFamily = "Libre Franklin"

    /// Font family for body text (body, callout, subheadline, footnote, caption)
    /// Options: "Libre Franklin", "Baskerville", "Georgia", "Palatino", "Didot", "Cochin", "Charter", "Avenir", "Futura"
    static let bodyFontFamily = "Libre Franklin"

    // MARK: - Heading Fonts

    /// Font for large titles (e.g., screen titles, hero text)
    static func largeTitle(size: CGFloat = 32, weight: Font.Weight = .bold) -> Font {
        .custom(headingFontName(for: weight), size: size)
    }

    /// Font for titles (e.g., section headers, card titles)
    static func title(size: CGFloat = 29, weight: Font.Weight = .bold) -> Font {
        .custom(headingFontName(for: weight), size: size)
    }

    /// Font for titles level 2
    static func title2(size: CGFloat = 23, weight: Font.Weight = .semibold) -> Font {
        .custom(headingFontName(for: weight), size: size)
    }

    /// Font for titles level 3
    static func title3(size: CGFloat = 20, weight: Font.Weight = .semibold) -> Font {
        .custom(headingFontName(for: weight), size: size)
    }

    /// Font for headlines
    static func headline(size: CGFloat = 18, weight: Font.Weight = .semibold) -> Font {
        .custom(headingFontName(for: weight), size: size)
    }

    // MARK: - Body Fonts

    /// Font for body text
    static func body(size: CGFloat = 16, weight: Font.Weight = .regular, italic: Bool = false) -> Font {
        .custom(bodyFontName(for: weight, italic: italic), size: size)
    }

    /// Font for callouts
    static func callout(size: CGFloat = 16, weight: Font.Weight = .regular, italic: Bool = false) -> Font {
        .custom(bodyFontName(for: weight, italic: italic), size: size)
    }

    /// Font for subheadlines
    static func subheadline(size: CGFloat = 14, weight: Font.Weight = .regular, italic: Bool = false) -> Font {
        .custom(bodyFontName(for: weight, italic: italic), size: size)
    }

    /// Font for footnotes
    static func footnote(size: CGFloat = 13, weight: Font.Weight = .regular, italic: Bool = false) -> Font {
        .custom(bodyFontName(for: weight, italic: italic), size: size)
    }

    /// Font for captions
    static func caption(size: CGFloat = 12, weight: Font.Weight = .regular, italic: Bool = false) -> Font {
        .custom(bodyFontName(for: weight, italic: italic), size: size)
    }

    /// Font for smaller captions
    static func caption2(size: CGFloat = 11, weight: Font.Weight = .regular, italic: Bool = false) -> Font {
        .custom(bodyFontName(for: weight, italic: italic), size: size)
    }

    // MARK: - Font Name Helpers

    /// Helper to get the correct heading font name variant for the weight
    private static func headingFontName(for weight: Font.Weight) -> String {
        switch headingFontFamily {
        case "Libre Franklin":
            switch weight {
            case .black, .heavy: return "LibreFranklin-Black"
            case .bold: return "LibreFranklin-Bold"
            case .semibold: return "LibreFranklin-SemiBold"
            case .medium: return "LibreFranklin-Medium"
            case .light, .ultraLight, .thin: return "LibreFranklin-Light"
            default: return "LibreFranklin-Regular"
            }
        case "Futura":
            switch weight {
            case .bold, .heavy, .black: return "Futura-Bold"
            case .semibold, .medium: return "Futura-Medium"
            case .light, .ultraLight, .thin: return "Futura-Medium"
            default: return "Futura-Medium"
            }
        case "Avenir":
            switch weight {
            case .bold, .heavy, .black: return "Avenir-Black"
            case .semibold, .medium: return "Avenir-Medium"
            case .light, .ultraLight, .thin: return "Avenir-Light"
            default: return "Avenir-Book"
            }
        case "Helvetica Neue":
            switch weight {
            case .bold, .heavy, .black: return "HelveticaNeue-Bold"
            case .semibold, .medium: return "HelveticaNeue-Medium"
            case .light, .ultraLight, .thin: return "HelveticaNeue-Light"
            default: return "HelveticaNeue"
            }
        case "Gill Sans":
            switch weight {
            case .bold, .heavy, .black: return "GillSans-Bold"
            case .semibold, .medium: return "GillSans-SemiBold"
            case .light, .ultraLight, .thin: return "GillSans-Light"
            default: return "GillSans"
            }
        case "Optima":
            switch weight {
            case .bold, .heavy, .black: return "Optima-Bold"
            case .semibold, .medium: return "Optima-Regular"
            default: return "Optima-Regular"
            }
        case "Baskerville":
            switch weight {
            case .bold, .heavy, .black: return "Baskerville-Bold"
            case .semibold, .medium: return "Baskerville-SemiBold"
            case .light, .ultraLight, .thin: return "Baskerville"
            default: return "Baskerville"
            }
        default:
            return headingFontFamily
        }
    }

    /// Helper to get the correct body font name variant for the weight
    /// Only LibreFranklin-Italic (Regular weight) is bundled today; italic + non-regular
    /// weight falls back to the upright weight name (no slant) rather than silently
    /// returning the wrong glyph set.
    private static func bodyFontName(for weight: Font.Weight, italic: Bool = false) -> String {
        switch bodyFontFamily {
        case "Libre Franklin":
            if italic && (weight == .regular) {
                return "LibreFranklin-Italic"
            }
            switch weight {
            case .black, .heavy: return "LibreFranklin-Black"
            case .bold: return "LibreFranklin-Bold"
            case .semibold: return "LibreFranklin-SemiBold"
            case .medium: return "LibreFranklin-Medium"
            case .light, .ultraLight, .thin: return "LibreFranklin-Light"
            default: return "LibreFranklin-Regular"
            }
        case "Futura":
            switch weight {
            case .bold, .heavy, .black: return "Futura-Bold"
            case .semibold, .medium: return "Futura-Medium"
            case .light, .ultraLight, .thin: return "Futura-Medium"
            default: return "Futura-Medium"
            }
        case "Baskerville":
            switch weight {
            case .bold, .heavy, .black: return "Baskerville-Bold"
            case .semibold, .medium: return "Baskerville-SemiBold"
            case .light, .ultraLight, .thin: return "Baskerville"
            default: return "Baskerville"
            }
        case "Georgia":
            switch weight {
            case .bold, .heavy, .black, .semibold: return "Georgia-Bold"
            case .light, .ultraLight, .thin: return "Georgia"
            default: return "Georgia"
            }
        case "Palatino":
            switch weight {
            case .bold, .heavy, .black, .semibold: return "Palatino-Bold"
            case .light, .ultraLight, .thin: return "Palatino-Roman"
            default: return "Palatino-Roman"
            }
        case "Didot":
            switch weight {
            case .bold, .heavy, .black, .semibold: return "Didot-Bold"
            default: return "Didot"
            }
        case "Cochin":
            switch weight {
            case .bold, .heavy, .black, .semibold: return "Cochin-Bold"
            default: return "Cochin"
            }
        default:
            return bodyFontFamily
        }
    }

    #if os(iOS)
    // MARK: - UIKit Font Helpers

    /// Returns a UIFont for the heading style (for UIKit components like navigation bars)
    static func uiHeadingFont(size: CGFloat, weight: UIFont.Weight = .bold) -> UIFont {
        let fontWeight: Font.Weight = {
            switch weight {
            case .bold, .heavy, .black: return .bold
            case .semibold, .medium: return .semibold
            case .light, .ultraLight, .thin: return .light
            default: return .regular
            }
        }()
        let fontName = headingFontName(for: fontWeight)
        if let font = UIFont(name: fontName, size: size) {
            return font
        } else {
            return UIFont.systemFont(ofSize: size, weight: weight)
        }
    }

    /// Returns a UIFont for the body style (for UIKit components)
    static func uiBodyFont(size: CGFloat, weight: UIFont.Weight = .regular) -> UIFont {
        let fontWeight: Font.Weight = {
            switch weight {
            case .bold, .heavy, .black: return .bold
            case .semibold, .medium: return .semibold
            case .light, .ultraLight, .thin: return .light
            default: return .regular
            }
        }()
        let fontName = bodyFontName(for: fontWeight)
        return UIFont(name: fontName, size: size) ?? UIFont.systemFont(ofSize: size, weight: weight)
    }

    // MARK: - Navigation Bar Appearance

    /// Creates a configured UINavigationBarAppearance with ApproachNoteTheme fonts
    static func navigationBarAppearance() -> UINavigationBarAppearance {
        let appearance = UINavigationBarAppearance()
        appearance.configureWithOpaqueBackground()
        appearance.backgroundColor = UIColor(brand)

        // Large title font (used when scrolled to top)
        appearance.largeTitleTextAttributes = [
            .font: uiHeadingFont(size: 32, weight: .bold),
            .foregroundColor: UIColor(textOnDark)
        ]

        // Inline title font (used when scrolled or in compact mode)
        appearance.titleTextAttributes = [
            .font: uiHeadingFont(size: 18, weight: .semibold),
            .foregroundColor: UIColor(textOnDark)
        ]

        return appearance
    }

    /// Configures the navigation bar appearance to use ApproachNoteTheme fonts
    /// Call this once at app startup (e.g., in App init or ContentView.onAppear)
    static func configureNavigationBarAppearance() {
        let appearance = navigationBarAppearance()
        UINavigationBar.appearance().standardAppearance = appearance
        UINavigationBar.appearance().scrollEdgeAppearance = appearance
        UINavigationBar.appearance().compactAppearance = appearance
    }
    #endif
}

#if os(iOS)
// MARK: - Navigation Bar Styling (iOS)

/// Helper view that finds and configures the parent UINavigationController
struct NavigationBarConfigurator: UIViewControllerRepresentable {
    func makeUIViewController(context: Context) -> UIViewController {
        let controller = UIViewController()
        return controller
    }

    func updateUIViewController(_ uiViewController: UIViewController, context: Context) {
        DispatchQueue.main.async {
            if let navController = uiViewController.navigationController {
                let appearance = ApproachNoteTheme.navigationBarAppearance()
                navController.navigationBar.standardAppearance = appearance
                navController.navigationBar.scrollEdgeAppearance = appearance
                navController.navigationBar.compactAppearance = appearance
            }
        }
    }
}

/// Custom navigation title view with ApproachNoteTheme fonts
struct JazzNavigationTitle: View {
    let title: String

    var body: some View {
        Text(title)
            .font(ApproachNoteTheme.headline())
            .foregroundColor(.white)
    }
}

/// Large navigation title for scroll edge (top of screen)
struct JazzLargeNavigationTitle: View {
    let title: String

    var body: some View {
        Text(title)
            .font(ApproachNoteTheme.largeTitle())
            .foregroundColor(ApproachNoteTheme.textOnDark)
    }
}

extension View {
    /// Applies ApproachNoteTheme styling to the navigation bar with custom title font.
    /// Use this instead of `.navigationTitle()` for themed headers.
    /// - Parameters:
    ///   - title: The navigation bar title.
    ///   - color: Background color (defaults to brand). Override only for special cases —
    ///     under the new semantic theme nearly all nav bars should use `.brand`.
    func jazzNavigationBar(title: String, color: Color = ApproachNoteTheme.brand) -> some View {
        self
            .navigationBarTitleDisplayMode(.inline)
            .toolbarBackground(color, for: .navigationBar)
            .toolbarBackground(.visible, for: .navigationBar)
            .toolbarColorScheme(.dark, for: .navigationBar)
            .toolbar {
                ToolbarItem(placement: .principal) {
                    Text(title)
                        .font(ApproachNoteTheme.headline())
                        .foregroundColor(ApproachNoteTheme.textOnDark)
                        .lineLimit(1)
                        .truncationMode(.tail)
                }
            }
    }
}
#endif

// MARK: - Semantic Color Tokens
//
// Colors are organized by *role*, not pigment. Pick the token that matches
// what the color is doing semantically — don't pick by hue.
//
// Roles:
//   - Surface:   background, surface, surfaceMuted
//   - Text:      textPrimary, textSecondary, textTertiary, textOnDark, textOnAccent
//   - Brand:     brand (navigation chrome, app identity)
//   - Accent:    accent, accentMuted, accentBackground (interactive, links, selection)
//   - Status:    warning, warningBackground (alerts, destructive actions)

extension ApproachNoteTheme {

    // MARK: - Brand

    /// Brand identity color. Navigation chrome, section headers, app identity.
    /// Deep blue (#363A87). Shares its hex with `accent` — same hue serves two
    /// roles (filled surface vs. interactive foreground) and is intentionally
    /// kept as separate tokens so they can diverge later without a rename pass.
    static let brand = Color(red: 0.212, green: 0.227, blue: 0.529)

    // MARK: - Surfaces

    /// App background. Off-white (#FFFCF7).
    static let background = Color(red: 1.0, green: 0.988, blue: 0.969)

    /// Elevated surface — cards, sheets, modals. White (#FFFFFF).
    static let surface = Color.white

    /// Recessed / grouped surface. Light gray (#D9D7D7). Also useful for divider fills.
    static let surfaceMuted = Color(red: 0.851, green: 0.843, blue: 0.843)

    // MARK: - Text

    /// Primary text on light surfaces. Warm near-black (#413737).
    static let textPrimary = Color(red: 0.255, green: 0.216, blue: 0.216)

    /// Secondary text — captions, supporting copy, metadata. (#675F5F)
    static let textSecondary = Color(red: 0.404, green: 0.373, blue: 0.373)

    /// Tertiary text — disabled, hint, placeholder. (#B3AFAF)
    static let textTertiary = Color(red: 0.702, green: 0.686, blue: 0.686)

    /// Text/icons on dark surfaces (brand chrome, nav bar titles). Off-white (#FFFCF7).
    static let textOnDark = Color(red: 1.0, green: 0.988, blue: 0.969)

    /// Text/icons on filled accent buttons. Pure white.
    static let textOnAccent = Color.white

    // MARK: - Accent (interactive)

    /// Primary interactive color — links, action buttons, selection, ratings. Blue (#363A87).
    static let accent = Color(red: 0.212, green: 0.227, blue: 0.529)

    /// Pressed / hover state of accent. Slightly lighter blue (#5D619F).
    static let accentMuted = Color(red: 0.365, green: 0.380, blue: 0.624)

    /// Tinted background for accent-themed regions (e.g. selected row, info panels). (#ECEDFF)
    static let accentBackground = Color(red: 0.925, green: 0.929, blue: 1.0)

    // MARK: - Status

    /// Warnings, alerts, destructive actions. Red (#FF3A4E).
    static let warning = Color(red: 1.0, green: 0.227, blue: 0.306)

    /// Tinted background for warning regions. (#FFEBED)
    static let warningBackground = Color(red: 1.0, green: 0.922, blue: 0.929)

}

// MARK: - Section Headers
//
// Typography-only — uppercase bold title in `textPrimary` on the body
// background. No chrome band, no icon. Optional subtitle in `textSecondary`.

struct ThemedSectionHeader: View {
    let title: String
    let subtitle: String?

    init(_ title: String, subtitle: String? = nil) {
        self.title = title
        self.subtitle = subtitle
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            Text(title.uppercased())
                .font(ApproachNoteTheme.headline())
                .fontWeight(.bold)
                .foregroundColor(ApproachNoteTheme.textPrimary)
            if let subtitle {
                Text(subtitle)
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(ApproachNoteTheme.textSecondary)
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(.horizontal)
        .padding(.top, 24)
        .padding(.bottom, 8)
    }
}

// MARK: - Themed Progress View

/// A progress view with consistent ApproachNoteTheme styling.
/// Use this for all loading indicators to ensure consistent typography.
struct ThemedProgressView: View {
    let message: String
    var tintColor: Color = ApproachNoteTheme.accent

    var body: some View {
        ProgressView {
            Text(message)
                .font(ApproachNoteTheme.subheadline())
                .foregroundColor(ApproachNoteTheme.textPrimary)
        }
        .tint(tintColor)
    }
}

// MARK: - Preview

struct ApproachNoteThemePreview: View {
    var body: some View {
        ScrollView {
            VStack(spacing: 20) {
                // Section header demonstrations — typography only on body background.
                VStack(alignment: .leading, spacing: 0) {
                    ThemedSectionHeader("Featured Recordings",
                                        subtitle: "Take a look at these important recordings for this song.")
                    ThemedSectionHeader("Performers")
                    ThemedSectionHeader("About the Song")
                }
                .frame(maxWidth: .infinity)

                // Color swatches grouped by role.
                VStack(alignment: .leading, spacing: 16) {
                    Text("Brand & Surfaces")
                        .font(.title2).bold()
                    ColorSwatch(name: "brand", color: ApproachNoteTheme.brand)
                    ColorSwatch(name: "background", color: ApproachNoteTheme.background)
                    ColorSwatch(name: "surface", color: ApproachNoteTheme.surface)
                    ColorSwatch(name: "surfaceMuted", color: ApproachNoteTheme.surfaceMuted)

                    Text("Text")
                        .font(.title2).bold()
                        .padding(.top, 8)
                    ColorSwatch(name: "textPrimary", color: ApproachNoteTheme.textPrimary)
                    ColorSwatch(name: "textSecondary", color: ApproachNoteTheme.textSecondary)
                    ColorSwatch(name: "textTertiary", color: ApproachNoteTheme.textTertiary)
                    ColorSwatch(name: "textOnDark", color: ApproachNoteTheme.textOnDark, darkBackground: true)
                    ColorSwatch(name: "textOnAccent", color: ApproachNoteTheme.textOnAccent, darkBackground: true)

                    Text("Accent")
                        .font(.title2).bold()
                        .padding(.top, 8)
                    ColorSwatch(name: "accent", color: ApproachNoteTheme.accent)
                    ColorSwatch(name: "accentMuted", color: ApproachNoteTheme.accentMuted)
                    ColorSwatch(name: "accentBackground", color: ApproachNoteTheme.accentBackground)

                    Text("Status")
                        .font(.title2).bold()
                        .padding(.top, 8)
                    ColorSwatch(name: "warning", color: ApproachNoteTheme.warning)
                    ColorSwatch(name: "warningBackground", color: ApproachNoteTheme.warningBackground)
                }
                .padding()
            }
        }
        .background(ApproachNoteTheme.background)
    }

}

struct ColorSwatch: View {
    let name: String
    let color: Color
    var darkBackground: Bool = false

    var body: some View {
        HStack {
            RoundedRectangle(cornerRadius: 8)
                .fill(color)
                .frame(width: 60, height: 60)
                .overlay(
                    RoundedRectangle(cornerRadius: 8)
                        .stroke(Color.gray.opacity(0.3), lineWidth: 1)
                )
                .background(
                    // Show light/dark backdrop behind on-dark/on-accent swatches so they're visible.
                    darkBackground
                        ? RoundedRectangle(cornerRadius: 8).fill(ApproachNoteTheme.brand)
                        : nil
                )

            VStack(alignment: .leading) {
                Text(name)
                    .font(.headline)
                Text(darkBackground ? "For dark / accent backgrounds" : "Primary use")
                    .font(.caption)
                    .foregroundColor(ApproachNoteTheme.textSecondary)
            }
            Spacer()
        }
    }
}

#Preview {
    ApproachNoteThemePreview()
}
