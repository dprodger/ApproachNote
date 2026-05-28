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

    /// Extra line spacing for body text, as a fraction of font size.
    /// `.lineSpacing(fontSize * bodyLineSpacingRatio)` adds breathing room on
    /// top of the font's natural line height. 0.2 ≈ designer-spec "120%".
    /// Tune to taste — lower is tighter, higher is airier.
    static let bodyLineSpacingRatio: CGFloat = 0.2

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

// MARK: - Body Line Spacing

extension View {
    /// Adds the project's standard body line spacing for the given font size.
    /// Pair with `.font(ApproachNoteTheme.body(...))` for prose readability.
    func bodyLineSpacing(size: CGFloat = 16) -> some View {
        self.lineSpacing(size * ApproachNoteTheme.bodyLineSpacingRatio)
    }
}

// MARK: - Spacing Scale
//
// The six recurring spacing values from the layout spec (doc/design/layout.md).
// Use these for padding and inter-element spacing; treat any value outside this
// set as a smell. `SongDetailView` is the reference adoption.
//
//   xxs 4   hairline offset (sub-element below its header)
//   xs  8   tight component internals (icon ↔ label, paragraph stack)
//   sm  12  grouped elements within a section (card body stack)
//   md  16  section separation (between sibling sections)
//   lg  20  horizontal carousel item gap
//   xl  24  screen edge gutter, page-header top inset

extension ApproachNoteTheme {
    static let spacingXXS: CGFloat = 4
    static let spacingXS:  CGFloat = 8
    static let spacingSM:  CGFloat = 12
    static let spacingMD:  CGFloat = 16
    static let spacingLG:  CGFloat = 20
    static let spacingXL:  CGFloat = 24
}

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

    // Color tokens are computed properties that resolve through the current
    // palette selected via `ThemeManager` (see `ThemePalette.swift`). This is
    // a temporary affordance for evaluating multiple palettes at runtime; to
    // lock in a single palette, restore these to `static let` and inline the
    // chosen values.

    // MARK: - Brand

    static var brand: Color { currentPaletteColors.brand }

    // MARK: - Surfaces

    static var background: Color   { currentPaletteColors.background }
    static var surface: Color      { currentPaletteColors.surface }
    static var surfaceMuted: Color { currentPaletteColors.surfaceMuted }

    // MARK: - Text

    static var textPrimary: Color    { currentPaletteColors.textPrimary }
    static var textSecondary: Color  { currentPaletteColors.textSecondary }
    static var textTertiary: Color   { currentPaletteColors.textTertiary }
    static var textOnDark: Color     { currentPaletteColors.textOnDark }
    static var textOnAccent: Color   { currentPaletteColors.textOnAccent }

    // MARK: - Accent (interactive)

    static var accent: Color            { currentPaletteColors.accent }
    static var accentMuted: Color       { currentPaletteColors.accentMuted }
    static var accentBackground: Color  { currentPaletteColors.accentBackground }

    // MARK: - Status

    static var warning: Color           { currentPaletteColors.warning }
    static var warningBackground: Color { currentPaletteColors.warningBackground }

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
        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXXS) {
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
        .padding(.top, ApproachNoteTheme.spacingXL)
        .padding(.bottom, ApproachNoteTheme.spacingXS)
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
