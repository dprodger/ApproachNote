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
    static func largeTitle(size: CGFloat = 34, weight: Font.Weight = .bold) -> Font {
        .custom(headingFontName(for: weight), size: size)
    }

    /// Font for titles (e.g., section headers, card titles)
    static func title(size: CGFloat = 28, weight: Font.Weight = .bold) -> Font {
        .custom(headingFontName(for: weight), size: size)
    }

    /// Font for titles level 2
    static func title2(size: CGFloat = 22, weight: Font.Weight = .semibold) -> Font {
        .custom(headingFontName(for: weight), size: size)
    }

    /// Font for titles level 3
    static func title3(size: CGFloat = 20, weight: Font.Weight = .semibold) -> Font {
        .custom(headingFontName(for: weight), size: size)
    }

    /// Font for headlines
    static func headline(size: CGFloat = 17, weight: Font.Weight = .semibold) -> Font {
        .custom(headingFontName(for: weight), size: size)
    }

    // MARK: - Body Fonts

    /// Font for body text
    static func body(size: CGFloat = 17, weight: Font.Weight = .regular, italic: Bool = false) -> Font {
        .custom(bodyFontName(for: weight, italic: italic), size: size)
    }

    /// Font for callouts
    static func callout(size: CGFloat = 16, weight: Font.Weight = .regular, italic: Bool = false) -> Font {
        .custom(bodyFontName(for: weight, italic: italic), size: size)
    }

    /// Font for subheadlines
    static func subheadline(size: CGFloat = 15, weight: Font.Weight = .regular, italic: Bool = false) -> Font {
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
        appearance.backgroundColor = UIColor(burgundy)

        // Large title font (used when scrolled to top)
        appearance.largeTitleTextAttributes = [
            .font: uiHeadingFont(size: 34, weight: .bold),
            .foregroundColor: UIColor.white
        ]

        // Inline title font (used when scrolled or in compact mode)
        appearance.titleTextAttributes = [
            .font: uiHeadingFont(size: 17, weight: .semibold),
            .foregroundColor: UIColor.white
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
            .foregroundColor(.white)
    }
}

extension View {
    /// Applies ApproachNoteTheme styling to the navigation bar with custom title font
    /// Use this instead of .navigationTitle() for themed headers
    /// - Parameters:
    ///   - title: The navigation bar title
    ///   - color: Background color (defaults to burgundy)
    func jazzNavigationBar(title: String, color: Color = ApproachNoteTheme.burgundy) -> some View {
        self
            .navigationBarTitleDisplayMode(.inline)
            .toolbarBackground(color, for: .navigationBar)
            .toolbarBackground(.visible, for: .navigationBar)
            .toolbarColorScheme(.dark, for: .navigationBar)
            .toolbar {
                ToolbarItem(placement: .principal) {
                    Text(title)
                        .font(ApproachNoteTheme.headline())
                        .foregroundColor(.white)
                        .lineLimit(1)
                        .truncationMode(.tail)
                }
            }
    }
}
#endif

extension ApproachNoteTheme {
    // MARK: - Primary Colors
    
    /// Deep burgundy - main accent color
    static let burgundy = Color(red: 0.45, green: 0.15, blue: 0.15) // #731A1A
    
    /// Warm amber - secondary accent
    static let amber = Color(red: 0.85, green: 0.55, blue: 0.25) // #D98C3F
    
    /// Smoky brass - tertiary accent
    static let brass = Color(red: 0.65, green: 0.50, blue: 0.30) // #A67F4D
    
    /// Deep teal - cool accent
    static let teal = Color(red: 0.20, green: 0.35, blue: 0.40) // #335966

    /// Forest green - for backing tracks and practice materials
    static let green = Color(red: 0.25, green: 0.50, blue: 0.35) // #408059

    // MARK: - Neutrals
    
    /// Rich charcoal - primary text on light backgrounds
    static let charcoal = Color(red: 0.15, green: 0.15, blue: 0.15) // #262626
    
    /// Warm cream - text on dark backgrounds
    static let cream = Color(red: 0.95, green: 0.93, blue: 0.88) // #F2EDE0
    
    /// Muted gray - secondary text
    static let smokeGray = Color(red: 0.55, green: 0.52, blue: 0.48) // #8C857A
    
    // MARK: - Special
    
    /// Gold star - for canonical recordings
    static let gold = Color(red: 0.85, green: 0.65, blue: 0.13) // #D9A521
    
    // MARK: - Gradients
    
    static let burgundyGradient = LinearGradient(
        gradient: Gradient(colors: [burgundy, burgundy.opacity(0.8)]),
        startPoint: .leading,
        endPoint: .trailing
    )
    
    static let amberGradient = LinearGradient(
        gradient: Gradient(colors: [amber, amber.opacity(0.8)]),
        startPoint: .leading,
        endPoint: .trailing
    )
    
    static let brassGradient = LinearGradient(
        gradient: Gradient(colors: [brass, brass.opacity(0.8)]),
        startPoint: .leading,
        endPoint: .trailing
    )
    
    static let tealGradient = LinearGradient(
        gradient: Gradient(colors: [teal, teal.opacity(0.8)]),
        startPoint: .leading,
        endPoint: .trailing
    )
    
    // MARK: - Background Colors
    
    /// Light warm background
    static let backgroundLight = Color(red: 0.97, green: 0.95, blue: 0.92) // #F7F2EB
    
    /// Card background
    static let cardBackground = Color(red: 0.93, green: 0.91, blue: 0.87) // #EDE8DD
    
    // MARK: - Section Headers by Type
    
    struct SectionHeader {
        let gradient: LinearGradient
        let icon: String
        
        static let song = SectionHeader(
            gradient: burgundyGradient,
            icon: "music.note"
        )
        
        static let recording = SectionHeader(
            gradient: brassGradient,
            icon: "opticaldisc"
        )
        
        static let artist = SectionHeader(
            gradient: amberGradient,
            icon: "person.fill"
        )
    }
}

// MARK: - View Extension for Easy Access

extension View {
    func jazzThemedSectionHeader(title: String, type: ApproachNoteTheme.SectionHeader) -> some View {
        HStack {
            Image(systemName: type.icon)
                .font(.title2)
                .foregroundColor(ApproachNoteTheme.cream)
            Text(title)
                .font(.headline)
                .fontWeight(.semibold)
                .foregroundColor(ApproachNoteTheme.cream)
            Spacer()
        }
        .padding()
        .background(type.gradient)
    }
}

// MARK: - Themed Progress View

/// A progress view with consistent ApproachNoteTheme styling
/// Use this for all loading indicators to ensure consistent typography
struct ThemedProgressView: View {
    let message: String
    var tintColor: Color = ApproachNoteTheme.brass

    var body: some View {
        ProgressView {
            Text(message)
                .font(ApproachNoteTheme.subheadline())
                .foregroundColor(ApproachNoteTheme.charcoal)
        }
        .tint(tintColor)
    }
}

// MARK: - Usage Examples

struct ApproachNoteThemePreview: View {
    var body: some View {
        ScrollView {
            VStack(spacing: 20) {
                // Song Header
                VStack {
                    Text("SONG")
                        .font(.headline)
                        .fontWeight(.semibold)
                        .foregroundColor(ApproachNoteTheme.cream)
                }
                .frame(maxWidth: .infinity)
                .padding()
                .background(ApproachNoteTheme.SectionHeader.song.gradient)
                
                // Recording Header
                VStack {
                    Text("RECORDING")
                        .font(.headline)
                        .fontWeight(.semibold)
                        .foregroundColor(ApproachNoteTheme.cream)
                }
                .frame(maxWidth: .infinity)
                .padding()
                .background(ApproachNoteTheme.SectionHeader.recording.gradient)
                
                // Artist Header
                VStack {
                    Text("ARTIST")
                        .font(.headline)
                        .fontWeight(.semibold)
                        .foregroundColor(ApproachNoteTheme.cream)
                }
                .frame(maxWidth: .infinity)
                .padding()
                .background(ApproachNoteTheme.SectionHeader.artist.gradient)
                
                // Color Swatches
                VStack(alignment: .leading, spacing: 16) {
                    Text("Color Palette")
                        .font(.title2)
                        .bold()
                    
                    ColorSwatch(name: "Burgundy", color: ApproachNoteTheme.burgundy)
                    ColorSwatch(name: "Amber", color: ApproachNoteTheme.amber)
                    ColorSwatch(name: "Brass", color: ApproachNoteTheme.brass)
                    ColorSwatch(name: "Teal", color: ApproachNoteTheme.teal)
                    ColorSwatch(name: "Gold (Star)", color: ApproachNoteTheme.gold)
                    ColorSwatch(name: "Charcoal", color: ApproachNoteTheme.charcoal)
                    ColorSwatch(name: "Cream", color: ApproachNoteTheme.cream, darkBackground: true)
                    ColorSwatch(name: "Smoke Gray", color: ApproachNoteTheme.smokeGray)
                }
                .padding()
            }
        }
        .background(ApproachNoteTheme.backgroundLight)
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
            
            VStack(alignment: .leading) {
                Text(name)
                    .font(.headline)
                Text(darkBackground ? "For dark backgrounds" : "Primary use")
                    .font(.caption)
                    .foregroundColor(ApproachNoteTheme.smokeGray)
            }
            Spacer()
        }
    }
}

#Preview {
    ApproachNoteThemePreview()
}
