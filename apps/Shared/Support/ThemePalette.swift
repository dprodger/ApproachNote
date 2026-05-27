//
//  ThemePalette.swift
//  Approach Note
//
//  Temporary runtime palette switcher for evaluating two design palettes
//  side-by-side. Driven from Settings. Drop the picker and revert
//  `ApproachNoteTheme`'s color tokens back to `static let` when done.
//

import SwiftUI
import Combine

// MARK: - Palette Choice

enum PaletteChoice: String, CaseIterable, Identifiable {
    case palette1Blue
    case palette1Red
    case palette2Blue
    case palette2Red

    var id: String { rawValue }

    var displayName: String {
        switch self {
        case .palette1Blue: return "Palette 1 · Blue"
        case .palette1Red:  return "Palette 1 · Red"
        case .palette2Blue: return "Palette 2 · Blue"
        case .palette2Red:  return "Palette 2 · Red"
        }
    }

    var colors: ThemeColors {
        switch self {
        case .palette1Blue: return .palette1Blue
        case .palette1Red:  return .palette1Red
        case .palette2Blue: return .palette2Blue
        case .palette2Red:  return .palette2Red
        }
    }
}

// MARK: - Theme Colors

struct ThemeColors {
    // Brand
    let brand: Color

    // Surfaces
    let background: Color
    let surface: Color
    let surfaceMuted: Color

    // Text
    let textPrimary: Color
    let textSecondary: Color
    let textTertiary: Color
    let textOnDark: Color
    let textOnAccent: Color

    // Accent
    let accent: Color
    let accentMuted: Color
    let accentBackground: Color

    // Status
    let warning: Color
    let warningBackground: Color
}

// MARK: - Palette 1 (lighter, brighter, more-saturated)
//
// Neutrals: pure-white background, near-black text, neutral grays.

private extension ThemeColors {
    static let palette1Base_background      = Color(hex: 0xFFFFFF)
    static let palette1Base_surface         = Color(hex: 0xFFFFFF)
    static let palette1Base_surfaceMuted    = Color(hex: 0xD4D4D4)
    static let palette1Base_textPrimary     = Color(hex: 0x2A2A2A)
    static let palette1Base_textSecondary   = Color(hex: 0x555555)
    static let palette1Base_textTertiary    = Color(hex: 0x7F7F7F)
    static let palette1Base_textOnDark      = Color(hex: 0xFFFFFF)
    static let palette1Base_textOnAccent    = Color.white

    static let palette2Base_background      = Color(hex: 0xFFFCF7)
    static let palette2Base_surface         = Color(hex: 0xFFFFFF)
    static let palette2Base_surfaceMuted    = Color(hex: 0xD9D7D7)
    static let palette2Base_textPrimary     = Color(hex: 0x413737)
    static let palette2Base_textSecondary   = Color(hex: 0x675F5F)
    static let palette2Base_textTertiary    = Color(hex: 0xB3AFAF)
    static let palette2Base_textOnDark      = Color(hex: 0xFFFCF7)
    static let palette2Base_textOnAccent    = Color.white
}

extension ThemeColors {
    static let palette1Blue = ThemeColors(
        brand:              Color(hex: 0x454EFF),
        background:         palette1Base_background,
        surface:            palette1Base_surface,
        surfaceMuted:       palette1Base_surfaceMuted,
        textPrimary:        palette1Base_textPrimary,
        textSecondary:      palette1Base_textSecondary,
        textTertiary:       palette1Base_textTertiary,
        textOnDark:         palette1Base_textOnDark,
        textOnAccent:       palette1Base_textOnAccent,
        accent:             Color(hex: 0x454EFF),
        accentMuted:        Color(hex: 0x8F95FF),
        accentBackground:   Color(hex: 0xECEDFF),
        warning:            Color(hex: 0xFF3A4E),
        warningBackground:  Color(hex: 0xFFEBED)
    )

    static let palette1Red = ThemeColors(
        brand:              Color(hex: 0xFF3A4E),
        background:         palette1Base_background,
        surface:            palette1Base_surface,
        surfaceMuted:       palette1Base_surfaceMuted,
        textPrimary:        palette1Base_textPrimary,
        textSecondary:      palette1Base_textSecondary,
        textTertiary:       palette1Base_textTertiary,
        textOnDark:         palette1Base_textOnDark,
        textOnAccent:       palette1Base_textOnAccent,
        accent:             Color(hex: 0xFF3A4E),
        accentMuted:        Color(hex: 0xFF8995),
        accentBackground:   Color(hex: 0xFFEBED),
        warning:            Color(hex: 0xFF3A4E),
        warningBackground:  Color(hex: 0xFFEBED)
    )

    static let palette2Blue = ThemeColors(
        brand:              Color(hex: 0x363A87),
        background:         palette2Base_background,
        surface:            palette2Base_surface,
        surfaceMuted:       palette2Base_surfaceMuted,
        textPrimary:        palette2Base_textPrimary,
        textSecondary:      palette2Base_textSecondary,
        textTertiary:       palette2Base_textTertiary,
        textOnDark:         palette2Base_textOnDark,
        textOnAccent:       palette2Base_textOnAccent,
        accent:             Color(hex: 0x363A87),
        accentMuted:        Color(hex: 0x5D619F),
        accentBackground:   Color(hex: 0xECEDFF),
        warning:            Color(hex: 0xFF3A4E),
        warningBackground:  Color(hex: 0xFFEBED)
    )

    static let palette2Red = ThemeColors(
        brand:              Color(hex: 0x93262F),
        background:         palette2Base_background,
        surface:            palette2Base_surface,
        surfaceMuted:       palette2Base_surfaceMuted,
        textPrimary:        palette2Base_textPrimary,
        textSecondary:      palette2Base_textSecondary,
        textTertiary:       palette2Base_textTertiary,
        textOnDark:         palette2Base_textOnDark,
        textOnAccent:       palette2Base_textOnAccent,
        accent:             Color(hex: 0x93262F),
        accentMuted:        Color(hex: 0xAA4E57),
        accentBackground:   Color(hex: 0xFFEBED),
        warning:            Color(hex: 0xFF3A4E),
        warningBackground:  Color(hex: 0xFFEBED)
    )
}

// MARK: - Theme Manager
//
// Source of truth for the current palette choice. Persists in UserDefaults
// so the picked palette survives relaunch. SwiftUI views observing this
// object re-render when `palette` changes; app entry points additionally
// apply `.id(theme.palette)` to the root view to force a tree-wide refresh
// (because `ApproachNoteTheme.brand` etc. are read as static accessors
// rather than via @Environment, they don't trigger fine-grained invalidation).

@MainActor
final class ThemeManager: ObservableObject {
    static let shared = ThemeManager()

    static let storageKey = "appThemePalette"

    @Published var palette: PaletteChoice {
        didSet {
            UserDefaults.standard.set(palette.rawValue, forKey: Self.storageKey)
        }
    }

    private init() {
        if let raw = UserDefaults.standard.string(forKey: Self.storageKey),
           let choice = PaletteChoice(rawValue: raw) {
            self.palette = choice
        } else {
            // Default mirrors the original hard-coded theme (Palette 2 + Blue).
            self.palette = .palette2Blue
        }
    }
}

// MARK: - Static read path (off-main-actor safe)
//
// `ApproachNoteTheme` exposes color tokens as `static var` computed
// properties so call sites (`ApproachNoteTheme.brand`) keep working without
// changes. They read the palette synchronously from UserDefaults, which is
// thread-safe and inexpensive — important because SwiftUI's layout pass can
// touch these from non-main contexts.

extension ApproachNoteTheme {
    static var currentPaletteColors: ThemeColors {
        let raw = UserDefaults.standard.string(forKey: ThemeManager.storageKey)
            ?? PaletteChoice.palette2Blue.rawValue
        return (PaletteChoice(rawValue: raw) ?? .palette2Blue).colors
    }
}

// MARK: - Color hex helper

extension Color {
    /// Initialize from a 24-bit RGB hex value, e.g. `Color(hex: 0x454EFF)`.
    init(hex: UInt32, alpha: Double = 1.0) {
        let r = Double((hex >> 16) & 0xFF) / 255.0
        let g = Double((hex >>  8) & 0xFF) / 255.0
        let b = Double( hex        & 0xFF) / 255.0
        self.init(.sRGB, red: r, green: g, blue: b, opacity: alpha)
    }
}
