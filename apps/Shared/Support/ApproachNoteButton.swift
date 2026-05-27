//
//  ApproachNoteButton.swift
//  Approach Note
//
//  Standardized button component matching the design system spec:
//  rounded corners, brand-blue fill (primary) or brand-blue outline
//  (secondary), uppercase title with optional leading/trailing
//  SF Symbols, and a pressed state on iOS / hover state on Mac.
//

import SwiftUI

/// A reusable themed button matching the Approach Note design system.
///
/// Example:
/// ```swift
/// ApproachNoteButton("Sign In", trailingSystemImage: "arrow.right") {
///     viewModel.signIn()
/// }
///
/// ApproachNoteButton("Cancel", style: .secondary) { dismiss() }
/// ```
struct ApproachNoteButton: View {
    enum Style {
        /// Filled brand-blue background, off-white text. Use for the primary
        /// action on a screen — there should typically be only one of these visible.
        case primary
        /// Brand-blue outline, brand-blue text on a light surface. Use for
        /// secondary actions next to a primary, or standalone less-emphasized actions.
        case secondary
    }

    private let title: String
    private let style: Style
    private let leadingSystemImage: String?
    private let trailingSystemImage: String?
    private let isLoading: Bool
    private let action: () -> Void

    @State private var isHovered = false

    init(
        _ title: String,
        style: Style = .primary,
        leadingSystemImage: String? = nil,
        trailingSystemImage: String? = nil,
        isLoading: Bool = false,
        action: @escaping () -> Void
    ) {
        self.title = title
        self.style = style
        self.leadingSystemImage = leadingSystemImage
        self.trailingSystemImage = trailingSystemImage
        self.isLoading = isLoading
        self.action = action
    }

    var body: some View {
        Button(action: action) {
            HStack(spacing: 10) {
                if let leadingSystemImage, !isLoading {
                    Image(systemName: leadingSystemImage)
                }
                if isLoading {
                    ProgressView()
                        .controlSize(.small)
                } else {
                    Text(title.uppercased())
                        .tracking(0.8)
                }
                if let trailingSystemImage, !isLoading {
                    Image(systemName: trailingSystemImage)
                }
            }
        }
        .buttonStyle(ApproachNoteButtonStyle(style: style, isHovered: isHovered))
        .disabled(isLoading)
        #if os(macOS)
        .onHover { hovering in isHovered = hovering }
        #endif
    }
}

// MARK: - ButtonStyle

private struct ApproachNoteButtonStyle: ButtonStyle {
    let style: ApproachNoteButton.Style
    let isHovered: Bool

    @Environment(\.isEnabled) private var isEnabled

    func makeBody(configuration: Configuration) -> some View {
        let active = configuration.isPressed || isHovered
        let appearance = appearance(active: active)

        configuration.label
            .font(ApproachNoteTheme.headline())
            .foregroundColor(appearance.foreground)
            .frame(maxWidth: .infinity)
            .padding(.vertical, 14)
            .padding(.horizontal, 24)
            .background(
                RoundedRectangle(cornerRadius: 10, style: .continuous)
                    .fill(appearance.background)
            )
            .overlay(
                RoundedRectangle(cornerRadius: 10, style: .continuous)
                    .strokeBorder(appearance.border, lineWidth: appearance.borderWidth)
            )
            .contentShape(RoundedRectangle(cornerRadius: 10, style: .continuous))
            .animation(.easeOut(duration: 0.12), value: configuration.isPressed)
            .animation(.easeOut(duration: 0.12), value: isHovered)
    }

    // MARK: Appearance per (style, enabled, active) combination

    private struct Appearance {
        let background: Color
        let foreground: Color
        let border: Color
        let borderWidth: CGFloat
    }

    private func appearance(active: Bool) -> Appearance {
        switch (style, isEnabled, active) {
        // Primary
        case (.primary, true, false):
            return Appearance(
                background: ApproachNoteTheme.brand,
                foreground: ApproachNoteTheme.textOnDark,
                border: .clear,
                borderWidth: 0
            )
        case (.primary, true, true):
            return Appearance(
                background: ApproachNoteTheme.accentMuted,
                foreground: ApproachNoteTheme.textOnDark,
                border: .clear,
                borderWidth: 0
            )
        case (.primary, false, _):
            return Appearance(
                background: ApproachNoteTheme.surfaceMuted,
                foreground: ApproachNoteTheme.textTertiary,
                border: .clear,
                borderWidth: 0
            )

        // Secondary
        case (.secondary, true, false):
            return Appearance(
                background: ApproachNoteTheme.surface,
                foreground: ApproachNoteTheme.brand,
                border: ApproachNoteTheme.brand,
                borderWidth: 1.5
            )
        case (.secondary, true, true):
            return Appearance(
                background: ApproachNoteTheme.accentBackground,
                foreground: ApproachNoteTheme.brand,
                border: ApproachNoteTheme.brand,
                borderWidth: 1.5
            )
        case (.secondary, false, _):
            return Appearance(
                background: ApproachNoteTheme.surface,
                foreground: ApproachNoteTheme.textTertiary,
                border: ApproachNoteTheme.textTertiary,
                borderWidth: 1.5
            )
        }
    }
}

// MARK: - Previews

#Preview("Primary") {
    VStack(spacing: 16) {
        ApproachNoteButton("Sign In") {}
        ApproachNoteButton("Open in Browser", trailingSystemImage: "arrow.up.right.square") {}
        ApproachNoteButton("Add", leadingSystemImage: "plus") {}
        ApproachNoteButton("Loading", isLoading: true) {}
        ApproachNoteButton("Disabled") {}
            .disabled(true)
    }
    .padding()
    .background(ApproachNoteTheme.background)
}

#Preview("Secondary") {
    VStack(spacing: 16) {
        ApproachNoteButton("Cancel", style: .secondary) {}
        ApproachNoteButton("Learn More", style: .secondary, trailingSystemImage: "arrow.up.right.square") {}
        ApproachNoteButton("Disabled", style: .secondary) {}
            .disabled(true)
    }
    .padding()
    .background(ApproachNoteTheme.background)
}
