//
//  DetailHeader.swift
//  Approach Note
//
//  Custom collapsing header for detail screens (issue #198). The system
//  nav bar can't give us the design's taller brand band, white-outlined
//  circle buttons (including the back chevron), or the "Song" -> title
//  label swap on scroll, so detail views hide the system bar and compose
//  these pieces instead.
//
//  Anatomy:
//    - DetailHeaderBar: the brand-colored header band (below the status bar)
//      holding the back button, a centered title, and trailing actions. Its
//      background bleeds up under the status bar. Its `height` is driven by
//      scroll offset, interpolating between `expandedHeight` and
//      `collapsedHeight`; the button row is bottom-aligned so it sits low in
//      the expanded band and rides up as the band collapses.
//    - The host screen pairs the bar with a brand spacer of `expandedHeight`
//      at the top of its scroll content so content starts below the expanded
//      header, then feeds scroll offset back in (see SongDetailView).
//    - SwipeBackEnabler restores the interactive pop gesture, which UIKit
//      disables once the back button is hidden.
//

import SwiftUI
import UIKit

// MARK: - Metrics

enum DetailHeaderMetrics {
    /// Header height (below the status bar) at rest, fully expanded.
    static let expandedHeight: CGFloat = 92

    /// Header height (below the status bar) once fully scrolled / collapsed.
    static let collapsedHeight: CGFloat = 58

    /// Diameter of the circular header buttons.
    static let buttonDiameter: CGFloat = 36

    /// Breathing room below the button row. Interpolated between the two so the
    /// buttons sit higher (roomier) in the expanded band but stay snug in the
    /// shorter collapsed bar.
    static let collapsedBottomPadding: CGFloat = 12
    static let expandedBottomPadding: CGFloat = 24

    /// Scroll distance after which the centered label swaps from the generic
    /// category ("Song") to the specific title — roughly when the in-page
    /// title has slid under the bar.
    static let titleSwapOffset: CGFloat = 64

    /// How far the header travels between expanded and collapsed.
    static var collapseDistance: CGFloat { expandedHeight - collapsedHeight }
}

// MARK: - Circle Button

/// Circular header button matching the design: a white-outlined circle with a
/// white glyph on the brand background. The `.filled` style (white fill, brand
/// glyph) is reserved for a primary action like play.
struct DetailCircleButton: View {
    enum Style { case outlined, filled }

    let systemName: String
    var style: Style = .outlined
    var accessibilityLabel: String
    let action: () -> Void

    private var diameter: CGFloat { DetailHeaderMetrics.buttonDiameter }

    var body: some View {
        Button(action: action) {
            Image(systemName: systemName)
                .font(.system(size: 15, weight: .semibold))
                .foregroundStyle(style == .filled ? ApproachNoteTheme.brand : Color.white)
                .frame(width: diameter, height: diameter)
                .background(Circle().fill(style == .filled ? Color.white : Color.clear))
                .overlay(
                    Circle()
                        .stroke(Color.white, lineWidth: 1.5)
                        .opacity(style == .filled ? 0 : 1)
                )
                .contentShape(Circle())
        }
        .buttonStyle(.plain)
        .accessibilityLabel(accessibilityLabel)
    }
}

// MARK: - Pinned Header Bar

/// The brand-colored header bar. Overlay this on a detail screen's scroll view
/// with `.overlay(alignment: .top)`, passing a `height` that interpolates
/// between `DetailHeaderMetrics.expandedHeight` and `.collapsedHeight` as the
/// user scrolls. The button row is bottom-aligned with breathing room, so the
/// buttons/title sit low in the expanded band and ride up as it collapses.
/// Pair it with a brand spacer of `DetailHeaderMetrics.expandedHeight` at the
/// top of the scroll content so content starts below the expanded header.
struct DetailHeaderBar<Trailing: View>: View {
    /// Title shown centered in the bar. Swap this string on scroll (e.g.
    /// "Song" -> the song title) and it cross-fades.
    let title: String
    /// Current header height below the status bar; drive from scroll offset.
    let height: CGFloat
    /// Pull-down overscroll amount. The brand background extends this far below
    /// the bar to cover the gap that would otherwise expose the content's
    /// background during rubber-band scrolling. Does not move the buttons.
    var overscroll: CGFloat = 0
    let onBack: () -> Void
    @ViewBuilder var trailing: () -> Trailing

    private var bottomPadding: CGFloat {
        let span = max(1, DetailHeaderMetrics.collapseDistance)
        let t = max(0, min(1, (height - DetailHeaderMetrics.collapsedHeight) / span))
        return DetailHeaderMetrics.collapsedBottomPadding
            + (DetailHeaderMetrics.expandedBottomPadding - DetailHeaderMetrics.collapsedBottomPadding) * t
    }

    var body: some View {
        ZStack {
            Text(title)
                .font(ApproachNoteTheme.headline())
                .foregroundColor(.white)
                .lineLimit(1)
                .truncationMode(.tail)
                .padding(.horizontal, 64)
                .frame(maxWidth: .infinity)
                .contentTransition(.opacity)
                .animation(.easeInOut(duration: 0.2), value: title)

            HStack {
                DetailCircleButton(systemName: "chevron.left",
                                   accessibilityLabel: "Back",
                                   action: onBack)
                Spacer()
                trailing()
            }
        }
        .frame(height: DetailHeaderMetrics.buttonDiameter)
        .padding(.horizontal, ApproachNoteTheme.spacingLG)
        .padding(.bottom, bottomPadding)
        .frame(maxWidth: .infinity, minHeight: height, maxHeight: height, alignment: .bottom)
        .background(alignment: .bottom) {
            // Pull-down fill: extends the brand below the bar so the content's
            // background never shows through during rubber-band overscroll.
            // Kept separate from the bar's own background so it can't disturb
            // the bar's coverage / status-bar bleed.
            ApproachNoteTheme.brand
                .frame(maxWidth: .infinity)
                .frame(height: max(0, overscroll))
                .offset(y: max(0, overscroll))
        }
        .background(ApproachNoteTheme.brand.ignoresSafeArea(edges: .top))
    }
}

// MARK: - Header Spacer

/// Brand-colored spacer placed at the very top of a detail screen's scroll
/// content, sized to the expanded header so content begins below it (and rides
/// up under the collapsing header overlay). Pair with `.collapsingDetailHeader`.
struct DetailHeaderSpacer: View {
    var body: some View {
        ApproachNoteTheme.brand
            .frame(height: DetailHeaderMetrics.expandedHeight)
    }
}

// MARK: - Collapsing Header Modifier

extension View {
    /// Applies the standard collapsing brand detail header (issue #198) to a
    /// detail screen's `ScrollView`: hides the system nav bar + back button,
    /// restores swipe-back, paints the screen background, and overlays a
    /// `DetailHeaderBar` whose height and title are driven by scroll offset.
    ///
    /// The label shows `expandedTitle` (a generic category like "Song") until
    /// the user scrolls past `DetailHeaderMetrics.titleSwapOffset`, then
    /// cross-fades to `collapsedTitle` (the specific name).
    ///
    /// The caller must place a `DetailHeaderSpacer()` at the top of the scroll
    /// content so content begins below the expanded header.
    func collapsingDetailHeader<Trailing: View>(
        expandedTitle: String,
        collapsedTitle: String,
        @ViewBuilder trailing: @escaping () -> Trailing = { EmptyView() }
    ) -> some View {
        modifier(CollapsingDetailHeaderModifier(
            expandedTitle: expandedTitle,
            collapsedTitle: collapsedTitle,
            trailing: trailing
        ))
    }
}

private struct CollapsingDetailHeaderModifier<Trailing: View>: ViewModifier {
    let expandedTitle: String
    let collapsedTitle: String
    @ViewBuilder var trailing: () -> Trailing

    @Environment(\.dismiss) private var dismiss
    @State private var scrollOffset: CGFloat = 0

    private var headerHeight: CGFloat {
        DetailHeaderMetrics.expandedHeight
            - min(max(0, scrollOffset), DetailHeaderMetrics.collapseDistance)
    }
    private var headerOverscroll: CGFloat { max(0, -scrollOffset) }
    private var isCollapsed: Bool {
        max(0, scrollOffset) >= DetailHeaderMetrics.titleSwapOffset
    }

    func body(content: Content) -> some View {
        content
            .background(ApproachNoteTheme.background)
            .onScrollGeometryChange(for: CGFloat.self) { geometry in
                geometry.contentOffset.y + geometry.contentInsets.top
            } action: { _, newValue in
                scrollOffset = newValue
            }
            .toolbar(.hidden, for: .navigationBar)
            .navigationBarBackButtonHidden(true)
            .background(SwipeBackEnabler())
            .overlay(alignment: .top) {
                DetailHeaderBar(
                    title: isCollapsed ? collapsedTitle : expandedTitle,
                    height: headerHeight,
                    overscroll: headerOverscroll,
                    onBack: { dismiss() },
                    trailing: trailing
                )
            }
    }
}

// MARK: - Swipe-Back Enabler

/// Restores the interactive swipe-to-go-back gesture on a view that has hidden
/// the navigation bar / back button (SwiftUI disables it otherwise). Attach via
/// `.background(SwipeBackEnabler())`.
struct SwipeBackEnabler: UIViewControllerRepresentable {
    func makeCoordinator() -> Coordinator { Coordinator() }

    func makeUIViewController(context: Context) -> UIViewController { UIViewController() }

    func updateUIViewController(_ uiViewController: UIViewController, context: Context) {
        DispatchQueue.main.async {
            guard let nav = uiViewController.navigationController else { return }
            context.coordinator.navController = nav
            nav.interactivePopGestureRecognizer?.isEnabled = true
            nav.interactivePopGestureRecognizer?.delegate = context.coordinator
        }
    }

    final class Coordinator: NSObject, UIGestureRecognizerDelegate {
        weak var navController: UINavigationController?

        // Only allow the swipe when there's a screen to pop back to.
        func gestureRecognizerShouldBegin(_ gestureRecognizer: UIGestureRecognizer) -> Bool {
            (navController?.viewControllers.count ?? 0) > 1
        }
    }
}
