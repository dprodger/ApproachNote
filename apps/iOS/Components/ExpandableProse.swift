//
//  ExpandableProse.swift
//  Approach Note
//
//  Collapsible block of prose (Wikipedia intros, performer biographies)
//  rendered as discrete paragraphs. The source text separates paragraphs with
//  newlines — single (`\n`, song summaries from the MediaWiki extracts API) or
//  double (`\n\n`, performer biographies). We split on any run of newlines so
//  both render with real paragraph breaks instead of one dense block.
//
//  The block is clamped to `maxCollapsedHeight`; when the full text overflows
//  that cap, a bold "Read more" / "Read less" text link toggles it inline. A
//  hidden full-height copy measures the real height so the toggle only appears
//  when the text actually overflows. (Generalized from the performer biography
//  block.)
//

import SwiftUI

struct ExpandableProse: View {
    let text: String
    let maxCollapsedHeight: CGFloat
    /// Body text color. Defaults to the secondary tone used by biographies;
    /// the song summary passes the primary tone.
    var textColor: Color = ApproachNoteTheme.textSecondary

    @State private var isExpanded = false
    @State private var fullHeight: CGFloat = 0

    private var paragraphs: [String] {
        text
            .replacingOccurrences(of: "\r\n", with: "\n")
            .components(separatedBy: "\n")
            .map { $0.trimmingCharacters(in: .whitespaces) }
            .filter { !$0.isEmpty }
    }

    private var isTruncatable: Bool {
        fullHeight > maxCollapsedHeight + 1
    }

    @ViewBuilder
    private var proseText: some View {
        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingSM) {
            ForEach(Array(paragraphs.enumerated()), id: \.offset) { _, paragraph in
                Text(paragraph)
                    .font(ApproachNoteTheme.body())
                    .bodyLineSpacing()
                    .foregroundColor(textColor)
                    .fixedSize(horizontal: false, vertical: true)
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }

    var body: some View {
        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingMD) {
            proseText
                .frame(maxHeight: isExpanded ? nil : maxCollapsedHeight, alignment: .top)
                .clipped()
                .background(
                    // Hidden full-height copy; .fixedSize forces the ideal
                    // height (ignoring the clamp above) so we can detect overflow.
                    proseText
                        .fixedSize(horizontal: false, vertical: true)
                        .background(
                            GeometryReader { proxy in
                                Color.clear
                                    .onAppear { fullHeight = proxy.size.height }
                                    .onChange(of: proxy.size.height) { _, newValue in fullHeight = newValue }
                            }
                        )
                        .hidden()
                )

            if isTruncatable {
                Button {
                    withAnimation(.easeInOut(duration: 0.2)) { isExpanded.toggle() }
                } label: {
                    Text(isExpanded ? "Read less" : "Read more")
                        .font(ApproachNoteTheme.body(weight: .bold))
                        .bodyLineSpacing()
                        .foregroundColor(ApproachNoteTheme.brand)
                }
                .buttonStyle(.plain)
            }
        }
    }
}
