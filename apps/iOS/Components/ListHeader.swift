//
//  ListHeader.swift
//  Approach Note
//
//  Custom brand header band for list screens (Songs, Artists). Mirrors the
//  detail screens' custom header so the app shows consistent opaque brand bars
//  without relying on the system navigation bar. On iOS 26 the system bar either
//  leaks scrolling content through Liquid Glass (when tinted) or breaks the
//  search / pop transition in compatibility mode; composing our own header
//  side-steps both. Hosts the screen title and an always-visible search field.
//
//  Apply with `.safeAreaInset(edge: .top) { ListHeaderBar(...) }` on a screen
//  that also hides the system bar via `.toolbar(.hidden, for: .navigationBar)`.
//  Using a safe-area inset (rather than an overlay) means the list's content and
//  its scroll anchors are inset below the header automatically, so the A–Z index
//  "scroll to letter" still lands in view.
//

import SwiftUI

enum ListHeaderMetrics {
    /// Header content height below the safe-area top (status bar on iPhone, tab
    /// bar on iPad). The brand background bleeds above this, under the status bar.
    static let contentHeight: CGFloat = 96
}

struct ListHeaderBar<Trailing: View>: View {
    let title: String
    @Binding var searchText: String
    var prompt: String
    var focused: FocusState<Bool>.Binding
    @ViewBuilder var trailing: () -> Trailing

    var body: some View {
        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingSM) {
            HStack(spacing: ApproachNoteTheme.spacingSM) {
                Text(title)
                    .font(ApproachNoteTheme.title3())
                    .bold()
                    .foregroundColor(.white)
                    .lineLimit(1)
                    .minimumScaleFactor(0.7)
                Spacer(minLength: 0)
                trailing()
            }

            HStack(spacing: ApproachNoteTheme.spacingXS) {
                Image(systemName: "magnifyingglass")
                    .foregroundColor(.white.opacity(0.7))
                TextField("", text: $searchText,
                          prompt: Text(prompt).foregroundColor(.white.opacity(0.6)))
                    .foregroundColor(.white)
                    .textInputAutocapitalization(.never)
                    .autocorrectionDisabled()
                    .submitLabel(.search)
                    .focused(focused)
                if !searchText.isEmpty {
                    Button {
                        searchText = ""
                    } label: {
                        Image(systemName: "xmark.circle.fill")
                            .foregroundColor(.white.opacity(0.7))
                    }
                    .buttonStyle(.plain)
                    .accessibilityLabel("Clear search")
                }
            }
            .padding(.horizontal, ApproachNoteTheme.spacingSM)
            .padding(.vertical, 7)
            .background(Capsule().fill(Color.white.opacity(0.18)))
        }
        .padding(.horizontal, ApproachNoteTheme.spacingLG)
        .padding(.bottom, ApproachNoteTheme.spacingSM)
        .frame(maxWidth: .infinity, minHeight: ListHeaderMetrics.contentHeight, alignment: .bottom)
        .background(ApproachNoteTheme.brand.ignoresSafeArea(edges: .top))
    }
}

// Convenience for headers without a trailing accessory.
extension ListHeaderBar where Trailing == EmptyView {
    init(title: String,
         searchText: Binding<String>,
         prompt: String,
         focused: FocusState<Bool>.Binding) {
        self.init(title: title,
                  searchText: searchText,
                  prompt: prompt,
                  focused: focused,
                  trailing: { EmptyView() })
    }
}
