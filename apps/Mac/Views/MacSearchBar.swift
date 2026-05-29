//
//  MacSearchBar.swift
//  Approach Note
//
//  Reusable search bar component for Mac list views
//

import SwiftUI

struct MacSearchBar: View {
    @Binding var text: String
    let placeholder: String
    let backgroundColor: Color
    /// Optional external focus binding so a parent can drive (or observe)
    /// focus on the search field.
    var focus: FocusState<Bool>.Binding? = nil

    var body: some View {
        HStack {
            Image(systemName: "magnifyingglass")
                .foregroundColor(ApproachNoteTheme.textSecondary)
            TextField(placeholder, text: $text)
                .textFieldStyle(.plain)
                .font(ApproachNoteTheme.body())
                .bodyLineSpacing()
                .foregroundColor(ApproachNoteTheme.textPrimary)
                .modifier(OptionalFocusModifier(focus: focus))
            if !text.isEmpty {
                Button(action: { text = "" }) {
                    Image(systemName: "xmark.circle.fill")
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                }
                .buttonStyle(.plain)
            }
        }
        .padding(ApproachNoteTheme.spacingXS)
        .background(Color.white)
        .cornerRadius(8)
        .overlay(
            RoundedRectangle(cornerRadius: 8)
                .stroke(ApproachNoteTheme.textSecondary.opacity(0.3), lineWidth: 1)
        )
        .padding(.horizontal)
        .padding(.vertical, ApproachNoteTheme.spacingSM)
        .background(backgroundColor)
    }
}

/// Applies `.focused` only when a binding is supplied, so callers that don't
/// need programmatic focus can omit it.
private struct OptionalFocusModifier: ViewModifier {
    let focus: FocusState<Bool>.Binding?

    func body(content: Content) -> some View {
        if let focus {
            content.focused(focus)
        } else {
            content
        }
    }
}

#Preview {
    VStack(spacing: 0) {
        MacSearchBar(
            text: .constant(""),
            placeholder: "Search songs...",
            backgroundColor: ApproachNoteTheme.brand
        )
        MacSearchBar(
            text: .constant("test"),
            placeholder: "Search artists...",
            backgroundColor: ApproachNoteTheme.accent
        )
        MacSearchBar(
            text: .constant(""),
            placeholder: "Search recordings...",
            backgroundColor: ApproachNoteTheme.textSecondary
        )
    }
}
