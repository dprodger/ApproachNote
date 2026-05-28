//
//  ToastView.swift
//  Approach Note
//
//  Created by Dave Rodger on 11/13/25.
//  Reusable toast notification component
//

import SwiftUI

// MARK: - Toast Type
enum ToastType {
    case success
    case error
    case info
    case warning
    
    var icon: String {
        switch self {
        case .success: return "checkmark.circle.fill"
        case .error: return "xmark.circle.fill"
        case .info: return "info.circle.fill"
        case .warning: return "exclamationmark.triangle.fill"
        }
    }
    
    var color: Color {
        switch self {
        case .success: return .green
        case .error: return .red
        case .info: return ApproachNoteTheme.textSecondary
        case .warning: return .orange
        }
    }
}

// MARK: - Toast Item
struct ToastItem: Equatable {
    let id = UUID()
    let type: ToastType
    let message: String
    
    static func == (lhs: ToastItem, rhs: ToastItem) -> Bool {
        lhs.id == rhs.id
    }
}

// MARK: - Toast View
struct ToastView: View {
    let toast: ToastItem
    let onDismiss: () -> Void
    
    var body: some View {
        HStack(spacing: ApproachNoteTheme.spacingSM) {
            Image(systemName: toast.type.icon)
                .font(.system(size: 20, weight: .semibold))
                .foregroundColor(toast.type.color)
            
            Text(toast.message)
                .font(ApproachNoteTheme.subheadline())
                .foregroundColor(ApproachNoteTheme.textPrimary)
                .fixedSize(horizontal: false, vertical: true)
            
            Spacer(minLength: 8)
            
            Button(action: onDismiss) {
                Image(systemName: "xmark")
                    .font(.system(size: 14, weight: .semibold))
                    .foregroundColor(ApproachNoteTheme.textSecondary)
            }
            .buttonStyle(.plain)
        }
        .padding(.horizontal, ApproachNoteTheme.spacingMD)
        .padding(.vertical, 14)
        .background(
            RoundedRectangle(cornerRadius: 12)
                .fill(.white)
                .shadow(color: .black.opacity(0.15), radius: 8, x: 0, y: 4)
        )
        .padding(.horizontal, ApproachNoteTheme.spacingMD)
        .padding(.top, ApproachNoteTheme.spacingXS)
    }
}

// MARK: - Toast Container View
struct ToastContainerView: View {
    let toast: ToastItem?
    let onDismiss: () -> Void
    
    var body: some View {
        VStack {
            if let toast = toast {
                ToastView(toast: toast, onDismiss: onDismiss)
                    .transition(.move(edge: .top).combined(with: .opacity))
                    .zIndex(1)
            }
            Spacer()
        }
        .animation(.spring(response: 0.4, dampingFraction: 0.8), value: toast?.id)
    }
}

// MARK: - Preview
#Preview("Success Toast") {
    VStack {
        ToastView(
            toast: ToastItem(type: .success, message: "Song queued for research"),
            onDismiss: {}
        )
        Spacer()
    }
    .frame(maxWidth: .infinity, maxHeight: .infinity)
    .background(ApproachNoteTheme.background)
}

#Preview("Error Toast") {
    VStack {
        ToastView(
            toast: ToastItem(type: .error, message: "Failed to queue song for refresh. Please try again."),
            onDismiss: {}
        )
        Spacer()
    }
    .frame(maxWidth: .infinity, maxHeight: .infinity)
    .background(ApproachNoteTheme.background)
}

#Preview("Long Message Toast") {
    VStack {
        ToastView(
            toast: ToastItem(type: .info, message: "This is a longer message that will wrap to multiple lines to show how the toast handles extended content gracefully."),
            onDismiss: {}
        )
        Spacer()
    }
    .frame(maxWidth: .infinity, maxHeight: .infinity)
    .background(ApproachNoteTheme.background)
}
