//
//  HelperViews.swift
//  Approach Note
//
//  Updated with ApproachNoteTheme color palette
//

import SwiftUI
import Combine

// MARK: - Helper Views

struct DetailRow: View {
    let icon: String
    let label: String
    let value: String
    
    var body: some View {
        HStack {
            Label {
                Text(label)
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(ApproachNoteTheme.textSecondary)
            } icon: {
                Image(systemName: icon)
                    .foregroundColor(ApproachNoteTheme.textSecondary)
            }
            Spacer()
            Text(value)
                .font(ApproachNoteTheme.subheadline())
                .bold()
                .foregroundColor(ApproachNoteTheme.textPrimary)
        }
    }
}

struct StreamingButton: View {
    let icon: String
    let color: Color
    let label: String
    
    var body: some View {
        VStack(spacing: ApproachNoteTheme.spacingXS) {
            Image(systemName: icon)
                .font(ApproachNoteTheme.title2())
                .foregroundColor(ApproachNoteTheme.textOnDark)
                .frame(width: 60, height: 60)
                .background(color)
                .clipShape(Circle())
            
            Text(label)
                .font(ApproachNoteTheme.caption())
                .foregroundColor(ApproachNoteTheme.textSecondary)
        }
    }
}

struct PerformerRowView: View {
    let performer: Performer
    
    var body: some View {
        HStack {
            VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXXS) {
                Text(performer.name)
                    .font(ApproachNoteTheme.headline())
                    .foregroundColor(ApproachNoteTheme.textPrimary)
                
                if let instrument = performer.instrument {
                    Text(instrument)
                        .font(ApproachNoteTheme.subheadline())
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                }
            }
            
            Spacer()
            
            if let role = performer.role {
                Text(role.capitalized)
                    .font(ApproachNoteTheme.caption())
                    .foregroundColor(ApproachNoteTheme.textOnDark)
                    .padding(.horizontal, ApproachNoteTheme.spacingXS)
                    .padding(.vertical, ApproachNoteTheme.spacingXXS)
                    .background(role == "leader" ? ApproachNoteTheme.brand : ApproachNoteTheme.textSecondary.opacity(0.7))
                    .cornerRadius(8)
            }
        }
        .padding()
        .background(ApproachNoteTheme.surface)
        .cornerRadius(10)
        .padding(.horizontal)
    }
}

// MARK: - NEW COMPONENTS

// External Reference Row Component
struct ExternalReferenceRow: View {
    let reference: ExternalReference
    
    var body: some View {
        Link(destination: URL(string: reference.url)!) {
            HStack(spacing: ApproachNoteTheme.spacingSM) {
                // Icon
                Image(systemName: reference.iconName)
                    .font(ApproachNoteTheme.title3())
                    .foregroundColor(ApproachNoteTheme.brand)
                    .frame(width: 32)
                
                // Source name
                Text(reference.displayName)
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(ApproachNoteTheme.textPrimary)
                
                Spacer()
                
                // External link indicator
                Image(systemName: "arrow.up.right.square")
                    .font(ApproachNoteTheme.caption())
                    .foregroundColor(ApproachNoteTheme.textSecondary)
            }
            .padding(.horizontal)
            .padding(.vertical, ApproachNoteTheme.spacingSM)
            .background(ApproachNoteTheme.surface)
            .cornerRadius(8)
            .padding(.horizontal)
        }
    }
}

// Enhanced Recording Row with Authority Badge
struct AuthorityRecordingRow: View {
    let recording: Recording
    
    var body: some View {
        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXS) {
            HStack(alignment: .top) {
                VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXS) {
                    // Title with canonical indicator
                    HStack(spacing: ApproachNoteTheme.spacingXS) {
                        if recording.isCanonical == true {
                            Image(systemName: "star.fill")
                                .foregroundColor(.yellow)
                                .font(ApproachNoteTheme.caption())
                        }
                        
                        Text(recording.albumTitle ?? "Unknown Album")
                            .font(ApproachNoteTheme.headline())
                            .foregroundColor(ApproachNoteTheme.textPrimary)
                    }
                    
                    // Year
                    if let year = recording.recordingYear {
                        Text(String(year))
                            .font(ApproachNoteTheme.subheadline())
                            .foregroundColor(ApproachNoteTheme.textSecondary)
                    }
                }
                
                Spacer()
                
                // Authority badge (if present)
                if recording.hasAuthority, let badgeText = recording.authorityBadgeText {
                    AuthorityBadge(text: badgeText, source: recording.primaryAuthoritySource)
                }
            }
        }
        .padding()
        .background(ApproachNoteTheme.surface)
        .cornerRadius(10)
        .padding(.horizontal)
    }
}

// Authority Badge Component
struct AuthorityBadge: View {
    let text: String
    let source: String?
    
    var body: some View {
        HStack(spacing: ApproachNoteTheme.spacingXXS) {
            Image(systemName: "checkmark.seal.fill")
                .font(ApproachNoteTheme.caption2())
            Text(text)
                .font(ApproachNoteTheme.caption2())
                .fontWeight(.semibold)
        }
        .foregroundColor(.white)
        .padding(.horizontal, ApproachNoteTheme.spacingXS)
        .padding(.vertical, ApproachNoteTheme.spacingXXS)
        .background(badgeColor)
        .cornerRadius(6)
    }
    
    private var badgeColor: Color {
        // Color code by source
        guard let source = source else { return ApproachNoteTheme.brand }
        
        switch source.lowercased() {
        case "jazzstandards.com":
            return Color(red: 0.2, green: 0.5, blue: 0.8) // Blue
        case "allmusic":
            return Color(red: 0.8, green: 0.3, blue: 0.3) // Red
        case "discogs":
            return Color(red: 0.4, green: 0.7, blue: 0.4) // Green
        default:
            return ApproachNoteTheme.brand
        }
    }
}
