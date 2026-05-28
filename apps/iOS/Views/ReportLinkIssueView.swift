//
//  ReportLinkIssueView.swift
//  Approach Note
//
//  Created by Dave Rodger on 10/24/25.
//
import SwiftUI
import os

// MARK: - Report Bad Reference View

struct ReportLinkIssueView: View {
    let entityType: String
    let entityId: String
    let entityName: String
    let externalSource: String
    let externalUrl: String
    let onSubmit: (String) -> Void
    let onCancel: () -> Void
    
    @State private var explanation: String = ""
    @Environment(\.dismiss) var dismiss
    
    var body: some View {
        NavigationView {
            ScrollView {
                VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingLG) {
                    // Header section with description
                    VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXS) {
                        Text("Report a Problem")
                            .font(ApproachNoteTheme.title3())
                            .fontWeight(.semibold)
                            .foregroundColor(ApproachNoteTheme.textPrimary)
                        
                        Text("Help us improve the quality of our external references by reporting broken or incorrect links.")
                            .font(ApproachNoteTheme.subheadline())
                            .foregroundColor(ApproachNoteTheme.textSecondary)
                    }
                    .padding()
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .background(ApproachNoteTheme.surface)
                    .cornerRadius(10)
                    
                    // Entity Information Card
                    VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingSM) {
                        Text("About This \(entityType)")
                            .font(ApproachNoteTheme.headline())
                            .foregroundColor(ApproachNoteTheme.textPrimary)
                        
                        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXS) {
                            IssueInfoRow(label: "Name", value: entityName)
                            IssueInfoRow(label: "Type", value: entityType)
                            IssueInfoRow(label: "ID", value: entityId, isMonospace: true)
                        }
                    }
                    .padding()
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .background(ApproachNoteTheme.surface)
                    .cornerRadius(10)
                    
                    // External Reference Card
                    VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingSM) {
                        Text("External Link")
                            .font(ApproachNoteTheme.headline())
                            .foregroundColor(ApproachNoteTheme.textPrimary)
                        
                        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXS) {
                            IssueInfoRow(label: "Source", value: externalSource)
                            
                            VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXXS) {
                                Text("URL")
                                    .font(ApproachNoteTheme.caption())
                                    .fontWeight(.medium)
                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                                
                                Text(externalUrl)
                                    .font(.system(.caption, design: .monospaced))
                                    .foregroundColor(ApproachNoteTheme.textPrimary)
                                    .lineLimit(3)
                            }
                        }
                    }
                    .padding()
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .background(ApproachNoteTheme.surface)
                    .cornerRadius(10)
                    
                    // Explanation Input
                    VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingSM) {
                        Text("What's Wrong?")
                            .font(ApproachNoteTheme.headline())
                            .foregroundColor(ApproachNoteTheme.textPrimary)
                        
                        TextEditor(text: $explanation)
                            .frame(minHeight: 120)
                            .padding(ApproachNoteTheme.spacingXS)
                            .background(Color(UIColor.systemBackground))
                            .cornerRadius(8)
                            .overlay(
                                RoundedRectangle(cornerRadius: 8)
                                    .stroke(ApproachNoteTheme.textSecondary.opacity(0.3), lineWidth: 1)
                            )
                        
                        Text("Examples: broken link, incorrect information, wrong page, outdated content")
                            .font(ApproachNoteTheme.caption(italic: true))
                            .foregroundColor(ApproachNoteTheme.textSecondary)
                    }
                    .padding()
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .background(ApproachNoteTheme.surface)
                    .cornerRadius(10)
                }
                .padding()
            }
            .background(Color(UIColor.systemGroupedBackground))
            .navigationTitle("Report Link Issue")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .navigationBarLeading) {
                    Button("Cancel") {
                        onCancel()
                    }
                }
                
                ToolbarItem(placement: .navigationBarTrailing) {
                    Button("Submit") {
                        onSubmit(explanation)
                    }
                    .disabled(explanation.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
                    .fontWeight(.semibold)
                }
            }
        }
    }
}

// MARK: - Helper View for Info Rows
struct IssueInfoRow: View {
    let label: String
    let value: String
    var isMonospace: Bool = false
    
    var body: some View {
        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXXS) {
            Text(label)
                .font(ApproachNoteTheme.caption())
                .fontWeight(.medium)
                .foregroundColor(ApproachNoteTheme.textSecondary)
            
            Text(value)
                .font(isMonospace ? .system(.body, design: .monospaced) : .body)
                .foregroundColor(ApproachNoteTheme.textPrimary)
        }
    }
}

#Preview {
    ReportLinkIssueView(
        entityType: "Song",
        entityId: "preview-song-1",
        entityName: "Take Five",
        externalSource: "Wikipedia",
        externalUrl: "https://en.wikipedia.org/wiki/Take_Five",
        onSubmit: { explanation in
            Log.ui.debug("Submitted: \(explanation, privacy: .public)")
        },
        onCancel: {
            Log.ui.debug("Cancelled")
        }
    )
}
