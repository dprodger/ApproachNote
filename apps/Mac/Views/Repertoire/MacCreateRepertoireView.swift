//
//  MacCreateRepertoireView.swift
//  Approach Note
//
//  View for creating new repertoires on macOS
//

import SwiftUI

struct MacCreateRepertoireView: View {
    @ObservedObject var repertoireManager: RepertoireManager
    @Environment(\.dismiss) var dismiss

    @State private var name: String = ""
    @State private var description: String = ""
    @State private var isCreating = false
    @State private var showError = false
    @State private var errorMessage = ""

    private var isFormValid: Bool {
        !name.trimmingCharacters(in: .whitespaces).isEmpty
    }

    var body: some View {
        VStack(spacing: ApproachNoteTheme.spacingLG) {
            // Header
            Text("Create Repertoire")
                .font(ApproachNoteTheme.title())
                .foregroundColor(ApproachNoteTheme.textPrimary)
                .padding(.top, ApproachNoteTheme.spacingLG)

            // Form
            VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingMD) {
                // Name field
                VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXS) {
                    Text("Name")
                        .font(ApproachNoteTheme.subheadline())
                        .foregroundColor(ApproachNoteTheme.textPrimary.opacity(0.7))

                    TextField("Repertoire Name", text: $name)
                        .textFieldStyle(.roundedBorder)

                    Text("Give your repertoire a descriptive name")
                        .font(ApproachNoteTheme.caption())
                        .foregroundColor(ApproachNoteTheme.textPrimary.opacity(0.7))
                }

                // Description field
                VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXS) {
                    Text("Description (optional)")
                        .font(ApproachNoteTheme.subheadline())
                        .foregroundColor(ApproachNoteTheme.textPrimary.opacity(0.7))

                    TextEditor(text: $description)
                        .frame(height: 80)
                        .font(ApproachNoteTheme.body())
                        .bodyLineSpacing()
                        .overlay(
                            RoundedRectangle(cornerRadius: 5)
                                .stroke(Color.gray.opacity(0.3), lineWidth: 1)
                        )

                    Text("Add notes about what this repertoire contains")
                        .font(ApproachNoteTheme.caption())
                        .foregroundColor(ApproachNoteTheme.textPrimary.opacity(0.7))
                }
            }
            .padding(.horizontal)

            Spacer()

            // Error message
            if let error = repertoireManager.errorMessage, showError {
                Text(error)
                    .font(ApproachNoteTheme.caption())
                    .foregroundColor(.red)
                    .padding(.horizontal)
            }

            // Buttons
            HStack(spacing: ApproachNoteTheme.spacingSM) {
                Button("Cancel") {
                    dismiss()
                }
                .buttonStyle(.bordered)
                .controlSize(.large)

                Button("Create") {
                    createRepertoire()
                }
                .buttonStyle(.borderedProminent)
                .tint(ApproachNoteTheme.brand)
                .controlSize(.large)
                .disabled(!isFormValid || isCreating)
            }
            .padding(.bottom, ApproachNoteTheme.spacingLG)
        }
        .frame(width: 350, height: 350)
        .overlay {
            if isCreating {
                ZStack {
                    Color.black.opacity(0.3)

                    VStack(spacing: ApproachNoteTheme.spacingMD) {
                        ProgressView()
                            .controlSize(.large)
                        Text("Creating repertoire...")
                            .font(ApproachNoteTheme.subheadline())
                    }
                    .padding(ApproachNoteTheme.spacingXL)
                    .background(ApproachNoteTheme.surface)
                    .cornerRadius(12)
                }
            }
        }
    }

    private func createRepertoire() {
        let trimmedName = name.trimmingCharacters(in: .whitespaces)
        guard !trimmedName.isEmpty else { return }

        let trimmedDescription = description.trimmingCharacters(in: .whitespaces)
        let finalDescription = trimmedDescription.isEmpty ? nil : trimmedDescription

        isCreating = true
        showError = false

        Task {
            let success = await repertoireManager.createRepertoire(
                name: trimmedName,
                description: finalDescription
            )

            await MainActor.run {
                isCreating = false

                if success {
                    dismiss()
                } else {
                    showError = true
                }
            }
        }
    }
}

#Preview {
    MacCreateRepertoireView(repertoireManager: RepertoireManager())
}
