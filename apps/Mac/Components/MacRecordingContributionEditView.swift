//
//  MacRecordingContributionEditView.swift
//  Approach Note
//
//  Form for editing user's contribution to recording metadata (key, tempo, instrumental/vocal)
//

import SwiftUI

struct MacRecordingContributionEditView: View {
    let recordingId: String
    let recordingTitle: String
    let currentContribution: UserContribution?
    let onSave: () -> Void

    @Environment(\.dismiss) var dismiss
    @EnvironmentObject var authManager: AuthenticationManager

    @State private var selectedKey: MusicalKey?
    @State private var selectedTempo: TempoMarking?
    @State private var isInstrumental: Bool?
    @State private var isSaving = false
    @State private var isDeleting = false
    @State private var showError = false
    @State private var errorMessage = ""
    @State private var showDeleteConfirmation = false

    var body: some View {
        VStack(spacing: 0) {
            // Header
            HStack {
                Text("Contribute Data")
                    .font(ApproachNoteTheme.title2())
                    .foregroundColor(ApproachNoteTheme.textPrimary)
                Spacer()
            }
            .padding()
            .background(ApproachNoteTheme.background)

            Divider()

            // Form content
            ScrollView {
                VStack(alignment: .leading, spacing: 24) {
                    // Recording info
                    Text(recordingTitle)
                        .font(ApproachNoteTheme.headline())
                        .foregroundColor(ApproachNoteTheme.textSecondary)

                    // Performance Key Section
                    VStack(alignment: .leading, spacing: 8) {
                        Text("Performance Key")
                            .font(ApproachNoteTheme.subheadline(weight: .medium))
                            .foregroundColor(ApproachNoteTheme.textPrimary)

                        Picker("Key", selection: $selectedKey) {
                            Text("Not set").tag(nil as MusicalKey?)
                            ForEach(MusicalKey.allCases) { key in
                                Text(key.displayName).tag(key as MusicalKey?)
                            }
                        }
                        .labelsHidden()
                        .frame(maxWidth: .infinity, alignment: .leading)

                        Text("The key this performance is played in (may differ from the original composed key)")
                            .font(ApproachNoteTheme.caption())
                            .foregroundColor(ApproachNoteTheme.textSecondary)
                    }

                    // Tempo Section
                    VStack(alignment: .leading, spacing: 8) {
                        Text("Tempo")
                            .font(ApproachNoteTheme.subheadline(weight: .medium))
                            .foregroundColor(ApproachNoteTheme.textPrimary)

                        Picker("Tempo", selection: $selectedTempo) {
                            Text("Not set").tag(nil as TempoMarking?)
                            ForEach(TempoMarking.allCases) { tempo in
                                Text("\(tempo.displayName) (\(tempo.bpmRange) BPM)").tag(tempo as TempoMarking?)
                            }
                        }
                        .labelsHidden()
                        .frame(maxWidth: .infinity, alignment: .leading)

                        Text("Select the general tempo feel of this performance")
                            .font(ApproachNoteTheme.caption())
                            .foregroundColor(ApproachNoteTheme.textSecondary)
                    }

                    // Instrumental/Vocal Section
                    VStack(alignment: .leading, spacing: 8) {
                        Text("Instrumental or Vocal")
                            .font(ApproachNoteTheme.subheadline(weight: .medium))
                            .foregroundColor(ApproachNoteTheme.textPrimary)

                        Picker("Type", selection: $isInstrumental) {
                            Text("Not set").tag(nil as Bool?)
                            Text("Instrumental").tag(true as Bool?)
                            Text("Vocal").tag(false as Bool?)
                        }
                        .pickerStyle(.segmented)
                        .labelsHidden()
                        .frame(maxWidth: 300)

                        Text("Is this an instrumental recording or does it include vocals?")
                            .font(ApproachNoteTheme.caption())
                            .foregroundColor(ApproachNoteTheme.textSecondary)
                    }

                    // Info text
                    if !hasChanges {
                        Text("Enter at least one value above to save")
                            .font(ApproachNoteTheme.caption())
                            .foregroundColor(ApproachNoteTheme.textSecondary)
                    } else {
                        Text("You can contribute just one field or all three - any contribution helps!")
                            .font(ApproachNoteTheme.caption())
                            .foregroundColor(ApproachNoteTheme.textSecondary)
                    }
                }
                .padding()
            }

            Divider()

            // Footer buttons
            HStack {
                // Delete button (if has existing contribution)
                if currentContribution != nil {
                    Button(role: .destructive) {
                        showDeleteConfirmation = true
                    } label: {
                        if isDeleting {
                            ProgressView()
                                .controlSize(.small)
                        } else {
                            Text("Delete My Contribution")
                        }
                    }
                    .disabled(isDeleting || isSaving)
                }

                Spacer()

                Button("Cancel") {
                    dismiss()
                }
                .keyboardShortcut(.cancelAction)
                .disabled(isSaving || isDeleting)

                Button {
                    saveContribution()
                } label: {
                    if isSaving {
                        ProgressView()
                            .controlSize(.small)
                    } else {
                        Text("Save")
                    }
                }
                .keyboardShortcut(.defaultAction)
                .buttonStyle(.borderedProminent)
                .tint(ApproachNoteTheme.brand)
                .disabled(isSaving || isDeleting || !hasChanges)
            }
            .padding()
            .background(ApproachNoteTheme.background)
        }
        .frame(width: 450, height: 500)
        .background(ApproachNoteTheme.background)
        .onAppear {
            loadCurrentValues()
        }
        .alert("Error", isPresented: $showError) {
            Button("OK", role: .cancel) { }
        } message: {
            Text(errorMessage)
        }
        .confirmationDialog(
            "Delete Contribution",
            isPresented: $showDeleteConfirmation,
            titleVisibility: .visible
        ) {
            Button("Delete", role: .destructive) {
                deleteContribution()
            }
            Button("Cancel", role: .cancel) { }
        } message: {
            Text("Are you sure you want to delete your contribution for this recording?")
        }
    }

    private var hasChanges: Bool {
        selectedKey != nil || selectedTempo != nil || isInstrumental != nil
    }

    private func loadCurrentValues() {
        if let contrib = currentContribution {
            if let key = contrib.performanceKey {
                selectedKey = MusicalKey(rawValue: key)
            }
            if let tempo = contrib.tempoMarking {
                selectedTempo = TempoMarking(rawValue: tempo)
            }
            isInstrumental = contrib.isInstrumental
        }
    }

    private func saveContribution() {
        // Check if at least one field has a value
        if selectedKey == nil && selectedTempo == nil && isInstrumental == nil {
            errorMessage = "Please provide at least one value"
            showError = true
            return
        }

        isSaving = true

        Task {
            do {
                // Build request body
                var body: [String: Any] = [:]
                if let key = selectedKey?.rawValue { body["performance_key"] = key }
                if let tempo = selectedTempo?.rawValue { body["tempo_marking"] = tempo }
                if let instrumental = isInstrumental { body["is_instrumental"] = instrumental }

                let bodyData = try JSONSerialization.data(withJSONObject: body)

                let url = URL.api(path: "/recordings/\(recordingId)/contribution")

                _ = try await authManager.makeAuthenticatedRequest(
                    url: url,
                    method: "PUT",
                    body: bodyData,
                    contentType: "application/json"
                )

                await MainActor.run {
                    isSaving = false
                    onSave()
                    dismiss()
                }
            } catch {
                await MainActor.run {
                    isSaving = false
                    if (error as? URLError)?.code == .userAuthenticationRequired {
                        errorMessage = "Not authenticated. Please sign in again."
                    } else {
                        errorMessage = "Failed to save contribution. Please try again."
                    }
                    showError = true
                }
            }
        }
    }

    private func deleteContribution() {
        isDeleting = true

        Task {
            do {
                let url = URL.api(path: "/recordings/\(recordingId)/contribution")

                _ = try await authManager.makeAuthenticatedRequest(
                    url: url,
                    method: "DELETE"
                )

                await MainActor.run {
                    isDeleting = false
                    onSave()
                    dismiss()
                }
            } catch {
                await MainActor.run {
                    isDeleting = false
                    if (error as? URLError)?.code == .userAuthenticationRequired {
                        errorMessage = "Not authenticated. Please sign in again."
                    } else {
                        errorMessage = "Failed to delete contribution. Please try again."
                    }
                    showError = true
                }
            }
        }
    }
}

// MARK: - Previews

#Preview("New Contribution") {
    MacRecordingContributionEditView(
        recordingId: "preview-1",
        recordingTitle: "Take Five - Time Out",
        currentContribution: nil,
        onSave: {}
    )
    .environmentObject(AuthenticationManager())
}

#Preview("Edit Contribution") {
    MacRecordingContributionEditView(
        recordingId: "preview-2",
        recordingTitle: "Take Five - Time Out",
        currentContribution: UserContribution.preview,
        onSave: {}
    )
    .environmentObject(AuthenticationManager())
}
