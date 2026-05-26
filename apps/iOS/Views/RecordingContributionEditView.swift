//
//  RecordingContributionEditView.swift
//  Approach Note
//
//  Form for editing user's contribution to recording metadata (key, tempo, instrumental/vocal)
//

import SwiftUI

struct RecordingContributionEditView: View {
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
        NavigationStack {
            Form {
                // Performance Key Section
                Section {
                    Picker("Key", selection: $selectedKey) {
                        Text("Not set").tag(nil as MusicalKey?)
                        ForEach(MusicalKey.allCases) { key in
                            Text(key.displayName).tag(key as MusicalKey?)
                        }
                    }
                } header: {
                    Text("Performance Key")
                } footer: {
                    Text("The key this performance is played in (may differ from the original composed key)")
                }

                // Tempo Section
                Section {
                    Picker("Tempo", selection: $selectedTempo) {
                        Text("Not set").tag(nil as TempoMarking?)
                        ForEach(TempoMarking.allCases) { tempo in
                            Text("\(tempo.displayName) (\(tempo.bpmRange) BPM)").tag(tempo as TempoMarking?)
                        }
                    }
                } header: {
                    Text("Tempo")
                } footer: {
                    Text("Select the general tempo feel of this performance")
                }

                // Instrumental/Vocal Section
                Section {
                    Picker("Type", selection: $isInstrumental) {
                        Text("Not set").tag(nil as Bool?)
                        Text("Instrumental").tag(true as Bool?)
                        Text("Vocal").tag(false as Bool?)
                    }
                    .pickerStyle(.segmented)
                } header: {
                    Text("Instrumental or Vocal")
                } footer: {
                    Text("Is this an instrumental recording or does it include vocals?")
                }

                // Save button section
                Section {
                    Button {
                        saveContribution()
                    } label: {
                        HStack {
                            Spacer()
                            if isSaving {
                                ProgressView()
                                    .tint(.white)
                            } else {
                                Text("Save Contribution")
                                    .fontWeight(.semibold)
                            }
                            Spacer()
                        }
                    }
                    .listRowBackground(hasChanges ? ApproachNoteTheme.brand : ApproachNoteTheme.textSecondary.opacity(0.5))
                    .foregroundColor(.white)
                    .disabled(isSaving || isDeleting || !hasChanges)
                } footer: {
                    if !hasChanges {
                        Text("Enter at least one value above to save")
                    } else {
                        Text("You can contribute just one field or all three - any contribution helps!")
                    }
                }

                // Delete contribution button
                if currentContribution != nil {
                    Section {
                        Button(role: .destructive) {
                            showDeleteConfirmation = true
                        } label: {
                            HStack {
                                Spacer()
                                if isDeleting {
                                    ProgressView()
                                        .tint(.red)
                                } else {
                                    Text("Delete My Contribution")
                                }
                                Spacer()
                            }
                        }
                        .disabled(isDeleting || isSaving)
                    }
                }
            }
            .scrollContentBackground(.hidden)
            .background(ApproachNoteTheme.background)
            .navigationTitle("Contribute Data")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .navigationBarLeading) {
                    Button("Cancel") {
                        dismiss()
                    }
                    .foregroundColor(ApproachNoteTheme.brand)
                    .disabled(isSaving || isDeleting)
                }
                ToolbarItem(placement: .navigationBarTrailing) {
                    Button("Save") {
                        saveContribution()
                    }
                    .foregroundColor(ApproachNoteTheme.brand)
                    .fontWeight(.semibold)
                    .disabled(isSaving || isDeleting || !hasChanges)
                }
            }
            .onAppear {
                loadCurrentValues()
            }
            .disabled(isSaving || isDeleting)
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
            .overlay {
                if isSaving {
                    savingOverlay
                }
            }
        }
    }

    private var savingOverlay: some View {
        ZStack {
            Color.black.opacity(0.3)
                .ignoresSafeArea()
            VStack(spacing: 12) {
                ProgressView()
                    .scaleEffect(1.2)
                Text("Saving...")
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(.white)
            }
            .padding(24)
            .background(Color.black.opacity(0.7))
            .cornerRadius(12)
        }
    }

    private var hasChanges: Bool {
        // Check if any field has a value
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
    RecordingContributionEditView(
        recordingId: "preview-1",
        recordingTitle: "Time Out",
        currentContribution: nil,
        onSave: {}
    )
    .environmentObject(AuthenticationManager())
}

#Preview("Edit Contribution") {
    RecordingContributionEditView(
        recordingId: "preview-2",
        recordingTitle: "Time Out",
        currentContribution: UserContribution.preview,
        onSave: {}
    )
    .environmentObject(AuthenticationManager())
}
