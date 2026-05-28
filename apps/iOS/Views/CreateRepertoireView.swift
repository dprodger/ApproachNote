//
//  CreateRepertoireView.swift
//  Approach Note
//
//  Created by Dave Rodger on 11/22/25.
//
import SwiftUI

// MARK: - Create Repertoire View

struct CreateRepertoireView: View {
    @ObservedObject var repertoireManager: RepertoireManager
    @Environment(\.dismiss) var dismiss
    
    @State private var name: String = ""
    @State private var description: String = ""
    @State private var isCreating = false
    @State private var showError = false
    @State private var errorMessage = ""
    
    var body: some View {
        NavigationStack {
            Form {
                Section {
                    TextField("Repertoire Name", text: $name)
                        .autocapitalization(.words)
                } header: {
                    Text("Name")
                } footer: {
                    Text("Give your repertoire a descriptive name")
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                }
                
                Section {
                    TextField("Description (optional)", text: $description, axis: .vertical)
                        .lineLimit(3...6)
                } header: {
                    Text("Description")
                } footer: {
                    Text("Add notes about what this repertoire contains")
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                }
            }
            .scrollContentBackground(.hidden)
            .background(ApproachNoteTheme.background)
            .navigationTitle("Create Repertoire")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .navigationBarLeading) {
                    Button("Cancel") {
                        dismiss()
                    }
                    .foregroundColor(ApproachNoteTheme.brand)
                }
                
                ToolbarItem(placement: .navigationBarTrailing) {
                    Button("Create") {
                        createRepertoire()
                    }
                    .foregroundColor(ApproachNoteTheme.brand)
                    .disabled(name.trimmingCharacters(in: .whitespaces).isEmpty || isCreating)
                }
            }
            .disabled(isCreating)
            .overlay {
                if isCreating {
                    ZStack {
                        Color.black.opacity(0.3)
                            .ignoresSafeArea()
                        
                        VStack(spacing: ApproachNoteTheme.spacingMD) {
                            ProgressView()
                                .tint(ApproachNoteTheme.brand)
                            Text("Creating repertoire...")
                                .foregroundColor(ApproachNoteTheme.textPrimary)
                        }
                        .padding(ApproachNoteTheme.spacingXL)
                        .background(ApproachNoteTheme.surface)
                        .cornerRadius(12)
                    }
                }
            }
            .alert("Error", isPresented: $showError) {
                Button("OK", role: .cancel) { }
            } message: {
                Text(errorMessage)
            }
        }
    }
    
    private func createRepertoire() {
        let trimmedName = name.trimmingCharacters(in: .whitespaces)
        guard !trimmedName.isEmpty else { return }
        
        let trimmedDescription = description.trimmingCharacters(in: .whitespaces)
        let finalDescription = trimmedDescription.isEmpty ? nil : trimmedDescription
        
        isCreating = true
        
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
                    errorMessage = repertoireManager.errorMessage ?? "Failed to create repertoire"
                    showError = true
                }
            }
        }
    }
}

