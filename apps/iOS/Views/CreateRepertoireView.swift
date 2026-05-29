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
            // Style the nav bar from the live palette (the global
            // UINavigationBar appearance is set once at launch and goes stale
            // when the palette changes), matching jazzNavigationBar.
            .toolbarBackground(ApproachNoteTheme.brand, for: .navigationBar)
            .toolbarBackground(.visible, for: .navigationBar)
            .toolbarColorScheme(.dark, for: .navigationBar)
            .toolbar {
                ToolbarItem(placement: .navigationBarLeading) {
                    Button("Cancel") {
                        dismiss()
                    }
                    .foregroundColor(ApproachNoteTheme.textOnDark)
                }

                ToolbarItem(placement: .navigationBarTrailing) {
                    Button("Create") {
                        createRepertoire()
                    }
                    .fontWeight(.semibold)
                    .foregroundColor(ApproachNoteTheme.textOnDark)
                    .disabled(name.trimmingCharacters(in: .whitespaces).isEmpty || isCreating)
                }
            }
            .disabled(isCreating)
            .overlay {
                if isCreating {
                    ZStack {
                        Color.black.opacity(0.3)
                            .ignoresSafeArea()
                        
                        ThemedProgressView(message: "Creating repertoire...",
                                           tintColor: ApproachNoteTheme.brand)
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
        .presentationDetents([.medium, .large])
        .presentationDragIndicator(.visible)
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

