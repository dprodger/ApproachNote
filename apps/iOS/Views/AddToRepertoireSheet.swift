//
//  AddToRepertoireSheet.swift
//  Approach Note
//
//  Created by Dave Rodger on 11/12/25.
//  UPDATED FOR PHASE 5: Added authentication check
//

import SwiftUI
import os

struct AddToRepertoireSheet: View {
    let songId: String
    let songTitle: String
    @ObservedObject var repertoireManager: RepertoireManager
    @Binding var isPresented: Bool
    let onSuccess: (String) -> Void
    let onError: (String) -> Void
    
    @State private var isAdding = false
    @State private var isLoadingRepertoires = false
    
    var body: some View {
        NavigationStack {
            Group {
                // Check authentication first
                if !repertoireManager.isAuthenticated {
                    authRequiredView
                } else if isLoadingRepertoires {
                    loadingView
                } else if repertoireManager.addableRepertoires.isEmpty {
                    emptyRepertoiresView
                } else {
                    repertoireList
                }
            }
            .navigationTitle("Add \"\(songTitle)\"")
            .navigationBarTitleDisplayMode(.inline)
            .toolbarBackground(ApproachNoteTheme.brand, for: .navigationBar)
            .toolbarBackground(.visible, for: .navigationBar)
            .toolbarColorScheme(.dark, for: .navigationBar)
            .toolbar {
                ToolbarItem(placement: .navigationBarLeading) {
                    Button("Cancel") {
                        isPresented = false
                    }
                    .foregroundColor(.white)
                    .disabled(isAdding)
                }
            }
            .overlay {
                if isAdding {
                    ZStack {
                        Color.black.opacity(0.3)
                            .ignoresSafeArea()
                        
                        VStack(spacing: 16) {
                            ProgressView()
                                .tint(.white)
                                .scaleEffect(1.5)
                            Text("Adding to repertoire...")
                                .foregroundColor(.white)
                                .font(ApproachNoteTheme.headline())
                        }
                        .padding(30)
                        .background(ApproachNoteTheme.textPrimary)
                        .cornerRadius(16)
                    }
                }
            }
            .task {
                // Only load repertoires if authenticated
                if repertoireManager.isAuthenticated &&
                   (repertoireManager.repertoires.isEmpty ||
                    repertoireManager.addableRepertoires.isEmpty) {
                    isLoadingRepertoires = true
                    await repertoireManager.loadRepertoires()
                    isLoadingRepertoires = false
                    
                    // Debug logging
                    let totalCount = repertoireManager.repertoires.count
                    let addableCount = repertoireManager.addableRepertoires.count
                    Log.ui.debug("Loaded \(totalCount, privacy: .public) total repertoires, \(addableCount, privacy: .public) addable")
                    for rep in repertoireManager.addableRepertoires {
                        let name = rep.name
                        let id = rep.id
                        Log.ui.debug("  - \(name, privacy: .public) (ID: \(id, privacy: .private))")
                    }
                }
            }
        }
    }
    
    // MARK: - Auth Required View
    
    private var authRequiredView: some View {
        VStack(spacing: 24) {
            Image(systemName: "lock.fill")
                .font(.system(size: 60))
                .foregroundColor(ApproachNoteTheme.brand.opacity(0.6))
            
            Text("Sign In Required")
                .font(ApproachNoteTheme.title2())
                .fontWeight(.semibold)
                .foregroundColor(ApproachNoteTheme.textPrimary)
            
            Text("You need to be signed in to add songs to repertoires")
                .font(ApproachNoteTheme.body())
                .foregroundColor(.secondary)
                .multilineTextAlignment(.center)
                .padding(.horizontal, 32)
            
            ApproachNoteButton("Close") {
                isPresented = false
            }
            .padding(.horizontal, 32)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(ApproachNoteTheme.background)
    }

    // MARK: - Loading View
    
    private var loadingView: some View {
        VStack(spacing: 16) {
            ProgressView()
                .tint(ApproachNoteTheme.brand)
            Text("Loading repertoires...")
                .foregroundColor(ApproachNoteTheme.textSecondary)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(ApproachNoteTheme.background)
    }
    
    // MARK: - Empty Repertoires View
    
    private var emptyRepertoiresView: some View {
        VStack(spacing: 20) {
            Image(systemName: "music.note.list")
                .font(.system(size: 60))
                .foregroundColor(ApproachNoteTheme.textSecondary.opacity(0.5))
            
            Text("No Repertoires Yet")
                .font(ApproachNoteTheme.title2())
                .fontWeight(.semibold)
                .foregroundColor(ApproachNoteTheme.textPrimary)
            
            Text("Create a repertoire first to start organizing your songs.")
                .font(ApproachNoteTheme.subheadline())
                .foregroundColor(ApproachNoteTheme.textSecondary)
                .multilineTextAlignment(.center)
                .padding(.horizontal, 40)
            
            // TODO: Add "Create Repertoire" button when that feature is implemented
            ApproachNoteButton("Close") {
                isPresented = false
            }
            .fixedSize(horizontal: true, vertical: false)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(ApproachNoteTheme.background)
    }
    
    // MARK: - Repertoire List
    
    private var repertoireList: some View {
        List {
            // Quick add to last used (if available)
            if let lastUsed = repertoireManager.lastUsedRepertoire {
                Section {
                    Button(action: {
                        addToRepertoire(lastUsed)
                    }) {
                        HStack {
                            VStack(alignment: .leading, spacing: 4) {
                                Text("Add to \(lastUsed.name)")
                                    .font(ApproachNoteTheme.headline())
                                    .foregroundColor(ApproachNoteTheme.textPrimary)
                                Text("Last used")
                                    .font(ApproachNoteTheme.caption())
                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                            }
                            Spacer()
                            Image(systemName: "arrow.right.circle.fill")
                                .foregroundColor(ApproachNoteTheme.accent)
                                .font(ApproachNoteTheme.title2())
                        }
                    }
                    .disabled(isAdding)
                    .listRowBackground(ApproachNoteTheme.accent.opacity(0.1))
                } header: {
                    Text("Quick Add")
                }
            }
            
            // All available repertoires
            Section {
                ForEach(repertoireManager.addableRepertoires) { repertoire in
                    Button(action: {
                        addToRepertoire(repertoire)
                    }) {
                        HStack {
                            VStack(alignment: .leading, spacing: 4) {
                                Text(repertoire.name)
                                    .font(ApproachNoteTheme.headline())
                                    .foregroundColor(ApproachNoteTheme.textPrimary)
                                
                                if let description = repertoire.description {
                                    Text(description)
                                        .font(ApproachNoteTheme.subheadline())
                                        .foregroundColor(ApproachNoteTheme.textSecondary)
                                        .lineLimit(2)
                                }
                                
                                Text("\(repertoire.songCount) songs")
                                    .font(ApproachNoteTheme.caption())
                                    .foregroundColor(ApproachNoteTheme.brand)
                            }
                            Spacer()
                        }
                    }
                    .disabled(isAdding)
                    .listRowBackground(ApproachNoteTheme.surface)
                }
            } header: {
                Text("All Repertoires")
            }
        }
        .listStyle(.insetGrouped)
        .scrollContentBackground(.hidden)
        .background(ApproachNoteTheme.background)
    }
    
    // MARK: - Add to Repertoire Action
    
    private func addToRepertoire(_ repertoire: Repertoire) {
        isAdding = true
        
        Task {
            let success = await repertoireManager.addSongToRepertoire(
                songId: songId,
                repertoireId: repertoire.id
            )
            
            await MainActor.run {
                isAdding = false
                
                if success {
                    // Update last used repertoire
                    repertoireManager.setLastUsedRepertoire(repertoire)
                    
                    isPresented = false
                    onSuccess("Added \"\(songTitle)\" to \(repertoire.name)")
                } else {
                    isPresented = false
                    let errorMessage = repertoireManager.errorMessage ?? "Failed to add song"
                    onError(errorMessage)
                }
            }
        }
    }
}
