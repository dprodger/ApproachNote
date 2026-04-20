//
//  ArtistImportConfirmationView.swift
//  Approach Note
//
//  Created by Dave Rodger on 11/1/25.
//

import SwiftUI

struct ArtistImportConfirmationView: View {
    let artistData: ArtistData
    let onImport: () -> Void
    let onCancel: () -> Void
    
    var body: some View {
        VStack(spacing: 20) {
            // Header
            VStack(spacing: 8) {
                Image(systemName: "person.badge.plus")
                    .font(.system(size: 50))
                    .foregroundColor(.blue)
                
                Text("Import Artist")
                    .font(.title2)
                    .bold()
            }
            .padding(.top, 40)
            
            // Artist information
            ScrollView {
                VStack(alignment: .leading, spacing: 16) {
                    InfoRow(label: "Name", value: artistData.name)
                    
                    InfoRow(label: "MusicBrainz ID", value: artistData.musicbrainzId)
                    
                }
                .padding()
            }
            .background(Color(.systemGray6))
            .cornerRadius(12)
            .padding(.horizontal)
            
            Spacer()
            
            // Action buttons
            VStack(spacing: 12) {
                Button(action: onImport) {
                    Text("Import to ApproachNote")
                        .font(.headline)
                        .foregroundColor(.white)
                        .frame(maxWidth: .infinity)
                        .padding()
                        .background(Color.blue)
                        .cornerRadius(12)
                }
                
                Button(action: onCancel) {
                    Text("Cancel")
                        .font(.headline)
                        .foregroundColor(.blue)
                        .frame(maxWidth: .infinity)
                        .padding()
                        .background(Color(.systemGray6))
                        .cornerRadius(12)
                }
            }
            .padding(.horizontal)
            .padding(.bottom, 20)
        }
        .background(Color.white)
    }
}

struct ArtistExactMatchView: View {
    let artistData: ArtistData
    let existingArtist: ExistingArtist
    let onOpenInApp: () -> Void
    let onCancel: () -> Void
    
    var body: some View {
        VStack(spacing: 20) {
            // Header with warning icon
            VStack(spacing: 12) {
                Image(systemName: "exclamationmark.triangle.fill")
                    .font(.system(size: 50))
                    .foregroundColor(.orange)
                
                Text("Artist Already Exists")
                    .font(.title2)
                    .bold()
                
                Text("\(artistData.name) is already in the database")
                    .font(.subheadline)
                    .foregroundColor(.secondary)
                    .multilineTextAlignment(.center)
            }
            .padding(.top, 40)
            .padding(.horizontal)
            
            // Comparison
            ScrollView {
                VStack(alignment: .leading, spacing: 16) {
                    Text("Database Record")
                        .font(.headline)
                        .foregroundColor(.secondary)
                    
                    InfoRow(label: "Name", value: existingArtist.name)
                    InfoRow(label: "MusicBrainz ID", value: existingArtist.musicbrainzId ?? "None")
                    
                    if let bio = existingArtist.biography, !bio.isEmpty {
                        VStack(alignment: .leading, spacing: 4) {
                            Text("Biography")
                                .font(.caption)
                                .foregroundColor(.secondary)
                            Text(existingArtist.shortBio)
                                .font(.caption)
                                .foregroundColor(.secondary)
                        }
                    }
                }
                .padding()
            }
            .background(Color(.systemGray6))
            .cornerRadius(12)
            .padding(.horizontal)
            
            Spacer()
            
            // Action buttons
            VStack(spacing: 12) {
                Button(action: onOpenInApp) {
                    HStack {
                        Image(systemName: "arrow.up.forward.app")
                        Text("View in ApproachNote")
                    }
                    .font(.headline)
                    .foregroundColor(.white)
                    .frame(maxWidth: .infinity)
                    .padding()
                    .background(Color.blue)
                    .cornerRadius(12)
                }
                
                Button(action: onCancel) {
                    Text("Close")
                        .font(.headline)
                        .foregroundColor(.blue)
                        .frame(maxWidth: .infinity)
                        .padding()
                        .background(Color(.systemGray6))
                        .cornerRadius(12)
                }
            }
            .padding(.horizontal)
            .padding(.bottom, 20)
        }
        .background(Color.white)
    }
}

struct ArtistNameMatchNoMbidView: View {
    let artistData: ArtistData
    let existingArtist: ExistingArtist
    let onAssociate: () -> Void
    let onCancel: () -> Void
    
    var body: some View {
        VStack(spacing: 20) {
            // Header
            VStack(spacing: 12) {
                Image(systemName: "link.badge.plus")
                    .font(.system(size: 50))
                    .foregroundColor(.blue)
                
                Text("Associate MusicBrainz ID?")
                    .font(.title2)
                    .bold()
                
                Text("An artist named \(artistData.name) exists without a MusicBrainz ID")
                    .font(.subheadline)
                    .foregroundColor(.secondary)
                    .multilineTextAlignment(.center)
            }
            .padding(.top, 40)
            .padding(.horizontal)
            
            // Comparison
            ScrollView {
                VStack(alignment: .leading, spacing: 20) {
                    // Existing artist
                    VStack(alignment: .leading, spacing: 12) {
                        Text("Current Database Record")
                            .font(.headline)
                            .foregroundColor(.secondary)
                        
                        InfoRow(label: "Name", value: existingArtist.name)
                        InfoRow(label: "MusicBrainz ID", value: "None (blank)")
                            .foregroundColor(.orange)
                    }
                    .padding()
                    .background(Color(.systemGray6))
                    .cornerRadius(8)
                    
                    // Arrow
                    HStack {
                        Spacer()
                        Image(systemName: "arrow.down")
                            .foregroundColor(.blue)
                            .font(.title3)
                        Spacer()
                    }
                    
                    // New data
                    VStack(alignment: .leading, spacing: 12) {
                        Text("From MusicBrainz")
                            .font(.headline)
                            .foregroundColor(.secondary)
                        
                        InfoRow(label: "Name", value: artistData.name)
                        InfoRow(label: "MusicBrainz ID", value: artistData.musicbrainzId)
                            .foregroundColor(.green)
                    }
                    .padding()
                    .background(Color(.systemGray6))
                    .cornerRadius(8)
                }
                .padding()
            }
            
            Spacer()
            
            // Action buttons
            VStack(spacing: 12) {
                Button(action: onAssociate) {
                    Text("Associate ID with Existing Artist")
                        .font(.headline)
                        .foregroundColor(.white)
                        .frame(maxWidth: .infinity)
                        .padding()
                        .background(Color.blue)
                        .cornerRadius(12)
                }
                
                Button(action: onCancel) {
                    Text("Cancel")
                        .font(.headline)
                        .foregroundColor(.blue)
                        .frame(maxWidth: .infinity)
                        .padding()
                        .background(Color(.systemGray6))
                        .cornerRadius(12)
                }
            }
            .padding(.horizontal)
            .padding(.bottom, 20)
        }
        .background(Color.white)
    }
}

struct ArtistNameMatchDifferentMbidView: View {
    let artistData: ArtistData
    let existingArtist: ExistingArtist
    let onOverwrite: () -> Void
    let onCancel: () -> Void
    
    var body: some View {
        VStack(spacing: 20) {
            // Header with warning
            VStack(spacing: 12) {
                Image(systemName: "exclamationmark.triangle.fill")
                    .font(.system(size: 50))
                    .foregroundColor(.red)
                
                Text("Different Artist Found")
                    .font(.title2)
                    .bold()
                
                Text("An artist named \(artistData.name) exists with a different MusicBrainz ID")
                    .font(.subheadline)
                    .foregroundColor(.secondary)
                    .multilineTextAlignment(.center)
            }
            .padding(.top, 40)
            .padding(.horizontal)
            
            // Comparison
            ScrollView {
                VStack(alignment: .leading, spacing: 20) {
                    // Existing artist
                    VStack(alignment: .leading, spacing: 12) {
                        Text("Current Database Record")
                            .font(.headline)
                            .foregroundColor(.secondary)
                        
                        InfoRow(label: "Name", value: existingArtist.name)
                        InfoRow(label: "MusicBrainz ID", value: existingArtist.musicbrainzId ?? "None")
                            .foregroundColor(.orange)
                        
                        if let bio = existingArtist.biography, !bio.isEmpty {
                            VStack(alignment: .leading, spacing: 4) {
                                Text("Biography")
                                    .font(.caption)
                                    .foregroundColor(.secondary)
                                Text(existingArtist.shortBio)
                                    .font(.caption)
                                    .foregroundColor(.secondary)
                            }
                        }
                    }
                    .padding()
                    .background(Color(.systemGray6))
                    .cornerRadius(8)
                    
                    Divider()
                    
                    // New data
                    VStack(alignment: .leading, spacing: 12) {
                        Text("From MusicBrainz (New)")
                            .font(.headline)
                            .foregroundColor(.secondary)
                        
                        InfoRow(label: "Name", value: artistData.name)
                        InfoRow(label: "MusicBrainz ID", value: artistData.musicbrainzId)
                            .foregroundColor(.red)
                    }
                    .padding()
                    .background(Color(.systemGray6))
                    .cornerRadius(8)
                    
                    Text("⚠️ These appear to be different artists with the same name")
                        .font(.caption)
                        .foregroundColor(.orange)
                        .padding()
                        .frame(maxWidth: .infinity)
                        .background(Color.orange.opacity(0.1))
                        .cornerRadius(8)
                }
                .padding()
            }
            
            Spacer()
            
            // Action buttons
            VStack(spacing: 12) {
                Button(action: onOverwrite) {
                    VStack(spacing: 4) {
                        Text("Overwrite with New Information")
                            .font(.headline)
                        Text("(This will replace the existing record)")
                            .font(.caption)
                    }
                    .foregroundColor(.white)
                    .frame(maxWidth: .infinity)
                    .padding()
                    .background(Color.red)
                    .cornerRadius(12)
                }
                
                Button(action: onCancel) {
                    Text("Cancel - Keep Existing")
                        .font(.headline)
                        .foregroundColor(.blue)
                        .frame(maxWidth: .infinity)
                        .padding()
                        .background(Color(.systemGray6))
                        .cornerRadius(12)
                }
            }
            .padding(.horizontal)
            .padding(.bottom, 20)
        }
        .background(Color.white)
    }
}
