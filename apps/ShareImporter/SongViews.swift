//
//  Untitled.swift
//  Approach Note
//
//  Created by Dave Rodger on 11/1/25.
//

import SwiftUI
// MARK: - Song Import Views

struct SongImportConfirmationView: View {
    let songData: SongData
    let onImport: () -> Void
    let onCancel: () -> Void
    
    var body: some View {
        VStack(spacing: 20) {
            VStack(spacing: 12) {
                Image(systemName: "music.note")
                    .font(.system(size: 50))
                    .foregroundColor(.blue)
                
                Text("Import Song")
                    .font(.title2)
                    .bold()
                
                Text("Review the song information below")
                    .font(.subheadline)
                    .foregroundColor(.secondary)
            }
            .padding(.top, 40)
            
            ScrollView {
                VStack(alignment: .leading, spacing: 16) {
                    InfoRow(label: "Title", value: songData.title)
                    
                    if let composerString = songData.composerString {
                        InfoRow(label: "Composer(s)", value: composerString)
                    }
                    
                    if let workType = songData.workType {
                        InfoRow(label: "Type", value: workType)
                    }
                    
                    if let key = songData.key {
                        InfoRow(label: "Key", value: key)
                    }
                    
                    InfoRow(label: "MusicBrainz ID", value: songData.musicbrainzId)
                }
                .padding()
            }
            .background(Color(.systemGray6))
            .cornerRadius(12)
            .padding(.horizontal)
            
            Spacer()
            
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

struct SongExactMatchView: View {
    let songData: SongData
    let existingSong: ExistingSong
    let onOpenInApp: () -> Void
    let onCancel: () -> Void
    
    var body: some View {
        VStack(spacing: 20) {
            VStack(spacing: 12) {
                Image(systemName: "checkmark.circle.fill")
                    .font(.system(size: 50))
                    .foregroundColor(.green)
                
                Text("Song Already Exists")
                    .font(.title2)
                    .bold()
                
                Text("\(songData.title) is already in the database")
                    .font(.subheadline)
                    .foregroundColor(.secondary)
                    .multilineTextAlignment(.center)
            }
            .padding(.top, 40)
            .padding(.horizontal)
            
            ScrollView {
                VStack(alignment: .leading, spacing: 16) {
                    Text("Database Record")
                        .font(.headline)
                        .foregroundColor(.secondary)
                    
                    InfoRow(label: "Title", value: existingSong.title)
                    
                    if let composer = existingSong.composer {
                        InfoRow(label: "Composer", value: composer)
                    }
                    
                    InfoRow(label: "MusicBrainz ID", value: existingSong.musicbrainzId ?? "None")
                }
                .padding()
            }
            .background(Color(.systemGray6))
            .cornerRadius(12)
            .padding(.horizontal)
            
            Spacer()
            
            VStack(spacing: 12) {
                Button(action: onOpenInApp) {
                    Text("Open in ApproachNote")
                        .font(.headline)
                        .foregroundColor(.white)
                        .frame(maxWidth: .infinity)
                        .padding()
                        .background(Color.blue)
                        .cornerRadius(12)
                }
                
                Button(action: onCancel) {
                    Text("Done")
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

struct SongTitleMatchNoMbidView: View {
    let songData: SongData
    let existingSong: ExistingSong
    let onAssociate: () -> Void
    let onCancel: () -> Void
    
    var body: some View {
        VStack(spacing: 20) {
            VStack(spacing: 12) {
                Image(systemName: "link.circle.fill")
                    .font(.system(size: 50))
                    .foregroundColor(.orange)
                
                Text("Song Found Without ID")
                    .font(.title2)
                    .bold()
                
                Text("A song named \(songData.title) exists but has no MusicBrainz ID")
                    .font(.subheadline)
                    .foregroundColor(.secondary)
                    .multilineTextAlignment(.center)
            }
            .padding(.top, 40)
            .padding(.horizontal)
            
            ScrollView {
                VStack(alignment: .leading, spacing: 20) {
                    VStack(alignment: .leading, spacing: 12) {
                        Text("Current Database Record")
                            .font(.headline)
                            .foregroundColor(.secondary)
                        
                        InfoRow(label: "Title", value: existingSong.title)
                        
                        if let composer = existingSong.composer {
                            InfoRow(label: "Composer", value: composer)
                        } else {
                            InfoRow(label: "Composer", value: "Not set")
                        }
                        
                        InfoRow(label: "MusicBrainz ID", value: "Not set")
                    }
                    .padding()
                    .background(Color(.systemGray5))
                    .cornerRadius(8)
                    
                    HStack {
                        Spacer()
                        Image(systemName: "arrow.down")
                            .foregroundColor(.secondary)
                        Spacer()
                    }
                    
                    VStack(alignment: .leading, spacing: 12) {
                        Text("MusicBrainz Data")
                            .font(.headline)
                            .foregroundColor(.secondary)
                        
                        InfoRow(label: "Title", value: songData.title)
                        
                        if let composerString = songData.composerString {
                            InfoRow(label: "Composer(s)", value: composerString)
                        }
                        
                        InfoRow(label: "MusicBrainz ID", value: songData.musicbrainzId)
                    }
                    .padding()
                    .background(Color.blue.opacity(0.1))
                    .cornerRadius(8)
                }
                .padding(.horizontal)
            }
            
            Spacer()
            
            VStack(spacing: 12) {
                Button(action: onAssociate) {
                    Text("Associate MusicBrainz ID")
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

struct SongTitleMatchDifferentMbidView: View {
    let songData: SongData
    let existingSong: ExistingSong
    let onOverwrite: () -> Void
    let onCancel: () -> Void
    
    var body: some View {
        VStack(spacing: 20) {
            VStack(spacing: 12) {
                Image(systemName: "exclamationmark.triangle.fill")
                    .font(.system(size: 50))
                    .foregroundColor(.red)
                
                Text("Different Song Found")
                    .font(.title2)
                    .bold()
                
                Text("A song named \(songData.title) exists with a different MusicBrainz ID")
                    .font(.subheadline)
                    .foregroundColor(.secondary)
                    .multilineTextAlignment(.center)
            }
            .padding(.top, 40)
            .padding(.horizontal)
            
            ScrollView {
                VStack(alignment: .leading, spacing: 20) {
                    VStack(alignment: .leading, spacing: 12) {
                        Text("Current Database Record")
                            .font(.headline)
                            .foregroundColor(.secondary)
                        
                        InfoRow(label: "Title", value: existingSong.title)
                        
                        if let composer = existingSong.composer {
                            InfoRow(label: "Composer", value: composer)
                        }
                        
                        InfoRow(label: "MusicBrainz ID", value: existingSong.musicbrainzId ?? "None")
                    }
                    .padding()
                    .background(Color(.systemGray5))
                    .cornerRadius(8)
                    
                    HStack {
                        Spacer()
                        Text("VS")
                            .font(.headline)
                            .foregroundColor(.red)
                        Spacer()
                    }
                    
                    VStack(alignment: .leading, spacing: 12) {
                        Text("MusicBrainz Data")
                            .font(.headline)
                            .foregroundColor(.secondary)
                        
                        InfoRow(label: "Title", value: songData.title)
                        
                        if let composerString = songData.composerString {
                            InfoRow(label: "Composer(s)", value: composerString)
                        }
                        
                        InfoRow(label: "MusicBrainz ID", value: songData.musicbrainzId)
                    }
                    .padding()
                    .background(Color.red.opacity(0.1))
                    .cornerRadius(8)
                }
                .padding(.horizontal)
            }
            
            VStack(spacing: 8) {
                Text("⚠️ Warning")
                    .font(.headline)
                    .foregroundColor(.red)
                
                Text("These may be different songs with the same title. Overwriting will replace the existing MusicBrainz ID.")
                    .font(.caption)
                    .foregroundColor(.secondary)
                    .multilineTextAlignment(.center)
            }
            .padding(.horizontal)
            
            Spacer()
            
            VStack(spacing: 12) {
                Button(action: onOverwrite) {
                    Text("Overwrite MusicBrainz ID")
                        .font(.headline)
                        .foregroundColor(.white)
                        .frame(maxWidth: .infinity)
                        .padding()
                        .background(Color.red)
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
