//
//  MacExtensionViews.swift
//  ShareImporterMac
//
//  SwiftUI views for the macOS Share Extension
//

import SwiftUI

// MARK: - Common Views

struct MacLoadingView: View {
    var body: some View {
        VStack(spacing: 20) {
            ProgressView()
                .scaleEffect(1.5)

            Text("Checking database...")
                .font(.subheadline)
                .foregroundColor(.secondary)
        }
        .frame(width: 400, height: 350)
    }
}

struct MacErrorView: View {
    let message: String
    let onDismiss: () -> Void

    var body: some View {
        VStack(spacing: 20) {
            Image(systemName: "xmark.circle.fill")
                .font(.system(size: 50))
                .foregroundColor(.red)

            Text("Error")
                .font(.title2)
                .bold()

            Text(message)
                .font(.body)
                .foregroundColor(.secondary)
                .multilineTextAlignment(.center)
                .padding(.horizontal)

            Spacer()

            Button(action: onDismiss) {
                Text("Close")
                    .frame(maxWidth: .infinity)
            }
            .buttonStyle(.bordered)
            .controlSize(.large)
            .padding(.horizontal)
            .padding(.bottom, 20)
        }
        .padding(.top, 40)
        .frame(width: 400, height: 350)
    }
}

struct MacLoginRequiredView: View {
    let onDismiss: () -> Void

    var body: some View {
        VStack(spacing: 20) {
            Image(systemName: "person.crop.circle.badge.exclamationmark")
                .font(.system(size: 50))
                .foregroundColor(.orange)

            Text("Login Required")
                .font(.title2)
                .bold()

            Text("Please open ApproachNote and log in to import songs or artists.")
                .font(.body)
                .foregroundColor(.secondary)
                .multilineTextAlignment(.center)
                .padding(.horizontal)

            Spacer()

            Button(action: onDismiss) {
                Text("Close")
                    .frame(maxWidth: .infinity)
            }
            .buttonStyle(.bordered)
            .controlSize(.large)
            .padding(.horizontal)
            .padding(.bottom, 20)
        }
        .padding(.top, 40)
        .frame(width: 400, height: 350)
    }
}

struct MacSuccessView: View {
    var body: some View {
        VStack(spacing: 20) {
            Image(systemName: "checkmark.circle.fill")
                .font(.system(size: 60))
                .foregroundColor(.green)

            Text("Saved!")
                .font(.title2)
                .bold()

            Text("Opening ApproachNote...")
                .font(.subheadline)
                .foregroundColor(.secondary)
        }
        .frame(width: 400, height: 350)
    }
}

struct MacNotImplementedView: View {
    let feature: String
    let onDismiss: () -> Void

    var body: some View {
        VStack(spacing: 20) {
            Image(systemName: "wrench.and.screwdriver")
                .font(.system(size: 50))
                .foregroundColor(.orange)

            Text("Coming Soon")
                .font(.title2)
                .bold()

            Text("\(feature) will be implemented in a future update")
                .font(.body)
                .foregroundColor(.secondary)
                .multilineTextAlignment(.center)
                .padding(.horizontal)

            Spacer()

            Button(action: onDismiss) {
                Text("Close")
                    .frame(maxWidth: .infinity)
            }
            .buttonStyle(.bordered)
            .controlSize(.large)
            .padding(.horizontal)
            .padding(.bottom, 20)
        }
        .padding(.top, 40)
        .frame(width: 400, height: 350)
    }
}

struct MacInfoRow: View {
    let label: String
    let value: String

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            Text(label)
                .font(.caption)
                .foregroundColor(.secondary)
            Text(value)
                .font(.body)
        }
    }
}

// MARK: - Artist Import Views

struct MacArtistImportConfirmationView: View {
    let artistData: ArtistData
    let onImport: () -> Void
    let onCancel: () -> Void

    var body: some View {
        VStack(spacing: 16) {
            // Header
            VStack(spacing: 8) {
                Image(systemName: "person.badge.plus")
                    .font(.system(size: 40))
                    .foregroundColor(.blue)

                Text("Import Artist")
                    .font(.title2)
                    .bold()
            }
            .padding(.top, 20)

            // Artist information
            VStack(alignment: .leading, spacing: 12) {
                MacInfoRow(label: "Name", value: artistData.name)
                MacInfoRow(label: "MusicBrainz ID", value: artistData.musicbrainzId)
            }
            .padding()
            .frame(maxWidth: .infinity, alignment: .leading)
            .background(Color(NSColor.controlBackgroundColor))
            .cornerRadius(8)
            .padding(.horizontal)

            Spacer()

            // Action buttons
            VStack(spacing: 10) {
                Button(action: onImport) {
                    Text("Import to ApproachNote")
                        .frame(maxWidth: .infinity)
                }
                .buttonStyle(.borderedProminent)
                .controlSize(.large)

                Button(action: onCancel) {
                    Text("Cancel")
                        .frame(maxWidth: .infinity)
                }
                .buttonStyle(.bordered)
                .controlSize(.large)
            }
            .padding(.horizontal)
            .padding(.bottom, 20)
        }
        .frame(width: 400, height: 350)
    }
}

struct MacArtistExactMatchView: View {
    let artistData: ArtistData
    let existingArtist: ExistingArtist
    let onOpenInApp: () -> Void
    let onCancel: () -> Void

    var body: some View {
        VStack(spacing: 16) {
            // Header with warning icon
            VStack(spacing: 8) {
                Image(systemName: "exclamationmark.triangle.fill")
                    .font(.system(size: 40))
                    .foregroundColor(.orange)

                Text("Artist Already Exists")
                    .font(.title2)
                    .bold()

                Text("\(artistData.name) is already in the database")
                    .font(.subheadline)
                    .foregroundColor(.secondary)
                    .multilineTextAlignment(.center)
            }
            .padding(.top, 20)
            .padding(.horizontal)

            // Comparison
            ScrollView {
                VStack(alignment: .leading, spacing: 12) {
                    Text("Database Record")
                        .font(.headline)
                        .foregroundColor(.secondary)

                    MacInfoRow(label: "Name", value: existingArtist.name)
                    MacInfoRow(label: "MusicBrainz ID", value: existingArtist.musicbrainzId ?? "None")
                }
                .padding()
            }
            .background(Color(NSColor.controlBackgroundColor))
            .cornerRadius(8)
            .padding(.horizontal)

            Spacer()

            // Action buttons
            VStack(spacing: 10) {
                Button(action: onOpenInApp) {
                    HStack {
                        Image(systemName: "arrow.up.forward.app")
                        Text("View in ApproachNote")
                    }
                    .frame(maxWidth: .infinity)
                }
                .buttonStyle(.borderedProminent)
                .controlSize(.large)

                Button(action: onCancel) {
                    Text("Close")
                        .frame(maxWidth: .infinity)
                }
                .buttonStyle(.bordered)
                .controlSize(.large)
            }
            .padding(.horizontal)
            .padding(.bottom, 20)
        }
        .frame(width: 400, height: 400)
    }
}

struct MacArtistNameMatchNoMbidView: View {
    let artistData: ArtistData
    let existingArtist: ExistingArtist
    let onAssociate: () -> Void
    let onCancel: () -> Void

    var body: some View {
        VStack(spacing: 16) {
            // Header
            VStack(spacing: 8) {
                Image(systemName: "link.badge.plus")
                    .font(.system(size: 40))
                    .foregroundColor(.blue)

                Text("Associate MusicBrainz ID?")
                    .font(.title2)
                    .bold()

                Text("An artist named \(artistData.name) exists without a MusicBrainz ID")
                    .font(.subheadline)
                    .foregroundColor(.secondary)
                    .multilineTextAlignment(.center)
            }
            .padding(.top, 20)
            .padding(.horizontal)

            // Comparison
            ScrollView {
                VStack(alignment: .leading, spacing: 16) {
                    // Existing artist
                    VStack(alignment: .leading, spacing: 8) {
                        Text("Current Database Record")
                            .font(.headline)
                            .foregroundColor(.secondary)

                        MacInfoRow(label: "Name", value: existingArtist.name)
                        MacInfoRow(label: "MusicBrainz ID", value: "None (blank)")
                    }
                    .padding()
                    .background(Color(NSColor.controlBackgroundColor))
                    .cornerRadius(8)

                    // Arrow
                    HStack {
                        Spacer()
                        Image(systemName: "arrow.down")
                            .foregroundColor(.blue)
                        Spacer()
                    }

                    // New data
                    VStack(alignment: .leading, spacing: 8) {
                        Text("From MusicBrainz")
                            .font(.headline)
                            .foregroundColor(.secondary)

                        MacInfoRow(label: "Name", value: artistData.name)
                        MacInfoRow(label: "MusicBrainz ID", value: artistData.musicbrainzId)
                    }
                    .padding()
                    .background(Color(NSColor.controlBackgroundColor))
                    .cornerRadius(8)
                }
                .padding(.horizontal)
            }

            Spacer()

            // Action buttons
            VStack(spacing: 10) {
                Button(action: onAssociate) {
                    Text("Associate ID with Existing Artist")
                        .frame(maxWidth: .infinity)
                }
                .buttonStyle(.borderedProminent)
                .controlSize(.large)

                Button(action: onCancel) {
                    Text("Cancel")
                        .frame(maxWidth: .infinity)
                }
                .buttonStyle(.bordered)
                .controlSize(.large)
            }
            .padding(.horizontal)
            .padding(.bottom, 20)
        }
        .frame(width: 400, height: 450)
    }
}

struct MacArtistNameMatchDifferentMbidView: View {
    let artistData: ArtistData
    let existingArtist: ExistingArtist
    let onOverwrite: () -> Void
    let onCancel: () -> Void

    var body: some View {
        VStack(spacing: 16) {
            // Header with warning
            VStack(spacing: 8) {
                Image(systemName: "exclamationmark.triangle.fill")
                    .font(.system(size: 40))
                    .foregroundColor(.red)

                Text("Different Artist Found")
                    .font(.title2)
                    .bold()

                Text("An artist named \(artistData.name) exists with a different MusicBrainz ID")
                    .font(.subheadline)
                    .foregroundColor(.secondary)
                    .multilineTextAlignment(.center)
            }
            .padding(.top, 20)
            .padding(.horizontal)

            // Comparison
            ScrollView {
                VStack(alignment: .leading, spacing: 16) {
                    // Existing artist
                    VStack(alignment: .leading, spacing: 8) {
                        Text("Current Database Record")
                            .font(.headline)
                            .foregroundColor(.secondary)

                        MacInfoRow(label: "Name", value: existingArtist.name)
                        MacInfoRow(label: "MusicBrainz ID", value: existingArtist.musicbrainzId ?? "None")
                    }
                    .padding()
                    .background(Color(NSColor.controlBackgroundColor))
                    .cornerRadius(8)

                    Divider()

                    // New data
                    VStack(alignment: .leading, spacing: 8) {
                        Text("From MusicBrainz (New)")
                            .font(.headline)
                            .foregroundColor(.secondary)

                        MacInfoRow(label: "Name", value: artistData.name)
                        MacInfoRow(label: "MusicBrainz ID", value: artistData.musicbrainzId)
                    }
                    .padding()
                    .background(Color(NSColor.controlBackgroundColor))
                    .cornerRadius(8)

                    Text("These appear to be different artists with the same name")
                        .font(.caption)
                        .foregroundColor(.orange)
                        .padding()
                        .frame(maxWidth: .infinity)
                        .background(Color.orange.opacity(0.1))
                        .cornerRadius(8)
                }
                .padding(.horizontal)
            }

            Spacer()

            // Action buttons
            VStack(spacing: 10) {
                Button(action: onOverwrite) {
                    Text("Overwrite with New Information")
                        .frame(maxWidth: .infinity)
                }
                .buttonStyle(.borderedProminent)
                .tint(.red)
                .controlSize(.large)

                Button(action: onCancel) {
                    Text("Cancel - Keep Existing")
                        .frame(maxWidth: .infinity)
                }
                .buttonStyle(.bordered)
                .controlSize(.large)
            }
            .padding(.horizontal)
            .padding(.bottom, 20)
        }
        .frame(width: 400, height: 480)
    }
}

// MARK: - Song Import Views

struct MacSongImportConfirmationView: View {
    let songData: SongData
    let onImport: () -> Void
    let onCancel: () -> Void

    var body: some View {
        VStack(spacing: 16) {
            VStack(spacing: 8) {
                Image(systemName: "music.note")
                    .font(.system(size: 40))
                    .foregroundColor(.blue)

                Text("Import Song")
                    .font(.title2)
                    .bold()

                Text("Review the song information below")
                    .font(.subheadline)
                    .foregroundColor(.secondary)
            }
            .padding(.top, 20)

            ScrollView {
                VStack(alignment: .leading, spacing: 12) {
                    MacInfoRow(label: "Title", value: songData.title)

                    if let composerString = songData.composerString {
                        MacInfoRow(label: "Composer(s)", value: composerString)
                    }

                    if let workType = songData.workType {
                        MacInfoRow(label: "Type", value: workType)
                    }

                    if let key = songData.key {
                        MacInfoRow(label: "Key", value: key)
                    }

                    MacInfoRow(label: "MusicBrainz ID", value: songData.musicbrainzId)
                }
                .padding()
            }
            .background(Color(NSColor.controlBackgroundColor))
            .cornerRadius(8)
            .padding(.horizontal)

            Spacer()

            VStack(spacing: 10) {
                Button(action: onImport) {
                    Text("Import to ApproachNote")
                        .frame(maxWidth: .infinity)
                }
                .buttonStyle(.borderedProminent)
                .controlSize(.large)

                Button(action: onCancel) {
                    Text("Cancel")
                        .frame(maxWidth: .infinity)
                }
                .buttonStyle(.bordered)
                .controlSize(.large)
            }
            .padding(.horizontal)
            .padding(.bottom, 20)
        }
        .frame(width: 400, height: 400)
    }
}

struct MacSongExactMatchView: View {
    let songData: SongData
    let existingSong: ExistingSong
    let onOpenInApp: () -> Void
    let onCancel: () -> Void

    var body: some View {
        VStack(spacing: 16) {
            VStack(spacing: 8) {
                Image(systemName: "checkmark.circle.fill")
                    .font(.system(size: 40))
                    .foregroundColor(.green)

                Text("Song Already Exists")
                    .font(.title2)
                    .bold()

                Text("\(songData.title) is already in the database")
                    .font(.subheadline)
                    .foregroundColor(.secondary)
                    .multilineTextAlignment(.center)
            }
            .padding(.top, 20)
            .padding(.horizontal)

            ScrollView {
                VStack(alignment: .leading, spacing: 12) {
                    Text("Database Record")
                        .font(.headline)
                        .foregroundColor(.secondary)

                    MacInfoRow(label: "Title", value: existingSong.title)

                    if let composer = existingSong.composer {
                        MacInfoRow(label: "Composer", value: composer)
                    }

                    MacInfoRow(label: "MusicBrainz ID", value: existingSong.musicbrainzId ?? "None")
                }
                .padding()
            }
            .background(Color(NSColor.controlBackgroundColor))
            .cornerRadius(8)
            .padding(.horizontal)

            Spacer()

            VStack(spacing: 10) {
                Button(action: onOpenInApp) {
                    Text("Open in ApproachNote")
                        .frame(maxWidth: .infinity)
                }
                .buttonStyle(.borderedProminent)
                .controlSize(.large)

                Button(action: onCancel) {
                    Text("Done")
                        .frame(maxWidth: .infinity)
                }
                .buttonStyle(.bordered)
                .controlSize(.large)
            }
            .padding(.horizontal)
            .padding(.bottom, 20)
        }
        .frame(width: 400, height: 380)
    }
}

struct MacSongTitleMatchNoMbidView: View {
    let songData: SongData
    let existingSong: ExistingSong
    let onAssociate: () -> Void
    let onCancel: () -> Void

    var body: some View {
        VStack(spacing: 16) {
            VStack(spacing: 8) {
                Image(systemName: "link.circle.fill")
                    .font(.system(size: 40))
                    .foregroundColor(.orange)

                Text("Song Found Without ID")
                    .font(.title2)
                    .bold()

                Text("A song named \(songData.title) exists but has no MusicBrainz ID")
                    .font(.subheadline)
                    .foregroundColor(.secondary)
                    .multilineTextAlignment(.center)
            }
            .padding(.top, 20)
            .padding(.horizontal)

            ScrollView {
                VStack(alignment: .leading, spacing: 16) {
                    VStack(alignment: .leading, spacing: 8) {
                        Text("Current Database Record")
                            .font(.headline)
                            .foregroundColor(.secondary)

                        MacInfoRow(label: "Title", value: existingSong.title)

                        if let composer = existingSong.composer {
                            MacInfoRow(label: "Composer", value: composer)
                        } else {
                            MacInfoRow(label: "Composer", value: "Not set")
                        }

                        MacInfoRow(label: "MusicBrainz ID", value: "Not set")
                    }
                    .padding()
                    .background(Color(NSColor.controlBackgroundColor))
                    .cornerRadius(8)

                    HStack {
                        Spacer()
                        Image(systemName: "arrow.down")
                            .foregroundColor(.secondary)
                        Spacer()
                    }

                    VStack(alignment: .leading, spacing: 8) {
                        Text("MusicBrainz Data")
                            .font(.headline)
                            .foregroundColor(.secondary)

                        MacInfoRow(label: "Title", value: songData.title)

                        if let composerString = songData.composerString {
                            MacInfoRow(label: "Composer(s)", value: composerString)
                        }

                        MacInfoRow(label: "MusicBrainz ID", value: songData.musicbrainzId)
                    }
                    .padding()
                    .background(Color.blue.opacity(0.1))
                    .cornerRadius(8)
                }
                .padding(.horizontal)
            }

            Spacer()

            VStack(spacing: 10) {
                Button(action: onAssociate) {
                    Text("Associate MusicBrainz ID")
                        .frame(maxWidth: .infinity)
                }
                .buttonStyle(.borderedProminent)
                .controlSize(.large)

                Button(action: onCancel) {
                    Text("Cancel")
                        .frame(maxWidth: .infinity)
                }
                .buttonStyle(.bordered)
                .controlSize(.large)
            }
            .padding(.horizontal)
            .padding(.bottom, 20)
        }
        .frame(width: 400, height: 480)
    }
}

struct MacSongTitleMatchDifferentMbidView: View {
    let songData: SongData
    let existingSong: ExistingSong
    let onOverwrite: () -> Void
    let onCancel: () -> Void

    var body: some View {
        VStack(spacing: 16) {
            VStack(spacing: 8) {
                Image(systemName: "exclamationmark.triangle.fill")
                    .font(.system(size: 40))
                    .foregroundColor(.red)

                Text("Different Song Found")
                    .font(.title2)
                    .bold()

                Text("A song named \(songData.title) exists with a different MusicBrainz ID")
                    .font(.subheadline)
                    .foregroundColor(.secondary)
                    .multilineTextAlignment(.center)
            }
            .padding(.top, 20)
            .padding(.horizontal)

            ScrollView {
                VStack(alignment: .leading, spacing: 16) {
                    VStack(alignment: .leading, spacing: 8) {
                        Text("Current Database Record")
                            .font(.headline)
                            .foregroundColor(.secondary)

                        MacInfoRow(label: "Title", value: existingSong.title)

                        if let composer = existingSong.composer {
                            MacInfoRow(label: "Composer", value: composer)
                        }

                        MacInfoRow(label: "MusicBrainz ID", value: existingSong.musicbrainzId ?? "None")
                    }
                    .padding()
                    .background(Color(NSColor.controlBackgroundColor))
                    .cornerRadius(8)

                    HStack {
                        Spacer()
                        Text("VS")
                            .font(.headline)
                            .foregroundColor(.red)
                        Spacer()
                    }

                    VStack(alignment: .leading, spacing: 8) {
                        Text("MusicBrainz Data")
                            .font(.headline)
                            .foregroundColor(.secondary)

                        MacInfoRow(label: "Title", value: songData.title)

                        if let composerString = songData.composerString {
                            MacInfoRow(label: "Composer(s)", value: composerString)
                        }

                        MacInfoRow(label: "MusicBrainz ID", value: songData.musicbrainzId)
                    }
                    .padding()
                    .background(Color.red.opacity(0.1))
                    .cornerRadius(8)
                }
                .padding(.horizontal)
            }

            Text("These may be different songs with the same title. Overwriting will replace the existing MusicBrainz ID.")
                .font(.caption)
                .foregroundColor(.secondary)
                .multilineTextAlignment(.center)
                .padding(.horizontal)

            Spacer()

            VStack(spacing: 10) {
                Button(action: onOverwrite) {
                    Text("Overwrite MusicBrainz ID")
                        .frame(maxWidth: .infinity)
                }
                .buttonStyle(.borderedProminent)
                .tint(.red)
                .controlSize(.large)

                Button(action: onCancel) {
                    Text("Cancel")
                        .frame(maxWidth: .infinity)
                }
                .buttonStyle(.bordered)
                .controlSize(.large)
            }
            .padding(.horizontal)
            .padding(.bottom, 20)
        }
        .frame(width: 400, height: 520)
    }
}

// MARK: - YouTube Import Views

struct MacYouTubeTypeSelectionView: View {
    let youtubeData: YouTubeData
    let onSelectType: (YouTubeVideoType) -> Void
    let onCancel: () -> Void

    var body: some View {
        VStack(spacing: 16) {
            // Header
            VStack(spacing: 8) {
                Image(systemName: "play.rectangle.fill")
                    .font(.system(size: 40))
                    .foregroundColor(.red)

                Text("Import YouTube Video")
                    .font(.title2)
                    .bold()

                Text("What type of video is this?")
                    .font(.subheadline)
                    .foregroundColor(.secondary)
            }
            .padding(.top, 20)

            // Video Info Card
            VStack(alignment: .leading, spacing: 8) {
                Text(youtubeData.title)
                    .font(.headline)
                    .lineLimit(2)

                if let channelName = youtubeData.channelName {
                    HStack(spacing: 4) {
                        Image(systemName: "person.circle")
                            .foregroundColor(.secondary)
                            .font(.caption)
                        Text(channelName)
                            .font(.subheadline)
                            .foregroundColor(.secondary)
                    }
                }
            }
            .padding()
            .frame(maxWidth: .infinity, alignment: .leading)
            .background(Color(NSColor.controlBackgroundColor))
            .cornerRadius(8)
            .padding(.horizontal)

            // Type Selection Buttons
            VStack(spacing: 12) {
                Text("Select video type:")
                    .font(.subheadline)
                    .foregroundColor(.secondary)

                // Transcription Option
                Button(action: { onSelectType(.transcription) }) {
                    HStack(spacing: 12) {
                        Image(systemName: YouTubeVideoType.transcription.iconName)
                            .font(.title2)
                            .foregroundColor(.blue)
                            .frame(width: 30)

                        VStack(alignment: .leading, spacing: 2) {
                            Text(YouTubeVideoType.transcription.displayName)
                                .font(.headline)
                                .foregroundColor(.primary)

                            Text(YouTubeVideoType.transcription.description)
                                .font(.caption)
                                .foregroundColor(.secondary)
                                .lineLimit(2)
                        }

                        Spacer()

                        Image(systemName: "chevron.right")
                            .foregroundColor(.secondary)
                    }
                    .padding()
                    .background(Color(NSColor.controlBackgroundColor))
                    .cornerRadius(8)
                }
                .buttonStyle(.plain)

                // Backing Track Option
                Button(action: { onSelectType(.backingTrack) }) {
                    HStack(spacing: 12) {
                        Image(systemName: YouTubeVideoType.backingTrack.iconName)
                            .font(.title2)
                            .foregroundColor(.green)
                            .frame(width: 30)

                        VStack(alignment: .leading, spacing: 2) {
                            Text(YouTubeVideoType.backingTrack.displayName)
                                .font(.headline)
                                .foregroundColor(.primary)

                            Text(YouTubeVideoType.backingTrack.description)
                                .font(.caption)
                                .foregroundColor(.secondary)
                                .lineLimit(2)
                        }

                        Spacer()

                        Image(systemName: "chevron.right")
                            .foregroundColor(.secondary)
                    }
                    .padding()
                    .background(Color(NSColor.controlBackgroundColor))
                    .cornerRadius(8)
                }
                .buttonStyle(.plain)
            }
            .padding(.horizontal)

            Spacer()

            // Cancel Button
            Button(action: onCancel) {
                Text("Cancel")
                    .frame(maxWidth: .infinity)
            }
            .buttonStyle(.bordered)
            .controlSize(.large)
            .padding(.horizontal)
            .padding(.bottom, 20)
        }
        .frame(width: 400, height: 450)
    }
}
