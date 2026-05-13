//
//  SongBulkEditRecordingsView.swift
//  Approach Note
//
//  Spreadsheet-like view for bulk editing community data (key, tempo, vocal/instrumental)
//  across all recordings for a song.
//

import SwiftUI
import os

// MARK: - Save Status

private enum SaveStatus: Equatable {
    case idle
    case saving
    case saved
    case error
}

// MARK: - Row State

private struct BulkEditRowState {
    var selectedKey: MusicalKey?
    var selectedTempo: TempoMarking?
    var isInstrumental: Bool?
    var saveStatus: SaveStatus = .idle
}

// MARK: - Sort State

private enum SortColumn {
    case artist, release, track, key, tempo, type
}

private enum SortOrder {
    case ascending, descending

    var toggled: SortOrder {
        self == .ascending ? .descending : .ascending
    }
}

// MARK: - Window Delegate

private class BulkEditWindowDelegate: NSObject, NSWindowDelegate {
    let onClose: () -> Void

    init(onClose: @escaping () -> Void) {
        self.onClose = onClose
    }

    func windowWillClose(_ notification: Notification) {
        onClose()
    }
}

// MARK: - Bulk Edit View

struct SongBulkEditRecordingsView: View {
    let songTitle: String
    let recordings: [Recording]

    @Environment(\.openURL) private var openURL
    @EnvironmentObject var authManager: AuthenticationManager
    @AppStorage("preferredStreamingService") private var preferredStreamingService: String = "spotify"

    @State private var rowStates: [String: BulkEditRowState] = [:]
    @State private var hostWindow: NSWindow?
    @State private var sortColumn: SortColumn?
    @State private var sortOrder: SortOrder = .ascending

    // MARK: - Window Management

    private static var windowController: NSWindowController?
    private static var windowDelegate: BulkEditWindowDelegate?
    static var hasUnsavedChanges = false

    static func openInWindow(songTitle: String, recordings: [Recording], authManager: AuthenticationManager, onDismiss: @escaping () -> Void = {}) {
        // Close existing window if open
        windowController?.window?.close()
        windowController = nil

        hasUnsavedChanges = false

        let view = SongBulkEditRecordingsView(
            songTitle: songTitle,
            recordings: recordings
        )
        .environmentObject(authManager)

        let controller = NSHostingController(rootView: view)
        let window = NSWindow(contentViewController: controller)
        window.title = "Bulk Edit - \(songTitle)"
        window.styleMask = [.titled, .closable, .resizable, .miniaturizable]
        window.setContentSize(NSSize(width: 1100, height: 700))
        window.minSize = NSSize(width: 600, height: 300)
        window.center()

        let delegate = BulkEditWindowDelegate {
            if hasUnsavedChanges {
                onDismiss()
            }
            windowController = nil
            windowDelegate = nil
        }
        window.delegate = delegate
        windowDelegate = delegate

        window.makeKeyAndOrderFront(nil)
        windowController = NSWindowController(window: window)
    }

    var body: some View {
        VStack(spacing: 0) {
            // Header (always full width, not horizontally scrollable)
            header

            Divider()

            // Table area: horizontal scroll wraps headers + rows together
            // so columns stay aligned. Vertical scroll is only for rows.
            ScrollView(.horizontal, showsIndicators: true) {
                VStack(spacing: 0) {
                    columnHeaders

                    Divider()

                    ScrollView(.vertical) {
                        LazyVStack(spacing: 0) {
                            ForEach(sortedRecordings) { recording in
                                recordingRow(recording)
                                Divider()
                            }
                        }
                    }
                }
                .frame(minWidth: 880)
            }
        }
        .frame(minWidth: 600, idealWidth: 1100, minHeight: 300, idealHeight: 700)
        .background(Color(NSColor.windowBackgroundColor))
        .background(WindowAccessor(window: $hostWindow))
        .onAppear {
            populateRowStates()
        }
    }

    // MARK: - Header

    private var header: some View {
        HStack {
            VStack(alignment: .leading, spacing: 4) {
                Text("Bulk Edit Recordings")
                    .font(ApproachNoteTheme.title2(weight: .semibold))
                    .foregroundColor(.primary)
                HStack(spacing: 8) {
                    Text(songTitle)
                        .font(ApproachNoteTheme.subheadline())
                        .foregroundColor(.secondary)
                    Text("\(recordings.count) recordings")
                        .font(ApproachNoteTheme.caption())
                        .foregroundColor(.secondary)
                        .padding(.horizontal, 8)
                        .padding(.vertical, 2)
                        .background(Color.accentColor.opacity(0.1))
                        .cornerRadius(4)
                }
            }

            Spacer()

            Button("Done") {
                hostWindow?.close()
            }
            .keyboardShortcut(.cancelAction)
        }
        .padding()
    }

    // MARK: - Column Headers

    private var columnHeaders: some View {
        HStack(spacing: 8) {
            sortableHeader("Artist", column: .artist)
                .frame(maxWidth: .infinity, alignment: .leading)
            sortableHeader("Release", column: .release)
                .frame(maxWidth: .infinity, alignment: .leading)
            sortableHeader("Track", column: .track)
                .frame(maxWidth: .infinity, alignment: .leading)
            sortableHeader("Key", column: .key)
                .frame(width: 120, alignment: .leading)
            sortableHeader("Tempo", column: .tempo)
                .frame(width: 140, alignment: .leading)
            sortableHeader("Type", column: .type)
                .frame(width: 140, alignment: .leading)
            Spacer()
                .frame(width: 30)
        }
        .font(ApproachNoteTheme.caption(weight: .semibold))
        .padding(.horizontal)
        .padding(.vertical, 8)
        .background(Color(NSColor.controlBackgroundColor))
    }

    @ViewBuilder
    private func sortableHeader(_ title: String, column: SortColumn) -> some View {
        Button {
            if sortColumn == column {
                sortOrder = sortOrder.toggled
            } else {
                sortColumn = column
                sortOrder = .ascending
            }
        } label: {
            HStack(spacing: 2) {
                Text(title)
                if sortColumn == column {
                    Image(systemName: sortOrder == .ascending ? "chevron.up" : "chevron.down")
                        .font(.system(size: 8, weight: .bold))
                }
            }
            .foregroundColor(sortColumn == column ? .primary : .secondary)
        }
        .buttonStyle(.plain)
    }

    // MARK: - Recording Row

    @ViewBuilder
    private func recordingRow(_ recording: Recording) -> some View {
        let state = rowStates[recording.id] ?? BulkEditRowState()

        HStack(spacing: 8) {
            // Artist
            Text(recording.artistCredit ?? "Unknown Artist")
                .font(ApproachNoteTheme.caption())
                .foregroundColor(.primary)
                .lineLimit(1)
                .frame(maxWidth: .infinity, alignment: .leading)

            // Release
            Text(recording.albumTitle ?? "Unknown Album")
                .font(ApproachNoteTheme.caption())
                .foregroundColor(.primary)
                .lineLimit(1)
                .frame(maxWidth: .infinity, alignment: .leading)

            // Track (linked if playback URL available)
            trackColumn(recording)
                .frame(maxWidth: .infinity, alignment: .leading)

            // Key picker
            Picker("", selection: keyBinding(for: recording.id)) {
                Text("Not set").tag(nil as MusicalKey?)
                ForEach(MusicalKey.allCases) { key in
                    Text(key.shortName).tag(key as MusicalKey?)
                }
            }
            .labelsHidden()
            .frame(width: 120)

            // Tempo picker
            Picker("", selection: tempoBinding(for: recording.id)) {
                Text("Not set").tag(nil as TempoMarking?)
                ForEach(TempoMarking.allCases) { tempo in
                    Text(tempo.displayName).tag(tempo as TempoMarking?)
                }
            }
            .labelsHidden()
            .frame(width: 140)

            // Type picker (segmented)
            Picker("", selection: instrumentalBinding(for: recording.id)) {
                Text("—").tag(nil as Bool?)
                Text("Inst.").tag(true as Bool?)
                Text("Vocal").tag(false as Bool?)
            }
            .pickerStyle(.segmented)
            .labelsHidden()
            .frame(width: 140)

            // Status indicator
            statusIndicator(for: state.saveStatus)
                .frame(width: 30)
        }
        .padding(.horizontal)
        .padding(.vertical, 6)
    }

    // MARK: - Track Column

    @ViewBuilder
    private func trackColumn(_ recording: Recording) -> some View {
        let title = recording.title ?? recording.songTitle ?? "Unknown"
        if let playback = recording.playbackUrl(preferring: preferredStreamingService),
           let url = URL(string: playback.url) {
            Button {
                openURL(url)
            } label: {
                HStack(spacing: 4) {
                    Image(systemName: "play.circle.fill")
                        .font(.system(size: 12))
                    Text(title)
                        .underline()
                        .lineLimit(1)
                    Image(systemName: "arrow.up.right")
                        .font(.system(size: 8))
                }
                .font(ApproachNoteTheme.caption())
                .foregroundColor(Color(NSColor.linkColor))
            }
            .buttonStyle(.plain)
            .help("Open in \(playback.service)")
        } else {
            Text(title)
                .font(ApproachNoteTheme.caption())
                .foregroundColor(.secondary)
                .lineLimit(1)
        }
    }

    // MARK: - Status Indicator

    @ViewBuilder
    private func statusIndicator(for status: SaveStatus) -> some View {
        switch status {
        case .idle:
            Color.clear.frame(width: 16, height: 16)
        case .saving:
            ProgressView()
                .controlSize(.small)
        case .saved:
            Image(systemName: "checkmark.circle.fill")
                .foregroundColor(.green)
                .font(.system(size: 14))
        case .error:
            Image(systemName: "exclamationmark.circle.fill")
                .foregroundColor(.red)
                .font(.system(size: 14))
        }
    }

    // MARK: - State Population

    private func populateRowStates() {
        for recording in recordings {
            var state = BulkEditRowState()
            if let consensus = recording.communityData?.consensus {
                if let keyStr = consensus.performanceKey {
                    state.selectedKey = MusicalKey(rawValue: keyStr)
                }
                if let tempoStr = consensus.tempoMarking {
                    state.selectedTempo = TempoMarking(rawValue: tempoStr)
                }
                state.isInstrumental = consensus.isInstrumental
            }
            rowStates[recording.id] = state
        }
    }

    // MARK: - Sorting

    private var sortedRecordings: [Recording] {
        guard let column = sortColumn else { return recordings }

        let sorted = recordings.sorted { a, b in
            switch column {
            case .artist:
                let aVal = a.artistCredit ?? ""
                let bVal = b.artistCredit ?? ""
                return aVal.localizedCaseInsensitiveCompare(bVal) == .orderedAscending

            case .release:
                let aVal = a.albumTitle ?? ""
                let bVal = b.albumTitle ?? ""
                return aVal.localizedCaseInsensitiveCompare(bVal) == .orderedAscending

            case .track:
                let aHasLink = a.playbackUrl(preferring: preferredStreamingService) != nil
                let bHasLink = b.playbackUrl(preferring: preferredStreamingService) != nil
                if aHasLink != bHasLink { return aHasLink }
                let aTitle = a.title ?? a.songTitle ?? ""
                let bTitle = b.title ?? b.songTitle ?? ""
                return aTitle.localizedCaseInsensitiveCompare(bTitle) == .orderedAscending

            case .key:
                let aKey = rowStates[a.id]?.selectedKey
                let bKey = rowStates[b.id]?.selectedKey
                if (aKey == nil) != (bKey == nil) { return aKey == nil }
                guard let aK = aKey, let bK = bKey else { return false }
                return aK.rawValue.localizedCaseInsensitiveCompare(bK.rawValue) == .orderedAscending

            case .tempo:
                let aTempo = rowStates[a.id]?.selectedTempo
                let bTempo = rowStates[b.id]?.selectedTempo
                if (aTempo == nil) != (bTempo == nil) { return aTempo == nil }
                guard let aT = aTempo, let bT = bTempo else { return false }
                let aIdx = TempoMarking.allCases.firstIndex(of: aT) ?? 0
                let bIdx = TempoMarking.allCases.firstIndex(of: bT) ?? 0
                return aIdx < bIdx

            case .type:
                let aType = rowStates[a.id]?.isInstrumental
                let bType = rowStates[b.id]?.isInstrumental
                let aRank = typeRank(aType)
                let bRank = typeRank(bType)
                return aRank < bRank
            }
        }

        return sortOrder == .ascending ? sorted : sorted.reversed()
    }

    /// Rank for type sorting: nil=0 (not set first), instrumental=1, vocal=2
    private func typeRank(_ value: Bool?) -> Int {
        guard let v = value else { return 0 }
        return v ? 1 : 2
    }

    // MARK: - Bindings with Auto-Save

    private func keyBinding(for recordingId: String) -> Binding<MusicalKey?> {
        Binding(
            get: { rowStates[recordingId]?.selectedKey },
            set: { newValue in
                rowStates[recordingId]?.selectedKey = newValue
                saveContribution(for: recordingId)
            }
        )
    }

    private func tempoBinding(for recordingId: String) -> Binding<TempoMarking?> {
        Binding(
            get: { rowStates[recordingId]?.selectedTempo },
            set: { newValue in
                rowStates[recordingId]?.selectedTempo = newValue
                saveContribution(for: recordingId)
            }
        )
    }

    private func instrumentalBinding(for recordingId: String) -> Binding<Bool?> {
        Binding(
            get: { rowStates[recordingId]?.isInstrumental },
            set: { newValue in
                rowStates[recordingId]?.isInstrumental = newValue
                saveContribution(for: recordingId)
            }
        )
    }

    // MARK: - Save

    private func saveContribution(for recordingId: String) {
        guard let state = rowStates[recordingId] else { return }

        // Must have at least one value to save
        guard state.selectedKey != nil || state.selectedTempo != nil || state.isInstrumental != nil else { return }

        rowStates[recordingId]?.saveStatus = .saving

        Task {
            do {
                var body: [String: Any] = [:]
                if let key = state.selectedKey?.rawValue { body["performance_key"] = key }
                if let tempo = state.selectedTempo?.rawValue { body["tempo_marking"] = tempo }
                if let instrumental = state.isInstrumental { body["is_instrumental"] = instrumental }

                let bodyData = try JSONSerialization.data(withJSONObject: body)

                let url = URL.api(path: "/recordings/\(recordingId)/contribution")

                _ = try await authManager.makeAuthenticatedRequest(
                    url: url,
                    method: "PUT",
                    body: bodyData,
                    contentType: "application/json"
                )

                await MainActor.run {
                    rowStates[recordingId]?.saveStatus = .saved
                    SongBulkEditRecordingsView.hasUnsavedChanges = true
                }

                // Clear saved status after 2 seconds
                try? await Task.sleep(nanoseconds: 2_000_000_000)
                await MainActor.run {
                    if rowStates[recordingId]?.saveStatus == .saved {
                        rowStates[recordingId]?.saveStatus = .idle
                    }
                }
            } catch {
                Log.ui.error("Bulk edit save failed for \(recordingId, privacy: .private): \(error.localizedDescription)")
                await MainActor.run {
                    rowStates[recordingId]?.saveStatus = .error
                }

                // Clear error status after 3 seconds
                try? await Task.sleep(nanoseconds: 3_000_000_000)
                await MainActor.run {
                    if rowStates[recordingId]?.saveStatus == .error {
                        rowStates[recordingId]?.saveStatus = .idle
                    }
                }
            }
        }
    }
}

// MARK: - Window Accessor

/// NSViewRepresentable that captures a reference to the hosting NSWindow.
private struct WindowAccessor: NSViewRepresentable {
    @Binding var window: NSWindow?

    func makeNSView(context: Context) -> NSView {
        let view = NSView()
        DispatchQueue.main.async {
            self.window = view.window
        }
        return view
    }

    func updateNSView(_ nsView: NSView, context: Context) {
        DispatchQueue.main.async {
            self.window = nsView.window
        }
    }
}

// MARK: - Preview

#Preview {
    SongBulkEditRecordingsView(
        songTitle: "Autumn Leaves",
        recordings: [Recording.preview1, Recording.preview2]
    )
    .environmentObject(AuthenticationManager())
}
