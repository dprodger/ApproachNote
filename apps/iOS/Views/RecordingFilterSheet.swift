//
//  RecordingFilterSheet.swift
//  Approach Note
//
//  Bottom sheet for filtering recordings by availability and instrument
//

import SwiftUI

struct RecordingFilterSheet: View {
    @Binding var selectedServices: Set<StreamingService>
    @Binding var selectedInstrument: InstrumentFamily?
    let availableInstruments: [InstrumentFamily]

    @Environment(\.dismiss) private var dismiss

    var body: some View {
        NavigationStack {
            ScrollView {
                VStack(alignment: .leading, spacing: 28) {

                    // MARK: - Playback availability (multi-select)
                    VStack(alignment: .leading, spacing: 8) {
                        Text("Playback availability")
                            .font(ApproachNoteTheme.headline())
                            .foregroundColor(ApproachNoteTheme.textPrimary)

                        Text("Select which service(s) you'd like to include for playback")
                            .font(ApproachNoteTheme.subheadline())
                            .foregroundColor(ApproachNoteTheme.textSecondary)

                        VStack(alignment: .leading, spacing: 4) {
                            ForEach(StreamingService.allCases) { service in
                                serviceCheckboxRow(service)
                            }
                        }
                        .padding(.top, 4)
                    }

                    // MARK: - Instrument Section
                    if !availableInstruments.isEmpty {
                        VStack(alignment: .leading, spacing: 8) {
                            Text("By Instrument")
                                .font(ApproachNoteTheme.headline())
                                .foregroundColor(ApproachNoteTheme.textPrimary)

                            Text("Select to filter for recordings that feature a specific instrument")
                                .font(ApproachNoteTheme.subheadline())
                                .foregroundColor(ApproachNoteTheme.textSecondary)

                            LazyVGrid(columns: [
                                GridItem(.flexible()),
                                GridItem(.flexible()),
                                GridItem(.flexible())
                            ], spacing: 10) {
                                ForEach(availableInstruments, id: \.self) { family in
                                    instrumentButton(family)
                                }
                            }
                            .padding(.top, 4)
                        }
                    }

                    Spacer(minLength: 40)
                }
                .padding()
            }
            .background(ApproachNoteTheme.background)
            .navigationTitle("Filter Recordings")
            .navigationBarTitleDisplayMode(.inline)
            // Style the nav bar from the live palette (the global
            // UINavigationBar appearance is set once at launch and goes stale
            // when the palette changes), matching jazzNavigationBar.
            .toolbarBackground(ApproachNoteTheme.brand, for: .navigationBar)
            .toolbarBackground(.visible, for: .navigationBar)
            .toolbarColorScheme(.dark, for: .navigationBar)
            .toolbar {
                ToolbarItem(placement: .navigationBarLeading) {
                    if hasActiveFilters {
                        Button("Clear All") {
                            clearAllFilters()
                        }
                        .foregroundColor(ApproachNoteTheme.textOnDark)
                    }
                }

                ToolbarItem(placement: .navigationBarTrailing) {
                    Button("Done") {
                        dismiss()
                    }
                    .fontWeight(.semibold)
                    .foregroundColor(ApproachNoteTheme.textOnDark)
                }
            }
        }
        .presentationDetents([.medium, .large])
        .presentationDragIndicator(.visible)
    }

    // MARK: - Helper Views

    @ViewBuilder
    private func serviceCheckboxRow(_ service: StreamingService) -> some View {
        let isSelected = selectedServices.contains(service)

        Button(action: {
            if isSelected {
                selectedServices.remove(service)
            } else {
                selectedServices.insert(service)
            }
        }) {
            HStack(spacing: 12) {
                Image(systemName: isSelected ? "checkmark.circle.fill" : "circle")
                    .font(ApproachNoteTheme.title3())
                    .foregroundColor(isSelected ? ApproachNoteTheme.brand : ApproachNoteTheme.textSecondary.opacity(0.5))

                Text(service.displayName)
                    .font(ApproachNoteTheme.body())
                    .bodyLineSpacing()
                    .foregroundColor(ApproachNoteTheme.textPrimary)

                Spacer()
            }
            .padding(.vertical, 8)
            .contentShape(Rectangle())
        }
        .buttonStyle(.plain)
    }

    @ViewBuilder
    private func instrumentButton(_ family: InstrumentFamily) -> some View {
        let isSelected = selectedInstrument == family

        Button(action: {
            if selectedInstrument == family {
                selectedInstrument = nil
            } else {
                selectedInstrument = family
            }
        }) {
            Text(family.rawValue)
                .font(ApproachNoteTheme.subheadline())
                .lineLimit(1)
                .minimumScaleFactor(0.8)
                .frame(maxWidth: .infinity)
                .padding(.vertical, 10)
                .padding(.horizontal, 8)
                .background(isSelected ? ApproachNoteTheme.brand : ApproachNoteTheme.surface)
                .foregroundColor(isSelected ? ApproachNoteTheme.textOnAccent : ApproachNoteTheme.textPrimary)
                .cornerRadius(8)
                .overlay(
                    RoundedRectangle(cornerRadius: 8)
                        .stroke(isSelected ? Color.clear : ApproachNoteTheme.textSecondary.opacity(0.5), lineWidth: 1)
                )
        }
        .buttonStyle(.plain)
    }

    // MARK: - Helpers

    private var hasActiveFilters: Bool {
        !selectedServices.isEmpty || selectedInstrument != nil
    }

    private func clearAllFilters() {
        selectedServices.removeAll()
        selectedInstrument = nil
    }
}

// MARK: - Preview

#Preview {
    RecordingFilterSheet(
        selectedServices: .constant([.spotify]),
        selectedInstrument: .constant(nil),
        availableInstruments: [.guitar, .saxophone, .trumpet, .piano, .bass, .drums]
    )
}
