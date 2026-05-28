//
//  OnboardingPages.swift
//  Approach Note
//
//  Shared onboarding page content for iOS and Mac
//

import SwiftUI

// MARK: - Platform Constants

private enum OnboardingLayout {
    #if os(iOS)
    static let horizontalPadding: CGFloat = 32
    #else
    static let horizontalPadding: CGFloat = 48
    #endif
}

// MARK: - Page 1: Welcome

struct OnboardingWelcomePage: View {
    var body: some View {
        let content = VStack(spacing: ApproachNoteTheme.spacingXL) {
            Spacer()
                .frame(height: 60)

            // Icon
            Image(systemName: "music.note.list")
                .font(.system(size: 60))
                .foregroundColor(ApproachNoteTheme.brand)

            Text("Welcome!")
                .font(ApproachNoteTheme.largeTitle())
                .foregroundColor(ApproachNoteTheme.textPrimary)

            VStack(spacing: ApproachNoteTheme.spacingMD) {
                Text("Thanks for checking out ApproachNote.")
                    .font(ApproachNoteTheme.title3())
                    .multilineTextAlignment(.center)

                Text("I'm going to give you a brief description of what is available here so you can get yourself oriented.")
                    .font(ApproachNoteTheme.body())
                    .bodyLineSpacing()
                    .foregroundColor(ApproachNoteTheme.textSecondary)
                    .multilineTextAlignment(.center)

                #if os(iOS)
                Text("You can always re-run this tutorial by going to the About section and tapping \"View Tutorial\".")
                    .font(ApproachNoteTheme.body())
                    .bodyLineSpacing()
                    .foregroundColor(ApproachNoteTheme.textSecondary)
                    .multilineTextAlignment(.center)
                #else
                Text("You can always re-run this tutorial from Settings.")
                    .font(ApproachNoteTheme.body())
                    .bodyLineSpacing()
                    .foregroundColor(ApproachNoteTheme.textSecondary)
                    .multilineTextAlignment(.center)
                #endif
            }
            .foregroundColor(ApproachNoteTheme.textPrimary)
            .padding(.horizontal, OnboardingLayout.horizontalPadding)

            Spacer()
                .frame(height: 40)

            // Decorative element
            VStack(spacing: ApproachNoteTheme.spacingSM) {
                Image(systemName: "info.circle")
                    .font(ApproachNoteTheme.title2())
                    .foregroundColor(ApproachNoteTheme.accent)

                Text("When it comes to music, the data are complicated.\nI'll walk you through the definitions.")
                    .font(ApproachNoteTheme.body(italic: true))
                    .bodyLineSpacing()
                    .multilineTextAlignment(.center)
                    .foregroundColor(ApproachNoteTheme.textSecondary)
            }
            .padding(.horizontal, OnboardingLayout.horizontalPadding)

            Spacer()
        }

        #if os(iOS)
        ScrollView {
            content
        }
        #else
        content.padding()
        #endif
    }
}

// MARK: - Page 2: Songs

struct OnboardingSongPage: View {
    var body: some View {
        let content = VStack(spacing: ApproachNoteTheme.spacingXL) {
            Spacer()
                .frame(height: 60)

            // Icon with label
            VStack(spacing: ApproachNoteTheme.spacingXS) {
                Image(systemName: "music.note")
                    .font(.system(size: 50))
                    .foregroundColor(ApproachNoteTheme.brand)

                Text("Song")
                    .font(ApproachNoteTheme.largeTitle())
                    .foregroundColor(ApproachNoteTheme.textPrimary)
            }

            VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingMD) {
                Text("When Gerald Marks and Seymour Simons sat down in 1932 to write \(Text("All of Me").font(ApproachNoteTheme.body(italic: true))), they were creating a \(Text("Song").fontWeight(.semibold)).")

                Text("This can sometimes be called a Work, or a Composition.")
                    .foregroundColor(ApproachNoteTheme.textSecondary)

                Text("But it's the basic chords, melody, and (if appropriate) lyrics of a particular written piece of music.")
                    .foregroundColor(ApproachNoteTheme.textSecondary)
            }
            .font(ApproachNoteTheme.body())
            .bodyLineSpacing()
            .foregroundColor(ApproachNoteTheme.textPrimary)
            .multilineTextAlignment(.leading)
            .padding(.horizontal, OnboardingLayout.horizontalPadding)

            Spacer()
                .frame(height: 40)

            // Visual representation
            VStack(spacing: ApproachNoteTheme.spacingXS) {
                HStack(spacing: ApproachNoteTheme.spacingSM) {
                    Image(systemName: "pianokeys")
                    Image(systemName: "plus")
                        .font(ApproachNoteTheme.caption())
                    Image(systemName: "waveform")
                    Image(systemName: "plus")
                        .font(ApproachNoteTheme.caption())
                    Image(systemName: "text.alignleft")
                }
                .font(ApproachNoteTheme.title2())
                .foregroundColor(ApproachNoteTheme.textSecondary)

                Text("Chords + Melody + Lyrics")
                    .font(ApproachNoteTheme.caption())
                    .foregroundColor(ApproachNoteTheme.textSecondary)
            }
            .padding()
            .background(
                RoundedRectangle(cornerRadius: 12)
                    .fill(ApproachNoteTheme.surface)
            )
            .padding(.horizontal, OnboardingLayout.horizontalPadding)

            Spacer()
        }

        #if os(iOS)
        ScrollView {
            content
        }
        #else
        content.padding()
        #endif
    }
}

// MARK: - Page 3: Recordings

struct OnboardingRecordingPage: View {
    var body: some View {
        let content = VStack(spacing: ApproachNoteTheme.spacingXL) {
            Spacer()
                .frame(height: 60)

            // Icon with label
            VStack(spacing: ApproachNoteTheme.spacingXS) {
                Image(systemName: "opticaldisc")
                    .font(.system(size: 50))
                    .foregroundColor(ApproachNoteTheme.textSecondary)

                Text("Recording")
                    .font(ApproachNoteTheme.largeTitle())
                    .foregroundColor(ApproachNoteTheme.textPrimary)
            }

            VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingMD) {
                Text("When Count Basie and his orchestra got together in November 1941 to play this song and commit it to media, that generated this \(Text("Recording").fontWeight(.semibold)).")

                Text("The lineup for this recording is what it was on that date & time.")
                    .foregroundColor(ApproachNoteTheme.textSecondary)
            }
            .font(ApproachNoteTheme.body())
            .bodyLineSpacing()
            .foregroundColor(ApproachNoteTheme.textPrimary)
            .multilineTextAlignment(.leading)
            .padding(.horizontal, OnboardingLayout.horizontalPadding)

            Spacer()
                .frame(height: 40)

            // Visual representation
            VStack(spacing: ApproachNoteTheme.spacingSM) {
                HStack {
                    Image(systemName: "person.3.fill")
                        .foregroundColor(ApproachNoteTheme.accent)
                    Text("+")
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                    Image(systemName: "music.note")
                        .foregroundColor(ApproachNoteTheme.brand)
                    Text("+")
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                    Image(systemName: "calendar")
                        .foregroundColor(ApproachNoteTheme.accent)
                }
                .font(ApproachNoteTheme.title2())

                Text("Artists + Song + Date")
                    .font(ApproachNoteTheme.caption())
                    .foregroundColor(ApproachNoteTheme.textSecondary)
            }
            .padding()
            .background(
                RoundedRectangle(cornerRadius: 12)
                    .fill(ApproachNoteTheme.surface)
            )
            .padding(.horizontal, OnboardingLayout.horizontalPadding)

            Spacer()
        }

        #if os(iOS)
        ScrollView {
            content
        }
        #else
        content.padding()
        #endif
    }
}

// MARK: - Page 4: Releases

struct OnboardingReleasesPage: View {
    var body: some View {
        let content = VStack(spacing: ApproachNoteTheme.spacingLG) {
            Spacer()
                .frame(height: 40)

            // Icon with label
            VStack(spacing: ApproachNoteTheme.spacingXS) {
                Image(systemName: "shippingbox")
                    .font(.system(size: 50))
                    .foregroundColor(ApproachNoteTheme.accent)

                Text("Releases")
                    .font(ApproachNoteTheme.largeTitle())
                    .foregroundColor(ApproachNoteTheme.textPrimary)
            }

            VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingSM) {
                Text("The music industry being what it is, here's where it gets complicated.")

                Text("That recording was issued to the public on a \(Text("Release").fontWeight(.semibold)). The release is a piece of commercial product (vinyl, CD, cassette, streaming) that was put into the world by a label.")

                Text("The same piece of audio often appears on multiple releases.")
            }
            .font(ApproachNoteTheme.body())
            .bodyLineSpacing()
            .foregroundColor(ApproachNoteTheme.textSecondary)
            .multilineTextAlignment(.leading)
            .padding(.horizontal, OnboardingLayout.horizontalPadding)

            // Formats visualization
            HStack(spacing: ApproachNoteTheme.spacingMD) {
                ForEach(["opticaldisc", "record.circle", "play.rectangle.fill"], id: \.self) { icon in
                    Image(systemName: icon)
                        .font(ApproachNoteTheme.title())
                        .foregroundColor(ApproachNoteTheme.accent)
                }
            }
            .padding(.vertical, ApproachNoteTheme.spacingXS)

            #if os(iOS)
            VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingSM) {
                Text("If you care about hearing the specific version (or Recording) of that song, it doesn't matter too much what Release it's on — they should sound the same.")

                Text("(Remastering, etc., may be counted as a separate release or may not.)")
                    .font(ApproachNoteTheme.caption())

                Text("Oftentimes, releases are restricted by geographic region; or they may no longer be available at all.")
            }
            .font(ApproachNoteTheme.body())
            .bodyLineSpacing()
            .foregroundColor(ApproachNoteTheme.textSecondary)
            .padding(.horizontal, OnboardingLayout.horizontalPadding)
            #endif

            // Key insight box
            VStack(spacing: ApproachNoteTheme.spacingXS) {
                Image(systemName: "lightbulb.fill")
                    .foregroundColor(ApproachNoteTheme.accent)

                Text("For our purposes, if we can find any Release of the same Recording, we can treat them interchangeably from a playback and lineup perspective.")
                    .font(ApproachNoteTheme.callout(italic: true))
                    .multilineTextAlignment(.center)
                    .foregroundColor(ApproachNoteTheme.textPrimary)
            }
            .padding()
            .background(
                RoundedRectangle(cornerRadius: 12)
                    .fill(ApproachNoteTheme.surface)
            )
            .padding(.horizontal, OnboardingLayout.horizontalPadding)

            Spacer()
        }

        #if os(iOS)
        ScrollView {
            content
        }
        #else
        content.padding()
        #endif
    }
}

// MARK: - Page 5: Completion

struct OnboardingCompletionPage: View {
    let onFinish: () -> Void

    var body: some View {
        VStack(spacing: 32) {
            Spacer()

            Image(systemName: "checkmark.circle.fill")
                .font(.system(size: 80))
                .foregroundColor(ApproachNoteTheme.brand)

            Text("You're All Set!")
                .font(ApproachNoteTheme.largeTitle())
                .foregroundColor(ApproachNoteTheme.textPrimary)

            Text("So, there you have it in a nutshell.")
                .font(ApproachNoteTheme.title3())
                .foregroundColor(ApproachNoteTheme.textPrimary)

            Text("Enjoy!")
                .font(ApproachNoteTheme.title2())
                .foregroundColor(ApproachNoteTheme.brand)

            Spacer()

            Button(action: onFinish) {
                Text("Get Started")
                    .font(ApproachNoteTheme.headline())
                    .foregroundColor(.white)
                    #if os(iOS)
                    .frame(maxWidth: .infinity)
                    #else
                    .frame(width: 200)
                    #endif
                    .padding()
                    .background(ApproachNoteTheme.brand)
                    .cornerRadius(12)
            }
            #if os(macOS)
            .buttonStyle(.plain)
            .padding(.horizontal, OnboardingLayout.horizontalPadding)
            #else
            .padding(.horizontal, OnboardingLayout.horizontalPadding)
            #endif

            Spacer()
                .frame(height: 60)
        }
        #if os(macOS)
        .padding()
        #endif
    }
}

// MARK: - Previews

#Preview("Welcome") {
    OnboardingWelcomePage()
}

#Preview("Song") {
    OnboardingSongPage()
}

#Preview("Recording") {
    OnboardingRecordingPage()
}

#Preview("Releases") {
    OnboardingReleasesPage()
}

#Preview("Completion") {
    OnboardingCompletionPage(onFinish: {})
}
