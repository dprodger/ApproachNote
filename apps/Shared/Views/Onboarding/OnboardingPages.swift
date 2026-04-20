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
        let content = VStack(spacing: 24) {
            Spacer()
                .frame(height: 60)

            // Icon
            Image(systemName: "music.note.list")
                .font(.system(size: 60))
                .foregroundColor(ApproachNoteTheme.burgundy)

            Text("Welcome!")
                .font(ApproachNoteTheme.largeTitle())
                .foregroundColor(ApproachNoteTheme.charcoal)

            VStack(spacing: 16) {
                Text("Thanks for checking out ApproachNote.")
                    .font(ApproachNoteTheme.title3())
                    .multilineTextAlignment(.center)

                Text("I'm going to give you a brief description of what is available here so you can get yourself oriented.")
                    .font(ApproachNoteTheme.body())
                    .foregroundColor(ApproachNoteTheme.smokeGray)
                    .multilineTextAlignment(.center)

                #if os(iOS)
                Text("You can always re-run this tutorial by going to the About section and tapping \"View Tutorial\".")
                    .font(ApproachNoteTheme.body())
                    .foregroundColor(ApproachNoteTheme.smokeGray)
                    .multilineTextAlignment(.center)
                #else
                Text("You can always re-run this tutorial from Settings.")
                    .font(ApproachNoteTheme.body())
                    .foregroundColor(ApproachNoteTheme.smokeGray)
                    .multilineTextAlignment(.center)
                #endif
            }
            .foregroundColor(ApproachNoteTheme.charcoal)
            .padding(.horizontal, OnboardingLayout.horizontalPadding)

            Spacer()
                .frame(height: 40)

            // Decorative element
            VStack(spacing: 12) {
                Image(systemName: "info.circle")
                    .font(ApproachNoteTheme.title2())
                    .foregroundColor(ApproachNoteTheme.amber)

                Text("When it comes to music, the data are complicated.\nI'll walk you through the definitions.")
                    .font(ApproachNoteTheme.body())
                    .italic()
                    .multilineTextAlignment(.center)
                    .foregroundColor(ApproachNoteTheme.smokeGray)
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
        let content = VStack(spacing: 24) {
            Spacer()
                .frame(height: 60)

            // Icon with label
            VStack(spacing: 8) {
                Image(systemName: "music.note")
                    .font(.system(size: 50))
                    .foregroundColor(ApproachNoteTheme.burgundy)

                Text("Song")
                    .font(ApproachNoteTheme.largeTitle())
                    .foregroundColor(ApproachNoteTheme.charcoal)
            }

            VStack(alignment: .leading, spacing: 16) {
                Text("When Gerald Marks and Seymour Simons sat down in 1932 to write ")
                + Text("All of Me")
                    .italic()
                + Text(", they were creating a ")
                + Text("Song")
                    .fontWeight(.semibold)
                + Text(".")

                Text("This can sometimes be called a Work, or a Composition.")
                    .foregroundColor(ApproachNoteTheme.smokeGray)

                Text("But it's the basic chords, melody, and (if appropriate) lyrics of a particular written piece of music.")
                    .foregroundColor(ApproachNoteTheme.smokeGray)
            }
            .font(ApproachNoteTheme.body())
            .foregroundColor(ApproachNoteTheme.charcoal)
            .multilineTextAlignment(.leading)
            .padding(.horizontal, OnboardingLayout.horizontalPadding)

            Spacer()
                .frame(height: 40)

            // Visual representation
            VStack(spacing: 8) {
                HStack(spacing: 12) {
                    Image(systemName: "pianokeys")
                    Image(systemName: "plus")
                        .font(ApproachNoteTheme.caption())
                    Image(systemName: "waveform")
                    Image(systemName: "plus")
                        .font(ApproachNoteTheme.caption())
                    Image(systemName: "text.alignleft")
                }
                .font(ApproachNoteTheme.title2())
                .foregroundColor(ApproachNoteTheme.brass)

                Text("Chords + Melody + Lyrics")
                    .font(ApproachNoteTheme.caption())
                    .foregroundColor(ApproachNoteTheme.smokeGray)
            }
            .padding()
            .background(
                RoundedRectangle(cornerRadius: 12)
                    .fill(ApproachNoteTheme.cardBackground)
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
        let content = VStack(spacing: 24) {
            Spacer()
                .frame(height: 60)

            // Icon with label
            VStack(spacing: 8) {
                Image(systemName: "opticaldisc")
                    .font(.system(size: 50))
                    .foregroundColor(ApproachNoteTheme.brass)

                Text("Recording")
                    .font(ApproachNoteTheme.largeTitle())
                    .foregroundColor(ApproachNoteTheme.charcoal)
            }

            VStack(alignment: .leading, spacing: 16) {
                Text("When Count Basie and his orchestra got together in November 1941 to play this song and commit it to media, that generated this ")
                + Text("Recording")
                    .fontWeight(.semibold)
                + Text(".")

                Text("The lineup for this recording is what it was on that date & time.")
                    .foregroundColor(ApproachNoteTheme.smokeGray)
            }
            .font(ApproachNoteTheme.body())
            .foregroundColor(ApproachNoteTheme.charcoal)
            .multilineTextAlignment(.leading)
            .padding(.horizontal, OnboardingLayout.horizontalPadding)

            Spacer()
                .frame(height: 40)

            // Visual representation
            VStack(spacing: 12) {
                HStack {
                    Image(systemName: "person.3.fill")
                        .foregroundColor(ApproachNoteTheme.amber)
                    Text("+")
                        .foregroundColor(ApproachNoteTheme.smokeGray)
                    Image(systemName: "music.note")
                        .foregroundColor(ApproachNoteTheme.burgundy)
                    Text("+")
                        .foregroundColor(ApproachNoteTheme.smokeGray)
                    Image(systemName: "calendar")
                        .foregroundColor(ApproachNoteTheme.teal)
                }
                .font(ApproachNoteTheme.title2())

                Text("Artists + Song + Date")
                    .font(ApproachNoteTheme.caption())
                    .foregroundColor(ApproachNoteTheme.smokeGray)
            }
            .padding()
            .background(
                RoundedRectangle(cornerRadius: 12)
                    .fill(ApproachNoteTheme.cardBackground)
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
        let content = VStack(spacing: 20) {
            Spacer()
                .frame(height: 40)

            // Icon with label
            VStack(spacing: 8) {
                Image(systemName: "shippingbox")
                    .font(.system(size: 50))
                    .foregroundColor(ApproachNoteTheme.teal)

                Text("Releases")
                    .font(ApproachNoteTheme.largeTitle())
                    .foregroundColor(ApproachNoteTheme.charcoal)
            }

            VStack(alignment: .leading, spacing: 12) {
                Text("The music industry being what it is, here's where it gets complicated.")

                Text("That recording was issued to the public on a ")
                + Text("Release")
                    .fontWeight(.semibold)
                + Text(". The release is a piece of commercial product (vinyl, CD, cassette, streaming) that was put into the world by a label.")

                Text("The same piece of audio often appears on multiple releases.")
            }
            .font(ApproachNoteTheme.body())
            .foregroundColor(ApproachNoteTheme.smokeGray)
            .multilineTextAlignment(.leading)
            .padding(.horizontal, OnboardingLayout.horizontalPadding)

            // Formats visualization
            HStack(spacing: 16) {
                ForEach(["opticaldisc", "record.circle", "play.rectangle.fill"], id: \.self) { icon in
                    Image(systemName: icon)
                        .font(ApproachNoteTheme.title())
                        .foregroundColor(ApproachNoteTheme.teal)
                }
            }
            .padding(.vertical, 8)

            #if os(iOS)
            VStack(alignment: .leading, spacing: 12) {
                Text("If you care about hearing the specific version (or Recording) of that song, it doesn't matter too much what Release it's on — they should sound the same.")

                Text("(Remastering, etc., may be counted as a separate release or may not.)")
                    .font(ApproachNoteTheme.caption())

                Text("Oftentimes, releases are restricted by geographic region; or they may no longer be available at all.")
            }
            .font(ApproachNoteTheme.body())
            .foregroundColor(ApproachNoteTheme.smokeGray)
            .padding(.horizontal, OnboardingLayout.horizontalPadding)
            #endif

            // Key insight box
            VStack(spacing: 8) {
                Image(systemName: "lightbulb.fill")
                    .foregroundColor(ApproachNoteTheme.gold)

                Text("For our purposes, if we can find any Release of the same Recording, we can treat them interchangeably from a playback and lineup perspective.")
                    .font(ApproachNoteTheme.callout())
                    .italic()
                    .multilineTextAlignment(.center)
                    .foregroundColor(ApproachNoteTheme.charcoal)
            }
            .padding()
            .background(
                RoundedRectangle(cornerRadius: 12)
                    .fill(ApproachNoteTheme.cardBackground)
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
                .foregroundColor(ApproachNoteTheme.burgundy)

            Text("You're All Set!")
                .font(ApproachNoteTheme.largeTitle())
                .foregroundColor(ApproachNoteTheme.charcoal)

            Text("So, there you have it in a nutshell.")
                .font(ApproachNoteTheme.title3())
                .foregroundColor(ApproachNoteTheme.charcoal)

            Text("Enjoy!")
                .font(ApproachNoteTheme.title2())
                .foregroundColor(ApproachNoteTheme.burgundy)

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
                    .background(ApproachNoteTheme.burgundy)
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
