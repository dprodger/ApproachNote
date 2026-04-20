//
//  CommonViews.swift
//  Approach Note
//
//  Created by Dave Rodger on 11/1/25.
//

import SwiftUI

// MARK: - SwiftUI Views

struct LoadingView: View {
    var body: some View {
        VStack(spacing: 20) {
            ProgressView()
                .scaleEffect(1.5)
            
            Text("Checking database...")
                .font(.subheadline)
                .foregroundColor(.secondary)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(Color.white)
    }
}


struct LoginRequiredView: View {
    let onDismiss: () -> Void

    var body: some View {
        VStack(spacing: 20) {
            Image(systemName: "person.crop.circle.badge.exclamationmark")
                .font(.system(size: 60))
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
                    .font(.headline)
                    .foregroundColor(.white)
                    .frame(maxWidth: .infinity)
                    .padding()
                    .background(Color.blue)
                    .cornerRadius(12)
            }
            .padding(.horizontal)
            .padding(.bottom, 20)
        }
        .padding(.top, 60)
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(Color.white)
    }
}

struct ErrorView: View {
    let message: String
    let onDismiss: () -> Void
    
    var body: some View {
        VStack(spacing: 20) {
            Image(systemName: "xmark.circle.fill")
                .font(.system(size: 60))
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
                    .font(.headline)
                    .foregroundColor(.white)
                    .frame(maxWidth: .infinity)
                    .padding()
                    .background(Color.red)
                    .cornerRadius(12)
            }
            .padding(.horizontal)
            .padding(.bottom, 20)
        }
        .padding(.top, 60)
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(Color.white)
    }
}

struct SuccessView: View {
    var body: some View {
        VStack(spacing: 20) {
            Image(systemName: "checkmark.circle.fill")
                .font(.system(size: 80))
                .foregroundColor(.green)
            
            Text("Artist Saved!")
                .font(.title2)
                .bold()
            
            Text("You can now close this window")
                .font(.subheadline)
                .foregroundColor(.secondary)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(Color.white)
    }
}

struct NotImplementedView: View {
    let feature: String
    let onDismiss: () -> Void
    
    var body: some View {
        VStack(spacing: 20) {
            Image(systemName: "wrench.and.screwdriver")
                .font(.system(size: 60))
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
                    .font(.headline)
                    .foregroundColor(.white)
                    .frame(maxWidth: .infinity)
                    .padding()
                    .background(Color.blue)
                    .cornerRadius(12)
            }
            .padding(.horizontal)
            .padding(.bottom, 20)
        }
        .padding(.top, 60)
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(Color.white)
    }
}


// MARK: - Helper Views

struct InfoRow: View {
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
