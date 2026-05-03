//
//  StreamingIcon.swift
//  Approach Note
//
//  Renders the appropriate icon for a streaming service: the official brand
//  mark from Assets.xcassets when one is required for attribution
//  (Spotify, YouTube), otherwise an SF Symbol.
//

import SwiftUI

struct StreamingIcon: View {
    let service: StreamingService
    let size: CGFloat

    var body: some View {
        if let assetName = service.brandAssetName {
            Image(assetName)
                .resizable()
                .aspectRatio(contentMode: .fit)
                .frame(width: size, height: size)
        } else {
            Image(systemName: service.iconName)
                .font(.system(size: size))
                .foregroundColor(service.brandColor)
        }
    }
}
