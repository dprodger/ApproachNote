//
//  GenericCoverArt.swift
//  Approach Note
//
//  Original, procedurally generated album cover art used in place of real
//  (copyrighted) covers when `ScreenshotMode` is on. The output is fully
//  determined by a seed string (the real artwork URL), so:
//    • the same recording always renders the same generated cover, and
//    • a grid of recordings shows a varied, natural-looking spread of covers.
//
//  The art is abstract (mid-century / jazz-club palettes, geometric shapes,
//  optional vinyl rings) and entirely our own — it infringes nothing, which
//  is the whole point under App Store Guideline 5.2.1.
//

import SwiftUI

#if canImport(UIKit)
import UIKit
typealias ANCoverImage = UIImage
#elseif canImport(AppKit)
import AppKit
typealias ANCoverImage = NSImage
#endif

// MARK: - Generator

enum GenericCoverArt {

    /// Curated palettes as `[backgroundTop, backgroundBottom, accent1, accent2]`
    /// 24-bit RGB. Deliberately moody and saturated so the covers read as
    /// intentional artwork rather than placeholders.
    private static let palettes: [[UInt32]] = [
        [0x10203A, 0x241B4A, 0xE8A13A, 0x4FB0A5], // midnight → indigo / amber / teal
        [0x0E2A2B, 0x123A2E, 0xE5654B, 0xEAD7A0], // teal → forest / coral / cream
        [0x2A1430, 0x451B2E, 0xF2B65A, 0xE07A93], // plum → maroon / gold / rose
        [0x1C1C22, 0x2A2E38, 0xF26B3A, 0x6FA8C7], // charcoal → slate / orange / sky
        [0x3A1418, 0x4A2A1E, 0xD9A441, 0x9FB07A], // burgundy → brown / mustard / sage
        [0x102234, 0x143A44, 0xE98A6B, 0x84C5B4], // navy → petrol / salmon / mint
        [0x241A12, 0x3A2A1C, 0xE9D5A8, 0xC45A2E], // espresso → umber / cream / rust
        [0x1E1645, 0x202A66, 0xC6D65A, 0xD361A0], // violet → blue / lime / magenta
        [0x16271A, 0x223421, 0xE3704F, 0xE9DCB5], // forest → moss / coral / cream
        [0x222838, 0x33384A, 0xE0A24A, 0xCB7E86], // slate → grey / amber / dusty rose
    ]

    private static let cache = NSCache<NSString, ANCoverImage>()

    /// A square generated cover for `seed`, cached by seed + size.
    static func image(seed: String, size: CGFloat = 600) -> ANCoverImage {
        let key = "\(seed)@\(Int(size))" as NSString
        if let cached = cache.object(forKey: key) { return cached }
        let img = render(seed: seed, size: size)
        cache.setObject(img, forKey: key)
        return img
    }

    // MARK: Platform rendering

    private static func render(seed: String, size: CGFloat) -> ANCoverImage {
        let pixels = CGSize(width: size, height: size)
        #if canImport(UIKit)
        let renderer = UIGraphicsImageRenderer(size: pixels)
        return renderer.image { ctx in
            draw(in: ctx.cgContext, size: size, seed: seed)
        }
        #elseif canImport(AppKit)
        let image = NSImage(size: pixels)
        image.lockFocus()
        if let ctx = NSGraphicsContext.current?.cgContext {
            draw(in: ctx, size: size, seed: seed)
        }
        image.unlockFocus()
        return image
        #endif
    }

    // MARK: Drawing

    private static func draw(in ctx: CGContext, size: CGFloat, seed: String) {
        var rng = SeededRNG(seed: Self.stableHash(seed))
        let palette = palettes[Int(rng.next() % UInt64(palettes.count))]
        let rect = CGRect(x: 0, y: 0, width: size, height: size)

        // 1. Diagonal background gradient.
        let colorSpace = CGColorSpaceCreateDeviceRGB()
        if let gradient = CGGradient(
            colorsSpace: colorSpace,
            colors: [cgColor(palette[0]), cgColor(palette[1])] as CFArray,
            locations: [0, 1]
        ) {
            ctx.saveGState()
            ctx.addRect(rect)
            ctx.clip()
            ctx.drawLinearGradient(
                gradient,
                start: CGPoint(x: 0, y: 0),
                end: CGPoint(x: size, y: size),
                options: []
            )
            ctx.restoreGState()
        }

        // 2. Large soft accent glow.
        let glowR = rng.range(size * 0.45, size * 0.7)
        let glowC = CGPoint(x: rng.range(0, size), y: rng.range(0, size))
        ctx.setFillColor(cgColor(palette[2], alpha: 0.16))
        ctx.fillEllipse(in: CGRect(x: glowC.x - glowR, y: glowC.y - glowR,
                                   width: glowR * 2, height: glowR * 2))

        // 3. Bold "sun" / record disc.
        let discR = rng.range(size * 0.18, size * 0.30)
        let discC = CGPoint(x: rng.range(discR, size - discR),
                            y: rng.range(discR, size - discR))
        ctx.setFillColor(cgColor(palette[3], alpha: 0.92))
        ctx.fillEllipse(in: CGRect(x: discC.x - discR, y: discC.y - discR,
                                   width: discR * 2, height: discR * 2))

        // 4. Optional vinyl grooves around the disc.
        if rng.unit() < 0.55 {
            ctx.setStrokeColor(cgColor(palette[0], alpha: 0.45))
            ctx.setLineWidth(max(1, size * 0.004))
            let grooves = Int(rng.range(3, 7))
            for i in 1...grooves {
                let gr = discR * CGFloat(i) / CGFloat(grooves + 1)
                ctx.strokeEllipse(in: CGRect(x: discC.x - gr, y: discC.y - gr,
                                             width: gr * 2, height: gr * 2))
            }
        }

        // 5. A bold geometric element — a diagonal band or a triangle.
        ctx.saveGState()
        ctx.setFillColor(cgColor(palette[2], alpha: 0.85))
        if rng.unit() < 0.5 {
            // Rotated band sweeping across the cover.
            ctx.translateBy(x: rng.range(0, size), y: rng.range(0, size))
            ctx.rotate(by: rng.range(-0.9, 0.9))
            let bandH = rng.range(size * 0.06, size * 0.16)
            ctx.fill(CGRect(x: -size, y: -bandH / 2, width: size * 2, height: bandH))
        } else {
            // Triangle wedge anchored to an edge.
            let p1 = CGPoint(x: rng.range(0, size), y: 0)
            let p2 = CGPoint(x: rng.range(0, size), y: size)
            let p3 = CGPoint(x: rng.unit() < 0.5 ? 0 : size, y: rng.range(0, size))
            ctx.beginPath()
            ctx.move(to: p1)
            ctx.addLine(to: p2)
            ctx.addLine(to: p3)
            ctx.closePath()
            ctx.fillPath()
        }
        ctx.restoreGState()

        // 6. Vignette for depth.
        if let vignette = CGGradient(
            colorsSpace: colorSpace,
            colors: [cgColor(0x000000, alpha: 0), cgColor(0x000000, alpha: 0.28)] as CFArray,
            locations: [0.55, 1]
        ) {
            ctx.drawRadialGradient(
                vignette,
                startCenter: CGPoint(x: size / 2, y: size / 2), startRadius: size * 0.2,
                endCenter: CGPoint(x: size / 2, y: size / 2), endRadius: size * 0.75,
                options: []
            )
        }
    }

    // MARK: Helpers

    private static func cgColor(_ hex: UInt32, alpha: CGFloat = 1) -> CGColor {
        let r = CGFloat((hex >> 16) & 0xFF) / 255
        let g = CGFloat((hex >> 8) & 0xFF) / 255
        let b = CGFloat(hex & 0xFF) / 255
        return CGColor(srgbRed: r, green: g, blue: b, alpha: alpha)
    }

    /// FNV-1a — stable across launches (unlike `String.hashValue`), so a given
    /// recording renders the same cover every time.
    private static func stableHash(_ s: String) -> UInt64 {
        var h: UInt64 = 0xcbf2_9ce4_8422_2325
        for byte in s.utf8 {
            h = (h ^ UInt64(byte)) &* 0x0000_0100_0000_01b3
        }
        return h
    }
}

// MARK: - Seeded RNG (SplitMix64)

/// Deterministic PRNG so a seed string always produces the same cover.
private struct SeededRNG {
    private var state: UInt64

    init(seed: UInt64) {
        state = seed == 0 ? 0x9E37_79B9_7F4A_7C15 : seed
    }

    mutating func next() -> UInt64 {
        state = state &+ 0x9E37_79B9_7F4A_7C15
        var z = state
        z = (z ^ (z >> 30)) &* 0xBF58_476D_1CE4_E5B9
        z = (z ^ (z >> 27)) &* 0x94D0_49BB_1331_11EB
        return z ^ (z >> 31)
    }

    /// Uniform `CGFloat` in [0, 1).
    mutating func unit() -> CGFloat {
        CGFloat(next() >> 11) / CGFloat(UInt64(1) << 53)
    }

    /// Uniform `CGFloat` in [lo, hi).
    mutating func range(_ lo: CGFloat, _ hi: CGFloat) -> CGFloat {
        lo + unit() * (hi - lo)
    }
}

// MARK: - SwiftUI views

/// A generated cover as a resizable SwiftUI `Image`. Drop into the spot a real
/// remote cover would occupy; the call site supplies the frame/clip shape.
struct GenericCoverArtView: View {
    let seed: String
    var contentMode: ContentMode = .fill

    var body: some View {
        let img = GenericCoverArt.image(seed: seed)
        #if canImport(UIKit)
        Image(uiImage: img)
            .resizable()
            .aspectRatio(contentMode: contentMode)
        #elseif canImport(AppKit)
        Image(nsImage: img)
            .resizable()
            .aspectRatio(contentMode: contentMode)
        #endif
    }
}

/// Album-cover slot that shows generated art in screenshot mode and the real
/// remote artwork (`real`) otherwise. Use this to wrap the raw `AsyncImage`
/// album-art call sites that don't go through `CachedAsyncImage`.
///
/// When `seed` is nil there's no real cover to stand in for, so it always
/// renders `real` (which shows the app's normal no-art placeholder).
struct CoverArtImage<Real: View>: View {
    let seed: String?
    var contentMode: ContentMode = .fill
    @ViewBuilder var real: () -> Real

    @AppStorage(ScreenshotMode.defaultsKey) private var screenshotEnabled = false

    var body: some View {
        if (screenshotEnabled || ScreenshotMode.envEnabled), let seed, !seed.isEmpty {
            GenericCoverArtView(seed: seed, contentMode: contentMode)
        } else {
            real()
        }
    }
}
