// ocr.swift <pdf> : render every page at 3x and run Apple Vision text recognition (accurate, en),
// print the page's lines top-to-bottom, words joined left-to-right. Local, deterministic, no network.
import Foundation
import PDFKit
import Vision
import AppKit

let path = CommandLine.arguments[1]
guard let doc = PDFDocument(url: URL(fileURLWithPath: path)) else { fputs("cannot open\n", stderr); exit(1) }
for i in 0..<doc.pageCount {
    guard let page = doc.page(at: i) else { continue }
    let box = page.bounds(for: .mediaBox)
    let scale: CGFloat = 3.0
    let w = Int(box.width * scale), h = Int(box.height * scale)
    let cs = CGColorSpaceCreateDeviceRGB()
    guard let ctx = CGContext(data: nil, width: w, height: h, bitsPerComponent: 8, bytesPerRow: 0, space: cs,
                              bitmapInfo: CGImageAlphaInfo.premultipliedLast.rawValue) else { continue }
    ctx.setFillColor(CGColor(red: 1, green: 1, blue: 1, alpha: 1)); ctx.fill(CGRect(x: 0, y: 0, width: w, height: h))
    ctx.scaleBy(x: scale, y: scale)
    page.draw(with: .mediaBox, to: ctx)
    guard let img = ctx.makeImage() else { continue }
    let req = VNRecognizeTextRequest()
    req.recognitionLevel = .accurate
    req.usesLanguageCorrection = false
    req.recognitionLanguages = ["en-US"]
    let handler = VNImageRequestHandler(cgImage: img, options: [:])
    try? handler.perform([req])
    let obs = (req.results ?? []).compactMap { o -> (CGFloat, CGFloat, CGFloat, String)? in
        guard let s = o.topCandidates(1).first?.string else { return nil }
        return (1 - o.boundingBox.midY, o.boundingBox.minX, o.boundingBox.height, s)
    }.sorted { $0.0 < $1.0 }
    // group into lines: same line when vertical centres are within half a text height
    var lines: [[(CGFloat, CGFloat, CGFloat, String)]] = []
    for o in obs {
        if var last = lines.last, let f = last.first, abs(f.0 - o.0) < max(f.2, o.2) * 0.5 {
            last.append(o); lines[lines.count - 1] = last
        } else { lines.append([o]) }
    }
    print("<<<PAGE \(i + 1)>>>")
    for l in lines { print(l.sorted { $0.1 < $1.1 }.map { $0.3 }.joined(separator: " ")) }
}
