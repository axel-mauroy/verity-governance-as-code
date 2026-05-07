class Verity < Formula
  desc "Hexagonal Data Contract & Transformation Engine"
  homepage "https://github.com/axel-mauroy/verity-governance-as-code"
  url "https://github.com/axel-mauroy/verity-governance-as-code/releases/download/v0.2.7/verity-macos-universal"
  sha256 "203bab54c770224c3fefa0fcc3b16decf0d348c0839305cf08036778294b30c4"
  version "0.2.7"

  def install
    bin.install "verity-macos-universal" => "verity"
  end

  test do
    system "#{bin}/verity", "--version"
  end
end
