class Verity < Formula
  desc "Hexagonal Data Contract & Transformation Engine"
  homepage "https://github.com/axel-mauroy/verity-governance-as-code"
  url "https://github.com/axel-mauroy/verity-governance-as-code/releases/download/v0.2.8/verity-macos-universal"
  sha256 "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

  def install
    bin.install "verity-macos-universal" => "verity"
  end

  test do
    system bin/"verity", "--version"
  end
end
