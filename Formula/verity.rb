class Verity < Formula
  desc "Hexagonal Data Contract & Transformation Engine"
  homepage "https://github.com/axel-mauroy/verity-governance-as-code"
  url "https://github.com/axel-mauroy/verity-governance-as-code/releases/download/v0.2.9/verity-macos-universal"
  sha256 "3ddae56b58b5d858e8c33d521e5dfcc5f9cf9490e35968899ca46fa2ad81dbf4"

  def install
    bin.install "verity-macos-universal" => "verity"
  end

  test do
    system bin/"verity", "--version"
  end
end
