class Verity < Formula
  desc "Hexagonal Data Contract & Transformation Engine"
  homepage "https://github.com/axel-mauroy/verity-governance-as-code"
  url "https://github.com/axel-mauroy/verity-governance-as-code/releases/download/v0.2.8/verity-macos-universal"
  sha256 "c5eb7e0309c4ded3350af3add40d522d782059670e10bd25229043782dcd1098"

  def install
    bin.install "verity-macos-universal" => "verity"
  end

  test do
    system bin/"verity", "--version"
  end
end
