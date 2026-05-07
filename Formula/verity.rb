class Verity < Formula
  desc "Hexagonal Data Contract & Transformation Engine"
  homepage "https://github.com/axel-mauroy/verity-governance-as-code"
  url "https://github.com/axel-mauroy/verity-governance-as-code/releases/download/v0.2.6/verity-macos-universal"
  sha256 "e44834643bd91d57e89d33f7968eb0bbe51add4649b2b3f859af7737e3cd285c"
  version "0.2.6"

  def install
    bin.install "verity-macos-universal" => "verity"
  end

  test do
    system "#{bin}/verity", "--version"
  end
end
