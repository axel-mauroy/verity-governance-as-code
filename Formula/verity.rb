class Verity < Formula
  desc "Hexagonal Data Contract & Transformation Engine"
  homepage "https://github.com/axel-mauroy/verity-governance-as-code"
  url "https://github.com/axel-mauroy/verity-governance-as-code/releases/download/v0.1.0/verity-macos-universal"
  sha256 "580929494243f221783dbe434bef1957026346e559dd1ad37c8a8b23f7df94a4"
  version "0.1.0"

  def install
    bin.install "verity-macos-universal" => "verity"
  end

  test do
    system "#{bin}/verity", "--version"
  end
end
