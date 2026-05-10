class VerityBigquery < Formula
  desc "BigQuery Connector for Verity Governance Engine"
  homepage "https://github.com/axel-mauroy/verity-governance-as-code"
  url "https://github.com/axel-mauroy/verity-governance-as-code/releases/download/v0.2.8/verity-bigquery-macos-universal"
  sha256 "9772b46867570839a4a6b68453cd4eb5e50947aff15b38a09f9265a92db7fa7a"

  depends_on "axel-mauroy/verity-governance-as-code/verity"

  def install
    bin.install "verity-bigquery-macos-universal" => "verity-bigquery"
  end

  test do
    # verity-bigquery expects env vars to start, so simple version check might fail without them
    # but we can check if binary exists
    assert_path_exists bin/"verity-bigquery"
  end
end
