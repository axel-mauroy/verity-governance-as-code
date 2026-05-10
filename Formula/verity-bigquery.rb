class VerityBigquery < Formula
  desc "BigQuery Connector for Verity Governance Engine"
  homepage "https://github.com/axel-mauroy/verity-governance-as-code"
  url "https://github.com/axel-mauroy/verity-governance-as-code/releases/download/v0.2.9/verity-bigquery-macos-universal"
  sha256 "9af1a767af88bbd6746d170b5c631bcff12098fb3c038e6a3326ba83e4159551"

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
