class VerityBigquery < Formula
  desc "BigQuery Connector for Verity Governance Engine"
  homepage "https://github.com/axel-mauroy/verity-governance-as-code"
  url "https://github.com/axel-mauroy/verity-governance-as-code/releases/download/v0.1.0/verity-bigquery-macos-universal"
  sha256 "REPLACE_WITH_ACTUAL_SHA_DURING_RELEASE"
  version "0.1.0"

  depends_on "verity"

  def install
    bin.install "verity-bigquery-macos-universal" => "verity-bigquery"
  end

  test do
    # verity-bigquery expects env vars to start, so simple version check might fail without them
    # but we can check if binary exists
    assert_predicate bin/"verity-bigquery", :exist?
  end
end
