class VerityBigquery < Formula
  desc "BigQuery Connector for Verity Governance Engine"
  homepage "https://github.com/axel-mauroy/verity-governance-as-code"
  url "https://github.com/axel-mauroy/verity-governance-as-code/releases/download/v0.2.7/verity-bigquery-macos-universal"
  sha256 "93cb90fd3f67ee8cdbccb046f3b3decc077e8389cef326cb707d0fa5b50919e1"
  version "0.2.7"

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
