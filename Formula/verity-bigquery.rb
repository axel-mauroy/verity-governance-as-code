class VerityBigquery < Formula
  desc "BigQuery Connector for Verity Governance Engine"
  homepage "https://github.com/axel-mauroy/verity-governance-as-code"
  url "https://github.com/axel-mauroy/verity-governance-as-code/releases/download/v0.2.5/verity-bigquery-macos-universal"
  sha256 "2e9a11e20b1fb29d6ec339603c4d72700868e4b0661f6db3c7b9f979ed38ba92"

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
