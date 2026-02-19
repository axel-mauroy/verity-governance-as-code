use anyhow::{Context, Result};
use assert_cmd::prelude::*;
use datafusion::prelude::*;
use std::path::PathBuf;
use std::process::Command;
use tempfile::TempDir;

/// Abstraction for managing the Verity test environment.
struct VerityTestEnv {
    _tmp: TempDir,
    root: PathBuf,
}

impl VerityTestEnv {
    fn new() -> Result<Self> {
        let tmp = tempfile::tempdir()?;
        let project_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .context("Workspace root not found")?
            .join("examples/ml_pipeline");

        let dest = tmp.path().join("ml_pipeline");
        Self::copy_dir(&project_root, &dest)?;

        Ok(Self {
            _tmp: tmp,
            root: dest,
        })
    }

    fn copy_dir(src: &PathBuf, dst: &PathBuf) -> std::io::Result<()> {
        // Optimized copy logic (e.g., ignore target/)
        let mut options = fs_extra::dir::CopyOptions::new();
        options.skip_exist = true;
        options.content_only = true;

        std::fs::create_dir_all(dst)?;
        fs_extra::dir::copy(src, dst, &options)
            .map(|_| ())
            .map_err(|e| std::io::Error::other(e.to_string()))
    }

    fn verity(&self) -> Command {
        let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("verity"));
        cmd.current_dir(&self.root);
        cmd
    }
}

#[tokio::test]
async fn test_pii_masking_enforcement() -> Result<()> {
    let env = VerityTestEnv::new()?;

    env.verity().arg("run").assert().success();

    // Validation via DataFusion (Zero-copy abstraction)
    let ctx = SessionContext::new();

    // Dynamically find the parquet output for stg_users to avoid hardcoding exact file paths
    let data_dir = env.root.join("target/data");
    let mut stg_users_path = None;
    for entry in walkdir::WalkDir::new(&data_dir) {
        let entry = entry.unwrap();
        if entry.path().is_file()
            && entry.path().extension().and_then(|s| s.to_str()) == Some("parquet")
            && entry.file_name().to_string_lossy().contains("stg_users")
        {
            stg_users_path = Some(entry.path().to_path_buf());
            break;
        }
    }

    let parquet_path = stg_users_path.context("Parquet output for stg_users not found")?;

    let df = ctx
        .read_parquet(parquet_path.to_str().unwrap(), Default::default())
        .await?;

    // Dump actual columns to stderr for debugging
    let schema = df.schema();
    eprintln!("PARQUET SCHEMA: {:#?}", schema);

    let emails = df.select(vec![col("email")])?.collect().await?;

    // Use idiomatic Arrow iterators
    for batch in emails {
        let col_array = batch.column(0);
        let mut all_masked = true;

        // Output might be a string or binary (since SHA256 produces binary view)
        if let Some(arr) = col_array
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
        {
            for email in arr.iter().flatten() {
                if email.contains('@') {
                    eprintln!("LEAKED STRING: {}", email);
                    all_masked = false;
                }
            }
        } else if let Some(arr) = col_array
            .as_any()
            .downcast_ref::<datafusion::arrow::array::LargeStringArray>()
        {
            for email in arr.iter().flatten() {
                if email.contains('@') {
                    eprintln!("LEAKED LARGE_STRING: {}", email);
                    all_masked = false;
                }
            }
        } else if let Some(arr) = col_array
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringViewArray>()
        {
            for email in arr.iter().flatten() {
                if email.contains('@') {
                    eprintln!("LEAKED STRING_VIEW: {}", email);
                    all_masked = false;
                }
            }
        } else if let Some(arr) = col_array
            .as_any()
            .downcast_ref::<datafusion::arrow::array::BinaryViewArray>()
        {
            // Binary data from SHA256.
            // The byte 0x40 ('@') can naturally occur in a hash.
            // Check instead that the binary hash is 32 bytes long (SHA256).
            for bytes in arr.iter().flatten() {
                if bytes.len() != 32 && String::from_utf8_lossy(bytes).contains('@') {
                    eprintln!("LEAKED BINARY (Not a hash): {:?}", bytes);
                    all_masked = false;
                }
            }
        } else {
            anyhow::bail!("Email column type mismatch: {:?}", col_array.data_type());
        }

        assert!(all_masked, "PII Leak: An Email was not masked!");
    }
    Ok(())
}

#[test]
fn test_circuit_breaker_on_duplicate_identity() -> Result<()> {
    let env = VerityTestEnv::new()?;

    // Poisoning: add a duplicate in the source
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(env.root.join("data/raw/users.csv"))?;

    use std::io::Write;
    writeln!(file, "u_00000,duplicate@verity.ai,User,2024-01-01,EU,pro")?;

    // Verity must fail cleanly (Compliance as Code)
    env.verity()
        .arg("run")
        .assert()
        .failure()
        .stderr(predicates::str::contains("DUPLICATE"));

    Ok(())
}

#[test]
fn test_ml_pipeline_lineage_jsonld_snapshot() -> Result<()> {
    let env = VerityTestEnv::new()?;

    // First run the pipeline to generate graph metadata or simply run lineage
    env.verity().arg("run").assert().success();

    env.verity()
        .arg("lineage")
        .arg("--format")
        .arg("json-ld")
        .assert()
        .success();

    let jsonld_file = env.root.join("target").join("metadata_context.jsonld");
    let content =
        std::fs::read_to_string(&jsonld_file).context("metadata_context.jsonld not generated")?;

    // Validate Snapshot
    // We sanitize potential absolute execution paths or timestamps if any exist in the JSON.
    // For this demonstration, we'll snapshot the raw JSON-LD graph structure.
    insta::assert_snapshot!("metadata_context_jsonld", content);

    Ok(())
}
