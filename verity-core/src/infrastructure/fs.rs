use crate::infrastructure::error::InfrastructureError;
use std::io::Write;
use std::path::Path;

/// Write content to a file atomically using a temporary file.
///
/// This function:
/// 1. Creates a temporary file in the same directory as the target path.
/// 2. Writes the content to the temporary file.
/// 3. Persists (renames) the temporary file to the target path.
///
/// This ensures that the target file is either fully written or not written at all,
/// preventing partial data corruption.
pub fn atomic_write<P: AsRef<Path>, C: AsRef<[u8]>>(
    path: P,
    content: C,
) -> Result<(), InfrastructureError> {
    let path = path.as_ref();
    let parent = path.parent().unwrap_or_else(|| Path::new("."));

    // Create a temporary file in the same directory to ensure atomic rename works across filesystems
    let mut temp_file = tempfile::NamedTempFile::new_in(parent).map_err(InfrastructureError::Io)?;

    // Write content
    temp_file
        .write_all(content.as_ref())
        .map_err(InfrastructureError::Io)?;

    // Atomic rename (persist)
    temp_file
        .persist(path)
        .map_err(|e| InfrastructureError::Io(e.error))?;

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use anyhow::Result;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_atomic_write_creates_file() -> Result<()> {
        let dir = tempdir()?;
        let file_path = dir.path().join("test.txt");
        let content = "Hello, World!";

        atomic_write(&file_path, content)?;

        assert!(file_path.exists());
        let read_content = fs::read_to_string(file_path)?;
        assert_eq!(read_content, content);
        Ok(())
    }

    #[test]
    fn test_atomic_write_overwrites_existing() -> Result<()> {
        let dir = tempdir()?;
        let file_path = dir.path().join("test.txt");

        // Initial write
        atomic_write(&file_path, "Initial")?;

        // Overwrite
        atomic_write(&file_path, "Updated")?;

        let read_content = fs::read_to_string(file_path)?;
        assert_eq!(read_content, "Updated");
        Ok(())
    }
}
