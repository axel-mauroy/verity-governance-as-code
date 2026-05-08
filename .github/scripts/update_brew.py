# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "packaging",
# ]
# ///

import os
import re
import sys
from pathlib import Path
from packaging import version

# Standardize encoding for cross-platform consistency
TEXT_ENCODING = "utf-8"

def validate_and_format_version(tag: str) -> str:
    """
    Validates the tag against PEP 440 and returns the clean version string.
    """
    try:
        # Check if the tag starts with 'v' and strip it for parsing
        clean_tag = tag.lstrip('v')
        v = version.parse(clean_tag)
        
        if v.is_prerelease or v.is_devrelease:
            # Using GitHub Actions annotation format
            print(f"::warning:: Version {tag} is a pre-release or dev-release.")
        
        return str(v)
    except version.InvalidVersion:
        print(f"::error:: '{tag}' is not a valid PEP 440 version format.")
        sys.exit(1)

def extract_sha256(checksum_data: str, binary_name: str) -> str:
    """
    Extracts the SHA256 hash for a specific binary from the checksum file content.
    """
    # Regex breakdown:
    # ^([a-fA-F0-9]{64}) -> Match 64 hex chars (SHA256) at start of line
    # \s+                -> One or more spaces
    # .*?\b{name}\b      -> Non-greedy match until the exact binary name (word boundary)
    pattern = rf"^([a-fA-F0-9]{{64}})\s+.*?\b{re.escape(binary_name)}\b.*$"
    match = re.search(pattern, checksum_data, re.MULTILINE)
    
    if not match:
        raise ValueError(f"No SHA256 entry found for binary: '{binary_name}'")
    
    return match.group(1)

def update_homebrew_formula(formula_path: Path, binary_name: str, version_str: str, sha256_hash: str):
    """
    Updates specific fields in a Homebrew formula file with atomicity.
    """
    if not formula_path.exists():
        raise FileNotFoundError(f"Formula file not found at: {formula_path}")

    content = formula_path.read_text(encoding=TEXT_ENCODING)

    # We use specific lookbehind/lookahead or anchor patterns to avoid 
    # replacing URLs or hashes in comments or other unexpected places.
    substitutions = [
        (r'(url\s+").*?(")', f'url "https://github.com/axel-mauroy/verity-governance-as-code/releases/download/v{version_str}/{binary_name}"'),
        (r'(version\s+").*?(")', f'version "{version_str}"'),
        (r'(sha256\s+").*?(")', f'sha256 "{sha256_hash}"'),
    ]

    for pattern, replacement in substitutions:
        # Using \g<1> and \g<2> prevents the issue where the replacement starts with a digit (e.g., '0.2.8' causing \10)
        content = re.sub(pattern, rf'\g<1>{replacement.split(" ", 1)[1].strip(""" " """)}\g<2>', content)

    formula_path.write_text(content, encoding=TEXT_ENCODING)
    print(f"✅ Updated {formula_path.name} for {binary_name}")

def run():
    # 1. Environment and Version Validation
    tag = os.environ.get('GITHUB_REF_NAME', '')
    if not tag.startswith('v'):
        print(f"::error::Tag '{tag}' must start with 'v' for release automation.")
        sys.exit(1)
    
    valid_version = validate_and_format_version(tag)
    
    # 2. Checksum Data Retrieval
    checksum_path = Path("release_assets/checksums.txt")
    if not checksum_path.exists():
        print(f"::error::Checksum file missing at {checksum_path}")
        sys.exit(1)
    
    checksum_data = checksum_path.read_text(encoding=TEXT_ENCODING)

    # 3. Formula Updates
    # Mapping formula files to their specific binary targets
    targets = {
        "Formula/verity.rb": "verity-macos-universal",
        "Formula/verity-bigquery.rb": "verity-bigquery-macos-universal"
    }

    try:
        for formula_str, binary in targets.items():
            formula_path = Path(formula_str)
            sha = extract_sha256(checksum_data, binary)
            update_homebrew_formula(formula_path, binary, valid_version, sha)
        
        # 4. GITHUB_ENV update
        github_env_path = os.environ.get('GITHUB_ENV')
        if github_env_path:
            with open(github_env_path, 'a', encoding=TEXT_ENCODING) as env_file:
                env_file.write(f"VERSION={valid_version}\n")
        else:
            print("::warning:: GITHUB_ENV not found, skipping environment export.")
            
    except Exception as error:
        print(f"::error::Automation failed: {error}")
        sys.exit(1)

if __name__ == "__main__":
    run()