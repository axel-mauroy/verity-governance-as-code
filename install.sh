#!/usr/bin/env bash
set -e

# Verity CLI Installer
# This script downloads the latest release of Verity and places it in your PATH.

REPO="axel-mauroy/verity-governance-as-code"
BINARY_NAME="verity"
DEST_DIR=${DEST_DIR:-"/usr/local/bin"}

echo "🚀 Installing Verity Data Governance Engine..."

# Detect OS
OS="$(uname -s)"
case "${OS}" in
    Linux*)     PLATFORM="linux";;
    Darwin*)    PLATFORM="macos";;
    *)          echo "❌ Unsupported OS: ${OS}"; exit 1;;
esac

# Detect Architecture
ARCH="$(uname -m)"
case "${ARCH}" in
    x86_64)  ARCH="amd64" ;;
    arm64)   ARCH="universal" ;; # We build a universal binary for macos
    aarch64) ARCH="universal" ;;
    *)       echo "❌ Unsupported architecture: ${ARCH}"; exit 1;;
esac

# Combine to suffix expected by CD pipeline
if [ "$PLATFORM" = "macos" ]; then
    SUFFIX="macos-universal"
elif [ "$PLATFORM" = "linux" ]; then
    SUFFIX="linux-amd64"
    if [ "$ARCH" != "amd64" ]; then
        echo "❌ Linux currently expects amd64. Found: ${ARCH}"
        exit 1
    fi
fi

# Fetch the latest release version from GitHub API
echo "🔍 Finding latest release..."
VERSION=$(curl -s "https://api.github.com/repos/${REPO}/releases/latest" | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/')

if [ -z "$VERSION" ]; then
    echo "❌ Failed to fetch latest version from GitHub."
    exit 1
fi

echo "📦 Found version ${VERSION}"

# Construct download URL
DOWNLOAD_URL="https://github.com/${REPO}/releases/download/${VERSION}/${BINARY_NAME}-${SUFFIX}"
TMP_DIR=$(mktemp -d)
TMP_BIN="${TMP_DIR}/${BINARY_NAME}"

echo "⬇️  Downloading ${DOWNLOAD_URL}..."
curl -sL --fail -o "${TMP_BIN}" "${DOWNLOAD_URL}"

if [ ! -f "${TMP_BIN}" ]; then
    echo "❌ Failed to download binary."
    exit 1
fi

chmod +x "${TMP_BIN}"

# Install the binary
echo "🔧 Installing..."
if [ -w "${DEST_DIR}" ]; then
    mv "${TMP_BIN}" "${DEST_DIR}/${BINARY_NAME}"
    echo "✅ Verity installed successfully in ${DEST_DIR}/${BINARY_NAME}!"
else
    # Try sudo if available
    if command -v sudo >/dev/null 2>&1 && sudo -n true 2>/dev/null; then
        echo "🔑 Using sudo to write to ${DEST_DIR}"
        sudo mv "${TMP_BIN}" "${DEST_DIR}/${BINARY_NAME}"
        echo "✅ Verity installed successfully in ${DEST_DIR}/${BINARY_NAME}!"
    else
        # Fallback to local user bin
        LOCAL_BIN="${HOME}/.local/bin"
        echo "⚠️  Cannot write to ${DEST_DIR} without sudo. Falling back to ${LOCAL_BIN}"
        mkdir -p "${LOCAL_BIN}"
        mv "${TMP_BIN}" "${LOCAL_BIN}/${BINARY_NAME}"
        echo "✅ Verity installed successfully in ${LOCAL_BIN}/${BINARY_NAME}!"
        echo "⚠️  Make sure ${LOCAL_BIN} is in your PATH."
    fi
fi

# Cleanup
rm -rf "${TMP_DIR}"

echo ""
echo "Run 'verity --help' to get started."
