#!/bin/bash
set -e

echo "🚀 SessionStart Hook: Initializing Graft development environment..."

# Only run setup in remote web environment
if [ "$CLAUDE_CODE_REMOTE" != "true" ]; then
  echo "ℹ️  Local CLI environment detected - skipping dependency installation"
  exit 0
fi

echo "🌐 Web session detected - setting up dependencies..."

# Try to install cargo-binstall if not present (for fast binary installations)
USE_BINSTALL=false
if ! command -v cargo-binstall &> /dev/null; then
  echo "📦 Installing cargo-binstall..."
  if curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash > /dev/null 2>&1; then
    USE_BINSTALL=true
  else
    echo "⚠️  cargo-binstall installation failed, falling back to cargo install"
    USE_BINSTALL=false
  fi
else
  USE_BINSTALL=true
fi

# Collect cargo binaries to install
cargo_bins=()
if ! command -v just &> /dev/null; then
  cargo_bins+=("just")
fi
if ! command -v cargo-nextest &> /dev/null; then
  cargo_bins+=("cargo-nextest")
fi

# Collect apt packages to install
apt_packages=()
if ! command -v mold &> /dev/null; then
  apt_packages+=("mold")
fi
if ! dpkg -l | grep -q libclang-dev; then
  apt_packages+=("libclang-dev")
fi

# Install cargo binaries in parallel (if any needed)
if [ ${#cargo_bins[@]} -gt 0 ]; then
  echo "📦 Installing cargo binaries: ${cargo_bins[*]}..."
  {
    if [ "$USE_BINSTALL" = true ]; then
      cargo binstall -y --quiet "${cargo_bins[@]}" > /dev/null 2>&1
    else
      for bin in "${cargo_bins[@]}"; do
        if [ "$bin" = "cargo-nextest" ]; then
          cargo install cargo-nextest --locked --quiet > /dev/null 2>&1
        else
          cargo install "$bin" --quiet > /dev/null 2>&1
        fi
      done
    fi
    echo "✓ Cargo binaries installed"
  } &
  cargo_pid=$!
else
  echo "✓ All cargo binaries already installed"
  cargo_pid=""
fi

# Install apt packages in parallel (if any needed)
if [ ${#apt_packages[@]} -gt 0 ]; then
  echo "📦 Installing apt packages: ${apt_packages[*]}..."
  {
    apt-get update -qq && apt-get install -y -qq "${apt_packages[@]}" > /dev/null 2>&1
    echo "✓ Apt packages installed"
  } &
  apt_pid=$!
else
  echo "✓ All apt packages already installed"
  apt_pid=""
fi

# Wait for parallel installations to complete
[ -n "$cargo_pid" ] && wait $cargo_pid
[ -n "$apt_pid" ] && wait $apt_pid

# Compile SQLite versions in parallel
echo "🔨 Compiling SQLite versions..."
{
  just run sqlite test > /dev/null 2>&1 || true
  echo "✓ SQLite test version ready"
} &
sqlite_test_pid=$!

{
  just run sqlite bin > /dev/null 2>&1 || true
  echo "✓ SQLite bin version ready"
} &
sqlite_bin_pid=$!

# Wait for SQLite compilations
wait $sqlite_test_pid
wait $sqlite_bin_pid

echo "✅ SessionStart Hook: Setup complete!"
echo "   You can now run 'just test' to run the test suite"
