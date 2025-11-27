#!/bin/bash
set -e

echo "🚀 SessionStart Hook: Initializing Graft development environment..."

# Only run setup in remote web environment
if [ "$CLAUDE_CODE_REMOTE" != "true" ]; then
  echo "ℹ️  Local CLI environment detected - skipping dependency installation"
  exit 0
fi

echo "🌐 Web session detected - setting up dependencies..."

# Install just (command runner)
if ! command -v just &> /dev/null; then
  echo "📦 Installing just..."
  cargo install just --quiet
else
  echo "✓ just already installed"
fi

# Install cargo-nextest (test runner)
if ! command -v cargo-nextest &> /dev/null; then
  echo "📦 Installing cargo-nextest..."
  cargo install cargo-nextest --locked --quiet
else
  echo "✓ cargo-nextest already installed"
fi

# Install mold (fast linker)
if ! command -v mold &> /dev/null; then
  echo "📦 Installing mold linker..."
  apt-get update -qq && apt-get install -y -qq mold > /dev/null 2>&1
else
  echo "✓ mold already installed"
fi

# Install libclang-dev (for bindgen)
if ! dpkg -l | grep -q libclang-dev; then
  echo "📦 Installing libclang-dev..."
  apt-get install -y -qq libclang-dev > /dev/null 2>&1
else
  echo "✓ libclang-dev already installed"
fi

# Compile SQLite versions
echo "🔨 Compiling SQLite test version..."
just run sqlite test > /dev/null 2>&1 || true

echo "🔨 Compiling SQLite bin version..."
just run sqlite bin > /dev/null 2>&1 || true

echo "✅ SessionStart Hook: Setup complete!"
echo "   You can now run 'just test' to run the test suite"
