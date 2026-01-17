# Technology Stack and Setup

## Core Technologies

### Programming Languages
- **Rust**: Core performance-critical components (crates)
- **Python**: User interface, strategy development, analysis
- **Cython**: Bridge layer between Python and Rust
- **C**: Low-level system interfaces and bindings

### Platform Support
- **Linux (x86_64)**: Primary development and production platform
- **macOS (arm64/x86_64)**: Full support with native compilation
- **Windows (x86_64)**: Limited to standard-precision mode

## Language Versions and Requirements

### Rust
- **Minimum Version**: 1.86.0+
- **Edition**: 2024
- **Toolchain**: Stable (configurable via `RUST_TOOLCHAIN`)
- **Features**: `extension-module`, `ffi`, `python`, `high-precision`

### Python
- **Supported Versions**: 3.11, 3.12
- **Required**: >=3.11,<3.13
- **Build Tools**: uv (package manager), poetry-core (build backend)

### Cython
- **Version**: 3.1.0a1 (pinned for coverage support)
- **Purpose**: Generate C extensions from .pyx files
- **Features**: Profile mode, annotation mode, parallel compilation

## Core Dependencies

### Python Runtime Dependencies
```toml
click>=8.1.8,<9.0.0           # CLI interface
fsspec>=2025.2.0,<2026.0.0    # Filesystem abstraction
msgspec>=0.19.0,<1.0.0        # Fast serialization
numpy>=1.26.4                  # Numerical computing
pandas>=2.2.3,<3.0.0          # Data analysis
pyarrow>=19.0.1               # Columnar data format
pytz>=2025.1.0                # Timezone handling
tqdm>=4.67.1,<5.0.0           # Progress bars
uvloop>=0.21.0,<1.0.0         # Fast async event loop (Unix only)
```

### Rust Core Dependencies
```toml
tokio = "1.44.1"              # Async runtime
pyo3 = "0.24.1"               # Python bindings
chrono = "0.4.40"             # Date/time handling
serde = "1.0.219"             # Serialization framework
rust_decimal = "1.37.1"       # High-precision decimals
redis = "0.29.2"              # Redis client (optional)
sqlx = "0.8.3"                # PostgreSQL client (optional)
```

## Build System

### Build Configuration
- **Primary Script**: [`build.py`](build.py)
- **Backend**: poetry-core (setuptools-compatible)
- **Automation**: [`Makefile`](Makefile) with common targets

### Environment Variables
```bash
BUILD_MODE=release            # release | debug
HIGH_PRECISION=true           # Enable 128-bit precision (Linux/macOS only)
RUST_TOOLCHAIN=stable         # stable | nightly
PARALLEL_BUILD=true           # Enable parallel compilation
COPY_TO_SOURCE=true           # Copy built files to source tree
PYO3_ONLY=false              # Build only PyO3 extensions
```

### Build Targets (Makefile)
```bash
make install                  # Full release build with dependencies
make install-debug            # Full debug build
make build                    # Build extensions only (release)
make build-debug              # Build extensions only (debug)
make clean                    # Remove build artifacts
make docs                     # Generate documentation
```

## Development Dependencies

### Python Development Tools
```toml
black>=25.1.0                 # Code formatter
ruff>=0.11.4                  # Linter and formatter
mypy>=1.15.0                  # Type checker
pre-commit>=4.2.0             # Git hooks
pytest>=7.4.4                # Testing framework
coverage>=7.8.0               # Code coverage
```

### Rust Development Tools
```bash
cargo-nextest                 # Fast test runner
cargo-llvm-cov               # Coverage reporting
clippy                       # Linter
rustfmt                      # Code formatter
```

## Optional Integrations

### Trading Venue Adapters
```toml
# Betfair sports betting
betfair = ["betfair-parser==0.14.4"]

# Interactive Brokers
ib = ["defusedxml>=0.7.1", "nautilus-ibapi==10.30.1"]

# dYdX derivatives exchange
dydx = ["v4-proto==7.0.5", "grpcio==1.68.1", "protobuf==5.29.1"]

# Polymarket prediction markets
polymarket = ["py-clob-client>=0.20.0"]
```

### Infrastructure
```toml
# Docker support
docker = ["docker>=7.1.0"]

# Redis message bus and cache
redis = ["redis==0.29.2"]

# PostgreSQL persistence
postgres = ["sqlx==0.8.3"]
```

## Installation Methods

### From PyPI (Recommended)
```bash
pip install -U nautilus_trader
```

### From Source (Development)
```bash
# Install Rust toolchain
curl https://sh.rustup.rs -sSf | sh

# Install clang compiler
sudo apt-get install clang  # Linux
# brew install llvm         # macOS

# Install uv package manager
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone and build
git clone --branch develop --depth 1 https://github.com/nautechsystems/nautilus_trader
cd nautilus_trader
uv sync --all-extras
```

### Docker Deployment
```bash
# Official images
docker pull ghcr.io/nautechsystems/nautilus_trader:latest
docker pull ghcr.io/nautechsystems/jupyterlab:latest
```

## Testing Infrastructure

### Rust Testing
```bash
cargo nextest run --workspace --features "python,ffi,high-precision"
```

### Python Testing
```bash
pytest --new-first --failed-first
```

### Performance Testing
```bash
pytest tests/performance_tests --benchmark-disable-gc --codspeed
```

## Precision Modes

### High-Precision Mode (Default)
- **Integers**: 128-bit
- **Decimal Places**: Up to 16
- **Platforms**: Linux, macOS
- **Enable**: `HIGH_PRECISION=true`

### Standard-Precision Mode
- **Integers**: 64-bit  
- **Decimal Places**: Up to 9
- **Platforms**: All (required for Windows)
- **Enable**: `HIGH_PRECISION=false`

## Development Workflow

### Common Commands
```bash
# Quick development build
make build-debug

# Full installation with tests
make install
make pytest

# Code quality checks
make pre-commit
make ruff

# Documentation generation
make docs

# Clean build artifacts
make clean
```

### Pre-commit Hooks
- **Rust**: clippy, rustfmt
- **Python**: black, ruff, mypy
- **General**: trailing whitespace, yaml validation