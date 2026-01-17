# Current Context

## Project Status

- **Active Development**: Bi-weekly release cycle
- **Python Version**: 1.215.0 
- **Rust Version**: 0.45.0
- **Target**: v2.0 stable API milestone
- **License**: LGPL-3.0

## Current Work Focus

### Primary: Rust Core Migration
The project is actively porting performance-critical components from Cython to Rust:
- **Goal**: Leverage Rust's performance and safety features
- **Strategy**: Replace existing Cython modules with Rust implementations
- **Integration**: PyO3 bindings for Python interoperability
- **Status**: Ongoing with core components partially migrated

### Secondary Priorities
1. **Documentation Enhancement**: Filling gaps in user and developer guides
2. **Code Ergonomics**: Improving type annotations and API intuitiveness
3. **Adapter Expansion**: Adding new trading venue integrations

## Recent Development Areas

Based on open files and project structure, current work includes:
- **Infrastructure Layer**: Redis cache and message bus implementations
- **SQL Integration**: Database cache adapters for persistence
- **Python Bindings**: PyO3 integration for Rust-Python bridge

## Immediate Next Steps

1. **Complete Rust Migration**: Continue porting remaining Cython components
2. **API Stabilization**: Work toward v2.0 stable interface
3. **Performance Benchmarking**: Measure improvements from Rust migration
4. **Integration Testing**: Ensure Python-Rust interoperability

## Development Environment

- **Build System**: Custom [`build.py`](build.py) script with Cargo integration
- **Package Manager**: uv for Python dependencies
- **Testing**: cargo-nextest for Rust, pytest for Python
- **CI/CD**: GitHub Actions with comprehensive test suites

## Key Technical Considerations

- **Precision Modes**: High-precision (128-bit) vs standard (64-bit) support
- **Platform Support**: Full support on Linux/macOS, limited Windows support
- **Performance**: Focus on zero-latency deployment between research and production