# System Architecture

## Overall Design Philosophy

NautilusTrader employs a hybrid Python/Rust architecture that combines:
- **Rust Core**: High-performance, type-safe components for performance-critical operations
- **Python Interface**: User-friendly API leveraging Python's rich ecosystem
- **Event-Driven Design**: Message-passing architecture for deterministic behavior
- **Modular Components**: Pluggable adapters and components for extensibility

## Core Architecture Layers

### 1. Python User Interface Layer
**Location**: [`nautilus_trader/`](nautilus_trader/)
- **Purpose**: User-facing API, strategy development, configuration
- **Key Modules**:
  - [`nautilus_trader/examples/strategies/`](nautilus_trader/examples/strategies/) - Strategy examples
  - [`nautilus_trader/common/`](nautilus_trader/common/) - Common components and base classes
  - [`nautilus_trader/analysis/`](nautilus_trader/analysis/) - Performance analysis and reporting

### 2. Cython Bridge Layer
**Location**: [`nautilus_trader/**/*.pyx`](nautilus_trader/) (Cython extension modules)
- **Purpose**: High-performance Python extensions bridging to Rust core
- **Key Components**:
  - [`nautilus_trader/cache/cache.pyx`](nautilus_trader/cache/cache.pyx) - Caching layer
  - [`nautilus_trader/execution/engine.pyx`](nautilus_trader/execution/engine.pyx) - Execution engine
  - [`nautilus_trader/indicators/**/*.pyx`](nautilus_trader/indicators/) - Technical indicators

### 3. Rust Core Layer
**Location**: [`crates/`](crates/)
- **Purpose**: Performance-critical components with type safety and memory safety
- **Key Crates**:
  - [`crates/core/`](crates/core/) - Fundamental types and utilities
  - [`crates/model/`](crates/model/) - Domain models (instruments, orders, events)
  - [`crates/execution/`](crates/execution/) - Order execution and matching
  - [`crates/infrastructure/`](crates/infrastructure/) - Redis, messaging, persistence

## Component Relationships

```
┌─────────────────────────────────────────────────┐
│                Python Layer                     │
│  ┌─────────────┐  ┌─────────────┐  ┌──────────┐ │
│  │ Strategies  │  │  Analysis   │  │ Examples │ │
│  └─────────────┘  └─────────────┘  └──────────┘ │
└─────────────────────────────────────────────────┘
                        │
┌─────────────────────────────────────────────────┐
│              Cython Extensions                  │
│  ┌─────────────┐  ┌─────────────┐  ┌──────────┐ │
│  │    Cache    │  │ Execution   │  │Indicators│ │
│  └─────────────┘  └─────────────┘  └──────────┘ │
└─────────────────────────────────────────────────┘
                        │
┌─────────────────────────────────────────────────┐
│                 Rust Core                       │
│  ┌─────────────┐  ┌─────────────┐  ┌──────────┐ │
│  │    Model    │  │   Common    │  │   Core   │ │
│  │             │  │             │  │          │ │
│  │ Execution   │  │Infrastructure│  │Indicators│ │
│  └─────────────┘  └─────────────┘  └──────────┘ │
└─────────────────────────────────────────────────┘
```

## Key Design Patterns

### Event-Driven Architecture
- **Message Bus**: Central event routing via [`crates/infrastructure/`](crates/infrastructure/)
- **Event Types**: Commands, events, requests/responses
- **Deterministic Processing**: Nanosecond precision event ordering

### Adapter Pattern
**Location**: [`crates/adapters/`](crates/adapters/)
- **Purpose**: Uniform interface to different trading venues and data sources
- **Current Adapters**:
  - [`databento/`](crates/adapters/databento/) - Market data provider
  - [`coinbase_intx/`](crates/adapters/coinbase_intx/) - Crypto exchange
  - [`tardis/`](crates/adapters/tardis/) - Historical crypto data

### Cache Layer Pattern
**Location**: [`nautilus_trader/cache/`](nautilus_trader/cache/) and [`crates/infrastructure/`](crates/infrastructure/)
- **Purpose**: High-performance data access and state management
- **Implementations**:
  - In-memory cache for real-time access
  - Redis-backed persistence for state recovery
  - Database adapters for historical data

## Critical Implementation Paths

### Order Execution Flow
1. **Strategy** generates order → **Python Layer**
2. **Execution Engine** validates order → **Cython Layer**  
3. **Risk Manager** applies controls → **Rust Core**
4. **Venue Adapter** sends to exchange → **Rust Core**
5. **Event Bus** propagates fills → **All Layers**

### Market Data Flow
1. **Venue Adapter** receives data → **Rust Core**
2. **Data Engine** processes/transforms → **Rust Core**
3. **Cache** stores current state → **Rust/Python**
4. **Strategy** receives updates → **Python Layer**

### Backtesting Flow
1. **Historical Data** loaded → **Data Engine**
2. **Backtest Engine** replays events → **Rust Core**
3. **Simulated Execution** matches orders → **Rust Core**
4. **Analysis Engine** generates reports → **Python Layer**

## Build System Integration

### PyO3 Bindings
**Location**: [`crates/pyo3/`](crates/pyo3/)
- **Purpose**: Expose Rust functionality to Python
- **Compilation**: Static linking of Rust libraries into Python extensions
- **Features**: `extension-module`, `ffi`, `high-precision` support

### Build Process
**Primary Script**: [`build.py`](build.py)
1. **Rust Compilation**: `cargo build` with feature flags
2. **Cython Compilation**: Generate C extensions from `.pyx` files
3. **Linking**: Static link Rust libraries with Cython extensions
4. **Distribution**: Package wheels with compiled binaries

## Configuration and Extensibility

### Precision Modes
- **High-Precision**: 128-bit integers, 16 decimal places
- **Standard-Precision**: 64-bit integers, 9 decimal places
- **Platform Support**: Windows limited to standard-precision

### Feature Flags
**Rust Features**:
- `python` - Enable Python bindings
- `ffi` - Enable C FFI
- `extension-module` - Enable PyO3 extension module
- `high-precision` - Enable 128-bit precision mode

### Environment Configuration
**Key Variables**:
- `BUILD_MODE` - `release` or `debug`
- `HIGH_PRECISION` - Enable high-precision mode
- `RUST_TOOLCHAIN` - Rust compiler version

## Performance Characteristics

### Memory Management
- **Rust**: Zero-cost abstractions, no garbage collector
- **Python**: Managed object lifecycle via reference counting
- **Shared State**: Minimal copying between language boundaries

### Threading Model
- **Async/Await**: Tokio-based async runtime in Rust
- **Python Integration**: PyO3 async runtime integration
- **Event Loop**: Single-threaded event processing for determinism

### Persistence Strategy
- **Redis**: Message bus and state persistence
- **PostgreSQL**: Optional historical data storage
- **In-Memory**: Hot cache for real-time operations