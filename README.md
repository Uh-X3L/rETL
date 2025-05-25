# rETL: Modular Rust ETL Pipeline

[![CI](https://github.com/Uh-X3L/rETL/actions/workflows/ci.yml/badge.svg)](https://github.com/Uh-X3L/rETL/actions/workflows/ci.yml)

rETL is a modular, extensible ETL (Extract, Transform, Load) framework written in Rust, designed for data engineering and analytics workflows. The project is organized as a Rust workspace with separate crates for each ETL stage.

## Project Structure
- `components/` - Main ETL crates:
  - `extract/` - Data extraction from various sources
  - `conform/` - Data profiling and schema normalization
  - `transform/` - Data transformation utilities
  - `load/` - Data loading to various sinks
- `docs/` - Documentation, milestones, and guides
- `scripts/` - Utility scripts (e.g., issue automation)

## Getting Started
1. **Clone the repo:**
   ```sh
   git clone https://github.com/Uh-X3L/rETL.git
   cd rETL
   ```
2. **Build the workspace:**
   ```sh
   cargo build --workspace
   ```
3. **Run the CLI:**
   ```sh
   cd components/data-profiler
   cargo run -- [OPTIONS]
   ```

## Maintainers
- [Uh-X3L](https://github.com/Uh-X3L) — primary maintainer

## Contributing
We welcome contributions! To get started:
- Fork the repository and create your branch from `main`.
- Ensure your code passes all CI checks (see the CI badge above).
- Open a pull request with a clear description of your changes.
- For major changes, please open an issue first to discuss what you would like to change.

## License
This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.

## Testing

Before submitting a pull request, please ensure your code passes all quality checks:

- Run code formatting:
  ```sh
  cargo fmt --all
  ```
- Run the linter:
  ```sh
  cargo clippy --workspace --all-targets -- -D warnings
  ```
- Run all tests:
  ```sh
  cargo test --workspace --all-targets
  ```
- Run security audit:
  ```sh
  cargo install cargo-audit --locked # if not already installed
  cargo audit
  ```
- Build documentation:
  ```sh
  cargo doc --workspace --no-deps --document-private-items
  ```

All of these checks are run automatically in CI, but running them locally helps you catch issues early.
