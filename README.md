# rETL: Modular Rust ETL Pipeline

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
See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License
This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.