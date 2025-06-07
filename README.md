# rETL: Hybrid Rust + Python ETL Framework

| Pull Requests CI Status                                                                                   | Full Repository Analysis Status                                                                                 | Coverage (Codecov) Status                                                              |
|-----------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------|
| [![Rust CI](https://github.com/Uh-X3L/rETL/actions/workflows/ci-pr.yml/badge.svg)](https://github.com/Uh-X3L/rETL/actions/workflows/ci-pr.yml) | [![CI](https://github.com/Uh-X3L/rETL/actions/workflows/ci-full.yml/badge.svg)](https://github.com/Uh-X3L/rETL/actions/workflows/ci-full.yml) | [![codecov](https://codecov.io/gh/Uh-X3L/rETL/graph/badge.svg)](https://codecov.io/gh/Uh-X3L/rETL) |

rETL is a hybrid ETL (Extract, Transform, Load) framework combining high-performance Rust components with Python orchestration. 

## Project Structure

### Rust Components (High-Performance Core)
- `components/` - Core ETL crates:
  - `extract/` - Data extraction from various sources
  - `conform/` - Data profiling and schema normalization
  - `transform/` - Data transformation utilities
  - `load/` - Data loading to various sinks
  - `sqldb/` - Database connectivity and utilities

### Python Orchestration Layer
- `dags/` - Orchestrator definitions (Airflow DAGs, Prefect Flows, Dagster Jobs)
- `scripts/` - Spark job scripts for Kubernetes execution
- `utils/` - Shared utilities (secrets management, logging)
- `tests/` - Python test suite
- `infra/` - Infrastructure as Code (Helm charts, K8s manifests)
- `templates/` - Pipeline templates and examples

### Documentation & Configuration
- `docs/` - Architecture guides, migration plans, best practices
- `pyproject.toml` - Python dependencies and tooling configuration
- `.pre-commit-config.yaml` - Code quality hooks for both Rust and Python

## Getting Started

### Prerequisites
- **Rust** (1.70+): For building core components
- **Python** (3.9+): For orchestration layer
- **Poetry**: For Python dependency management
- **Docker** & **Kubernetes**: For deployment (optional)

### Quick Start

1. **Clone the repository:**
   ```sh
   git clone https://github.com/Uh-X3L/rETL.git
   cd rETL
   ```

2. **Build Rust components:**
   ```sh
   cargo build --workspace
   ```

3. **Setup Python environment:**
   ```sh
   # Install Poetry if not already installed
   curl -sSL https://install.python-poetry.org | python3 -
   
   # Install Python dependencies
   poetry install
   
   # Activate virtual environment
   poetry shell
   ```

4. **Install pre-commit hooks:**
   ```sh
   pre-commit install
   ```

### Usage Examples

#### Running a Spark Copy Job
```sh
# Example: Copy from SQL database to Parquet
python scripts/run_copy.py \
  --jdbc-url "jdbc:sqlserver://myserver.example.com:1433;database=mydb" \
  --sql "SELECT * FROM dbo.accounts WHERE created_date >= '2025-01-01'" \
  --output-path "s3://my-bucket/data/accounts/" \
  --username-secret "DB_USERNAME" \
  --password-secret "DB_PASSWORD"
```

#### Testing the Framework
```sh
# Run Python tests
pytest tests/

# Run Rust tests
cargo test --workspace

# Run all quality checks
pre-commit run --all-files
```

## Maintainers
- Created and maintained by [Uh-X3L](https://github.com/Uh-X3L) — primary maintainer

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

## Security Exceptions

See [SECURITY_EXCEPTIONS.md](SECURITY_EXCEPTIONS.md) for documented audit exceptions and rationale.
