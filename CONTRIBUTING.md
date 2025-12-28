# Contributing to ImpactU Airflow

## Development Workflow

### For Contributors (Fork Workflow)

1. **Fork and Clone:**
   ```bash
   # Fork the repo on GitHub, then clone your fork
   git clone https://github.com/YOUR_USERNAME/impactu_airflow.git
   cd impactu_airflow
   ```

2. **Set up Development Environment:**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   pip install -r requirements-dev.txt
   ```

3. **Install Pre-commit Hooks:**
   ```bash
   pre-commit install
   ```

4. **Create a Feature Branch:**
   ```bash
   git checkout -b feat/your-feature-name
   ```

5. **Make Your Changes:**
   - Write code following PEP 8 style
   - Add NumPy-style docstrings
   - Include type hints
   - Write tests for new features

6. **Run Local Quality Checks:**
   ```bash
   # Format code
   ruff format .

   # Check linting
   ruff check .

   # Type checking
   mypy .

   # Run tests
   pytest
   ```

7. **Commit and Push:**
   ```bash
   git add .
   git commit -m "feat: your descriptive commit message"
   git push origin feat/your-feature-name
   ```

8. **Open a Pull Request:**
   - Go to the original repo on GitHub
   - Click "New Pull Request"
   - Select your fork and branch
   - Fill in the PR template with details

## Pull Request Review Process

### Automated Checks (Run on All PRs)

When you open a PR, these checks run automatically:

1. **Code Quality** (`code-quality` job):
   - Ruff linting
   - Ruff formatting check
   - Mypy type checking

2. **Tests** (`test` job):
   - DAG integrity tests
   - Feature-specific tests (if files changed)

These checks **do not require maintainer approval** and run on forks.

### Validation on Dev Environment (Requires Approval)

For security reasons, validation on the dev Airflow instance requires maintainer approval:

1. A **maintainer reviews your code** for security
2. If approved, maintainer adds the `validate-on-dev` label to your PR
3. The `validate-on-dev` job triggers:
   - Validates DAG in dev.airflow.colav.co
   - Runs integration tests
   - Reports results back to the PR

**Why this process?**
- PRs from forks don't have access to repository secrets (GitHub security)
- The `validate-on-dev` job needs `AIRFLOW_API_TOKEN` to trigger validation
- Using `pull_request_target` + label ensures only reviewed code runs with secrets

### For Maintainers

To approve a PR for dev validation:

```bash
# Review the PR code thoroughly
# If safe, add the label:
gh pr edit <PR_NUMBER> --add-label "validate-on-dev"
```

Or via GitHub UI:
1. Go to the PR
2. On the right sidebar, click "Labels"
3. Select "validate-on-dev"

## Code Style Guidelines

- **Python Version:** 3.12+
- **Line Length:** 100 characters
- **Docstrings:** NumPy style
- **Type Hints:** Required for all functions
- **Formatting:** Automated by Ruff
- **Linting:** Enforced by Ruff
- **Import Order:** Handled by Ruff (isort rules)

## Testing Requirements

- All new features must include tests
- DAG changes must pass DAG integrity tests
- Tests should use `pytest` framework
- Mock external dependencies (MongoDB, APIs, etc.)

## Commit Message Format

Follow conventional commits:

```
feat: add new extractor for DataSource
fix: correct timezone handling in DAG
docs: update README with setup instructions
test: add tests for scimagojr extractor
refactor: simplify MongoDB connection logic
chore: update dependencies
```

## Questions?

- Open an issue for bugs or feature requests
- Join discussions in existing issues/PRs
- Tag maintainers for urgent questions

## License

By contributing, you agree that your contributions will be licensed under the project's license.
