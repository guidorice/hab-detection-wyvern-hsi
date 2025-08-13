.PHONY: test type-check security-scan lint-check format-check format

test: type-check security-scan lint-check format-check

# start development server
dev:
	dagster dev

# run static type checker
type-check:
	pyright src/ tests/

# run linter in check mode
lint-check:
	ruff check src/ tests/

# run format in check mode
format-check:
	ruff format --check src/ tests/

# run format in edit mode
format:
	ruff format src/ tests/

# run security scanner
security-scan:
	bandit -r src/
