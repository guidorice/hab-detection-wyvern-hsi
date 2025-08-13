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
# skip # B603: subprocess_without_shell_equals_true - subprocess call without shell=True
# skip # [B404:blacklist] Consider possible security implications associated with the subprocess module.
security-scan:

	bandit -s B603,B404 -r src/
