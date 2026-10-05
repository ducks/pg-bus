.PHONY: help version-bump release test clean clippy fmt fmt-check lint install-hooks

# Auto-generate version from today's date with auto-incrementing patch
# Format: YYYYMMDD.0.X where X increments if releasing multiple times per day
define get_next_version
$(shell \
	TODAY=$$(date +%Y%m%d); \
	LATEST=$$(git tag -l "v$$TODAY.*" 2>/dev/null | sort -V | tail -1); \
	if [ -z "$$LATEST" ]; then \
		echo "$$TODAY.0.0"; \
	else \
		PATCH=$$(echo "$$LATEST" | sed 's/.*\.0\.\([0-9]*\)/\1/'); \
		echo "$$TODAY.0.$$((PATCH + 1))"; \
	fi \
)
endef

VERSION := $(get_next_version)

help:
	@echo "pg-bus Makefile"
	@echo ""
	@echo "Usage:"
	@echo "  make release                       - Auto-version and release (recommended)"
	@echo "  make release VERSION=20261005.0.0  - Release with specific version"
	@echo "  make test                          - Run tests (db_start first)"
	@echo "  make clippy                        - Run clippy"
	@echo "  make fmt / fmt-check               - Format / check formatting"
	@echo "  make lint                          - All CI checks (fmt-check, clippy, tests)"
	@echo "  make install-hooks                 - Install a pre-push hook running 'make lint'"
	@echo "  make clean                         - Clean build artifacts"
	@echo ""
	@echo "Next version will be: $(VERSION)"

# Bump version in Cargo.toml and commit on a branch
version-bump:
	@echo "Next version: $(VERSION)"
	@echo "Creating release branch for version $(VERSION)..."
	@git checkout -b release/v$(VERSION)
	@echo "Bumping version to $(VERSION)..."
	@sed -i 's/^version = .*/version = "$(VERSION)"/' Cargo.toml
	@echo "Updating Cargo.lock..."
	@cargo check --quiet 2>/dev/null || true
	@git add Cargo.toml Cargo.lock
	@git commit -m "chore: bump version to $(VERSION)"
	@echo ""
	@echo "Created branch release/v$(VERSION)"

# Merge to main, tag, push. No cargo publish yet: add it here once the
# crate is ready for crates.io.
release: version-bump
	@echo "Merging into main..."
	@git checkout main
	@git merge --no-ff release/v$(VERSION) -m "Merge branch 'release/v$(VERSION)'"
	@echo "Creating tag v$(VERSION) on main..."
	@git tag -a v$(VERSION) -m "Release v$(VERSION)"
	@echo "Pushing to origin..."
	@git push origin main
	@git push origin v$(VERSION)
	@echo ""
	@echo "Released v$(VERSION)"

# Tests run against the shell's PostgreSQL (DATABASE_URL, set by shell.nix);
# start it with db_start.
test:
	cargo nextest run --all-features

# Run clippy with warnings-as-errors (mirrors CI)
clippy:
	cargo clippy --all-targets --all-features -- -D warnings

clean:
	cargo clean

fmt:
	cargo fmt

fmt-check:
	cargo fmt -- --check

# Run all the checks CI runs, in order. Cheap to run locally before pushing.
lint: fmt-check clippy test

# Install a pre-push hook that runs `make lint` before any push.
install-hooks:
	@mkdir -p .git/hooks
	@printf '#!/usr/bin/env bash\nset -e\nexec make lint\n' > .git/hooks/pre-push
	@chmod +x .git/hooks/pre-push
	@echo "Installed pre-push hook -> make lint"
