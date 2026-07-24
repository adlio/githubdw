.PHONY: build test lint fmt fmt-check ci clean

build:
	cargo build --release

test:
	cargo test

lint:
	cargo clippy --all-targets -- -D warnings

fmt:
	cargo fmt

fmt-check:
	cargo fmt --check

ci: fmt-check lint test build
	@echo "CI checks passed"

clean:
	cargo clean
