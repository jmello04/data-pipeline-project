.PHONY: install run test clean help

# ── Variables ──────────────────────────────────────────────────────────────
PYTHON  := python
PYTEST  := pytest
VENV    := .venv
PIP     := $(VENV)/bin/pip || pip

# ── Targets ────────────────────────────────────────────────────────────────

help:
	@echo ""
	@echo "  install   Install all dependencies"
	@echo "  run       Execute the full pipeline"
	@echo "  test      Run the test suite"
	@echo "  clean     Remove generated artifacts (__pycache__, .pytest_cache)"
	@echo ""

install:
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements.txt

run:
	$(PYTHON) pipeline/run_all.py

test:
	$(PYTEST) tests/ -v --tb=short

clean:
	find . -type d -name "__pycache__" -not -path "./.git/*" -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .pytest_cache .mypy_cache || true
