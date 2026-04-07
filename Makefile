.PHONY: install run test clean help

PYTHON := python3
VENV   := .venv

# ── Targets ────────────────────────────────────────────────────────────────

help:
	@echo ""
	@echo "  install   Create venv and install all dependencies"
	@echo "  run       Execute the full pipeline"
	@echo "  test      Run the test suite"
	@echo "  clean     Remove generated artifacts (__pycache__, .pytest_cache)"
	@echo ""

install:
	$(PYTHON) -m venv $(VENV)
	$(VENV)/bin/pip install --upgrade pip
	$(VENV)/bin/pip install -r requirements.txt
	@echo ""
	@echo "Done. Activate with: source $(VENV)/bin/activate"

run:
	$(PYTHON) pipeline/run_all.py

test:
	$(PYTHON) -m pytest tests/ -v --tb=short

clean:
	find . -type d -name "__pycache__" -not -path "./.git/*" -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -not -path "./.git/*" -delete 2>/dev/null || true
	rm -rf .pytest_cache .mypy_cache 2>/dev/null || true
