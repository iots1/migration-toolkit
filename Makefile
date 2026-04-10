.PHONY: help setup install run run-reload run-no-reload clean test test-simple test-column test-suite

# Default Python version
PYTHON := python3.12
VENV := venv
BIN := $(VENV)/bin

help:
	@echo "HIS Migration Toolkit - Makefile Commands"
	@echo "==========================================="
	@echo ""
	@echo "Setup & Installation:"
	@echo "  make setup              Create venv and install dependencies"
	@echo "  make install            Install dependencies only (venv must exist)"
	@echo ""
	@echo "Development:"
	@echo "  make run                Run app with hot-reload (default)"
	@echo "  make run-reload         Run app with hot-reload (explicit)"
	@echo "  make run-no-reload      Run app without hot-reload"
	@echo ""
	@echo "Testing:"
	@echo "  make test               Run all unit tests (pytest discovers everything)"
	@echo "  make test-simple        Run AI pattern detection tests only"
	@echo "  make test-column        Run column analysis tests only"
	@echo "  make test-suite         Run tests/ directory only"
	@echo ""
	@echo "Utilities:"
	@echo "  make clean              Remove venv and __pycache__"
	@echo "  make help               Show this help message"
	@echo ""
	@echo "Examples:"
	@echo "  make setup              # First time: create env and install"
	@echo "  make run                # Start developing with hot-reload"
	@echo "  make test               # Run all unit tests"
	@echo "  make clean              # Clean up environment"
	@echo ""

setup: venv install
	@echo "✅ Environment ready! Run 'make run' to start."

venv:
	@echo "📦 Creating Python virtual environment..."
	$(PYTHON) -m venv $(VENV)
	@echo "✅ Virtual environment created: $(VENV)/"

install:
	@echo "📥 Installing dependencies from requirements.txt..."
	$(BIN)/pip install --upgrade pip
	$(BIN)/pip install -r requirements.txt
	@echo "✅ Dependencies installed!"

run: run-reload

run-reload:
	@echo "🚀 Starting HIS Migration Toolkit with hot-reload..."
	@echo "   App will reload automatically when you save files"
	@echo "   Open your browser to: http://localhost:8501"
	@echo ""
	. $(BIN)/activate && $(PYTHON) -m streamlit run app.py --server.runOnSave true

run-no-reload:
	@echo "🚀 Starting HIS Migration Toolkit (no hot-reload)..."
	@echo "   Open your browser to: http://localhost:8501"
	@echo ""
	. $(BIN)/activate && $(PYTHON) -m streamlit run app.py

test:
	@echo "🧪 Running all unit tests..."
	. $(BIN)/activate && $(PYTHON) -m pytest
	@echo "✅ All tests completed!"

test-simple:
	@echo "🧪 Running AI pattern detection tests..."
	. $(BIN)/activate && $(PYTHON) -m pytest test_analysis_simple.py
	@echo "✅ AI tests passed!"

test-column:
	@echo "🧪 Running column analysis tests..."
	. $(BIN)/activate && $(PYTHON) -m pytest test_column_analysis.py
	@echo "✅ Column analysis tests passed!"

test-suite:
	@echo "🧪 Running pytest test suite (tests/ only)..."
	. $(BIN)/activate && $(PYTHON) -m pytest tests/
	@echo "✅ Test suite passed!"

clean:
	@echo "🧹 Cleaning up..."
	rm -rf $(VENV)
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	@echo "✅ Cleaned!"

.SILENT: help
