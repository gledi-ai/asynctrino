.DEFAULT_GOAL := help

SHELL := /bin/bash

PROJECTNAME := $(shell basename $(CURDIR))

PY_VERSION := 3.14

UV := $(shell which uv 2>/dev/null || echo "uv")
UVX := $(shell which uvx 2>/dev/null || echo "uvx")
NOX := $(UVX) --with nox-uv==0.8.0 nox@2026.10.20

VENV_DIR := $(CURDIR)/.venv
VENV_PROMPT := $(PROJECTNAME)-$(PY_VERSION)
PY := $(VENV_DIR)/bin/python

WORKERS ?= auto

# Goals after a test target are its pytest arguments (make test tests/unit), as is ARGS=...
ARG_GOALS := test unit test/unit integration test/integration cov test/cov cov/report test/lower xdist test/xdist
ifneq ($(filter $(firstword $(MAKECMDGOALS)),$(ARG_GOALS)),)
EXTRA_GOALS := $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))
endif
ARGS ?= $(EXTRA_GOALS)


##@ General

.PHONY: help print-% versions
.SILENT: help print-% versions

help: ## Show this help
	grep -E '^[a-zA-Z_/%. -]+:.*?## .*$$|^##@' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "} /^##@/ {printf "\n\033[1m%s\033[0m\n", substr($$0, 5); next} {target=$$1; gsub(/ +/, " | ", target); printf "  \033[36m%-22s\033[0m %s\n", target, $$2}'

print-%: ## Print any variable (e.g. make print-UV)
	echo '$*=$($*)'

versions: ## Show python, uv and asynctrino versions
	echo "uv:       $$($(UV) --version 2>/dev/null || echo 'not installed')"
	echo "python:   $$($(PY) --version 2>/dev/null || echo 'not found')"
	echo "asynctrino: $$($(PY) -c 'import asynctrino; print(asynctrino.__version__)' 2>/dev/null || echo 'not found')"


##@ Environment

.PHONY: venv lock relock sync sync/dry outdated outdated/no-cooldown tree dev dev/up
.SILENT: venv

venv: ## Create the virtual environment
	if ! [[ -d $(VENV_DIR) ]]; then \
		$(UV) venv --no-project --seed --link-mode=copy --prompt=$(VENV_PROMPT) --managed-python --python=$(PY_VERSION); \
	else \
		echo "Virtual environment already exists at $(VENV_DIR)"; \
	fi

lock: ## Lock dependencies, upgrading to the highest versions
	$(UV) lock --refresh --upgrade --resolution=highest

relock: ## Re-lock dependencies without upgrading
	$(UV) lock

sync: ## Sync the environment from the lockfile (all extras + dev group, which includes lint and test)
	$(UV) sync --locked --all-extras --link-mode=copy

sync/dry: ## Dry-run of sync
	$(UV) sync --locked --all-extras --dry-run

outdated: ## List outdated dependencies, respecting the exclude-newer release cooldown
	$(UV) pip list --outdated

# --exclude-newer on the command line keeps the exclude-newer-package cutoffs; only --no-config drops them.
outdated/no-cooldown: ## List outdated dependencies, ignoring the exclude-newer release cooldown
	$(UV) pip list --outdated --no-config

tree: ## Show the dependency tree
	$(UV) tree

dev: venv relock sync ## Set up the development environment (venv + relock + sync)

dev/up: venv lock sync ## Set up the development environment, WARNING: upgrades dependencies (venv + lock + sync)


##@ Build

.PHONY: build clean

build: ## Build the sdist and wheel into a clean dist/
	rm -rf dist/
	$(UV) build

clean: ## Remove build artifacts and caches
	rm -rf dist/ build/ htmlcov/
	rm -rf .pytest_cache/ .ruff_cache/ .nox/ .coverage coverage*.xml junit*.xml
	find . -path ./.venv -prune -o -type d -name '*.egg-info' -print -exec rm -rf {} +
	find . -path ./.venv -prune -o -type d -name __pycache__ -print -exec rm -rf {} +


##@ Release

.PHONY: changelog release/notes dist/metadata dist/check

GIT_CLIFF := git-cliff@2.14.2
TWINE := twine@7.0.0

changelog: ## Regenerate CHANGELOG.md; VERSION=vX.Y.Z files unreleased commits under that version
	$(UVX) $(GIT_CLIFF) $(if $(VERSION),--tag $(VERSION)) --output CHANGELOG.md

release/notes: ## Print the changelog section of the release tagged at HEAD
	$(UVX) --quiet $(GIT_CLIFF) --current --strip all

dist/metadata: ## Check the metadata of the packages in dist/ would render on PyPI (twine check --strict)
	$(UVX) $(TWINE) check --strict dist/*

dist/check: build ## Build, check metadata, then install and import the wheel and sdist on Python 3.14 and 3.15
	$(MAKE) --no-print-directory dist/metadata
	$(NOX) --session dist


##@ Testing

.PHONY: test unit test/unit integration test/integration cov test/cov cov/report test/matrix test/lower xdist test/xdist nox

test: ## Run all tests on the current interpreter (pass args: make test tests/unit, or ARGS=...)
	$(UV) run --locked pytest $(ARGS)

unit test/unit: ## Run unit tests only
	$(UV) run --locked pytest tests/unit $(ARGS)

integration test/integration: ## Run integration tests (needs Docker; TRINO_VERSION=previous|latest)
	$(UV) run --locked pytest tests/integration $(ARGS)

xdist test/xdist: ## Run tests in parallel with pytest-xdist (WORKERS=auto; pass args: make xdist tests/unit)
	$(UV) run --locked pytest -n $(WORKERS) $(ARGS)

cov test/cov: ## Run tests with branch coverage and enforce the 90% gate
	$(UV) run --locked pytest --cov --cov-report=term-missing $(ARGS)

cov/report: ## Run tests with coverage and write xml + html + junit reports
	$(UV) run --locked pytest --cov --cov-report=term-missing --cov-report=xml --cov-report=html --junitxml=junit.xml $(ARGS)

test/matrix: ## Run tests across all supported Python versions (3.14/3.15) via nox
	$(NOX) --session test

test/lower: ## Run tests on Python 3.14 against the lowest allowed dependency versions via nox (pass args: make test/lower tests/unit)
	$(NOX) --session test_lower -- $(ARGS)

nox: ## Run nox with ARGS (e.g. make nox ARGS="--session lint format typecheck")
	$(NOX) $(ARGS)


##@ Lint, format & typecheck

.PHONY: lint lint/check lint/fix fmt fmt/check fmt/fix typecheck check

lint lint/check: ## Check for lint issues (ruff check)
	$(UV) run --locked ruff check src/ tests/ noxfile.py

lint/fix: ## Fix lint issues (ruff check --fix), WARNING: modifies source files
	$(UV) run --locked ruff check --fix src/ tests/ noxfile.py

fmt fmt/check: ## Check formatting (ruff format --check)
	$(UV) run --locked ruff format --check src/ tests/ noxfile.py

fmt/fix: ## Format code (ruff format), WARNING: modifies source files
	$(UV) run --locked ruff format src/ tests/ noxfile.py

typecheck: ## Type check the package and tests (pyrefly, maximum strictness)
	$(UV) run --locked pyrefly check

check: lint/check fmt/check typecheck cov ## Run lint, format check, typecheck and tests with coverage


##@ Git hooks

.PHONY: hooks hooks/install

hooks/install: ## Install the prek git hooks (prek.toml)
	$(UVX) prek install --install-hooks

hooks: ## Run the prek hooks on all files
	$(UVX) prek run --all-files


ifneq ($(EXTRA_GOALS),)
.PHONY: $(EXTRA_GOALS)
$(EXTRA_GOALS):
	@:
endif
