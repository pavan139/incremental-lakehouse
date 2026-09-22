PYTHON ?= python

.PHONY: install test demo evaluate verify
install:
	$(PYTHON) -m pip install -e . --no-build-isolation
test:
	PYTHONPATH=src $(PYTHON) -m unittest discover -s tests -v
demo:
	PYTHONPATH=src $(PYTHON) -m incremental_lakehouse demo --config contracts/experiment.json --output artifacts/demo.json
evaluate:
	PYTHONPATH=src $(PYTHON) -m incremental_lakehouse evaluate --config contracts/experiment.json --output artifacts/evaluation.json
verify:
	$(PYTHON) maintenance/check_publish.py --require-lint --wheel --output reports/verification.json
