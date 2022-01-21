NOTEBOOK_TESTS=$(addprefix examples/, \
		examples-dplyr-funcs.ipynb case-iris-select.ipynb examples-postgres.ipynb examples-varspec.ipynb \
		examples-siu.ipynb \
		)

AUTODOC_SCRIPT=docs/scripts/generate_autodoc.py

AUTODOC_PAGES=docs/api_extra/vector.rst docs/api_extra/forcats.rst

SPHINX_BUILDARGS=-j auto

.PHONY: docs

test:
	py.test --nbval $(NOTEBOOK_TESTS)
	pytest --dbs="sqlite,postgresql" siuba/

test-travis:
	py.test --nbval-lax $(filter-out %postgres.ipynb, $(NOTEBOOK_TESTS))
	pytest --dbs="sqlite,postgresql" $(PYTEST_FLAGS) siuba/

examples/%.ipynb:
	jupyter nbconvert --to notebook --inplace --execute $@
	jupytext --sync $@

docs/api_extra/%.rst: siuba/dply/%.py $(AUTODOC_SCRIPT)
	python3 $(AUTODOC_SCRIPT) . $< > $@

docs-watch: $(AUTODOC_PAGES)
	cd docs && sphinx-autobuild . ./_build/html $(SPHINX_BUILDARGS)

docs-build: $(AUTODOC_PAGES)
	cd docs && sphinx-build . ./_build/html $(SPHINX_BUILDARGS)

github_traffic:
	# keep github traffic, since it is only held for 2 weeks
	github_get_traffic -c gh_traffic/config.ini -o gh_traffic

