NOTEBOOK_TESTS=$(addprefix examples/, \
		examples-dplyr-funcs.ipynb case-iris-select.ipynb examples-postgres.ipynb examples-varspec.ipynb \
		examples-siu.ipynb \
		)

AUTODOC_SCRIPT=docs/generate_autodoc.py

AUTODOC_PAGES=docs/api_extra/vector.rst docs/api_extra/forcats.rst

.PHONY: docs

test:
	py.test --nbval $(NOTEBOOK_TESTS)
	pytest --dbs="sqlite,postgresql" siuba/

test-travis:
	py.test --nbval-lax $(filter-out %postgres.ipynb, $(NOTEBOOK_TESTS))
	pytest --dbs="sqlite,postgresql" siuba/

examples/%.ipynb:
	jupyter nbconvert --to notebook --inplace --execute $@
	jupytext --sync $@

docs/api_extra/%.rst: siuba/dply/%.py $(AUTODOC_SCRIPT)
	python3 docs/generate_autodoc.py . $< >> $@

docs-watch: $(AUTODOC_PAGES)
	cd docs && sphinx-autobuild . ./_build/html

docs-build: $(AUTODOC_PAGES)
	cd docs && sphinx-build . ./_build/html
