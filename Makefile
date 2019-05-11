NOTEBOOK_TESTS=$(addprefix examples/, examples-dplyr-funcs.ipynb case-iris-select.ipynb examples-postgres.ipynb examples-varspec.ipynb)

.PHONY: docs

test:
	py.test --nbval $(NOTEBOOK_TESTS)
	pytest --dbs="sqlite,postgresql" siuba/tests

test-travis:
	py.test --nbval $(filter-out %postgres.ipynb, $(NOTEBOOK_TESTS))
	pytest --dbs="sqlite,postgresql" siuba/tests

examples/%.ipynb:
	jupyter nbconvert --to notebook --inplace --execute $@
	jupytext --sync $@

docs:
	cd docs && sphinx-autobuild . ./_build/html
