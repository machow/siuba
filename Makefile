NOTEBOOK_TESTS=$(addprefix examples/, examples-dplyr-funcs.ipynb case-iris-select.ipynb examples-postgres.ipynb examples-varspec.ipynb)

test:
	py.test --nbval $(NOTEBOOK_TESTS)
	py.test

test-travis:
	py.test --nbval $(filter-out %postgres.ipynb, $(NOTEBOOK_TESTS))
	py.test

examples/%.ipynb:
	jupyter nbconvert --to notebook --inplace --execute $@
	jupytext --sync $@
