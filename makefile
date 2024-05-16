PYTHONPATH := $(shell pwd)/src

dummy:
	@echo "PYTHONPATH: $(PYTHONPATH)"
run:
#	source .env && cd src && flask db upgrade && flask run --port 5000 --debug --reload
	source .env && cd src && flask run --port 5000 --debug --reload

unittest:
	source .env && PYTHONPATH=$(PYTHONPATH) pytest -s tests/unit