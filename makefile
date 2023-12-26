TEST      = 12_00
TEST_FULL = stateSaves/$(TEST).pickle


test:
	python3 apps/powerTest.py $(TEST_FULL)


unprotect:
	chmod -R 777 apps


deploy:
	 rm -rf apps/__pycache__ apps/core/__pycache__
	 docker container restart appdaemon