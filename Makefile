.PHONY: clean-pyc clean-build clean lint test doctest version

VERSION=0.0.1
ABS_DIR=`pwd`
SOURCE_PATH=./uploadio
TEST_PATH=./test

help:
		@echo "    clean-pyc"
		@echo "        Remove python artifacts."
		@echo "    clean-build"
		@echo "        Remove build artifacts."
		@echo "    lint"
		@echo "        Check style with flake8."
		@echo "    test"
		@echo "        Run py.test"
		@echo "    doctest"
		@echo "        Run doctest"
		@echo "    rollback"
		@echo "        Rolls back any changes (use for bad version bumps)"
		@echo "    version"
		@echo "        Prints out the current version"

clean-pyc:
		find . -name '*.pyc' -delete
		find . -name '*.pyo' -delete
		find . -name '__pycache__' -delete
		
clean-build:
		rm --force --recursive build/
		rm --force --recursive dist/
		rm --force --recursive *.egg-info

clean: clean-pyc

lint:
		flake8 --exclude=.tox --max-line-length 120 --ignore=E722 $(SOURCE_PATH)

test:
		export PYTHONPATH=$(ABS_DIR) && \
		pytest --verbose --color=yes -s \
			--doctest-modules \
			--cov=$(SOURCE_PATH) --cov-report html --cov-report term $(TEST_PATH) \
			$(SOURCE_PATH)

doctest:
		pytest --verbose --color=yes --doctest-modules $(SOURCE_PATH)

docker:
		docker build -t uploadio:$(VERSION) -f Dockerfile .

release:
		$(eval NEXT_VERSION := $(shell bumpversion --dry-run --allow-dirty --list patch | grep new_version | sed s,"^.*=",,))
		@echo $(NEXT_VERSION)

rollback:
		git reset --hard HEAD~1                        # rollback the commit
		git tag -d `git describe --tags --abbrev=0`    # delete the tag

pin:
		./pip_requirements.sh

update:
		pip install -Ur dev-requirements.txt && pip install -Ur requirements.txt

version:
		@echo $(VERSION)