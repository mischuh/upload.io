from invoke import Collection

from tasks.docker import docker
from tasks.linting import (
    flake8,
    pylint,
    mypy,
    lint,
    doctest,
    test
)

ns = Collection()
ns.add_task(flake8)
ns.add_task(pylint)
ns.add_task(mypy)
ns.add_task(lint)
ns.add_task(doctest)
ns.add_task(test)

ns.add_collection(docker, 'docker')
