from invoke import task

from tasks.config import SOURCE_PATH, TEST_PATH, SCRIPT_PATH


@task
def flake8(ctx):
    """Runs flake8 linter against codebase."""
    ctx.run(
        "flake8 "
        "--max-line-length 120 "
        "--ignore=E704,E722,E731,F401,F811,W503 "
        "{} {}".format(SOURCE_PATH, SCRIPT_PATH)
    )


@task
def pylint(ctx):
    """Runs pylint linter against codebase."""
    ctx.run("pylint {} {}".format(SOURCE_PATH, SCRIPT_PATH))


@task
def mypy(ctx):
    """Runs mypy linter against codebase."""
    ctx.run("mypy --strict {} {}".format(SOURCE_PATH, SCRIPT_PATH))


@task(flake8, pylint, mypy)
def lint(ctx):
    """Run all linters against codebase."""
    pass


@task
def pytest(ctx):
    """Runs pytest testing framework against codebase."""
    import sys
    sys.path.append(SOURCE_PATH)

    ctx.run(
        "pytest "
        "-Wignore:::pytest_asyncio.plugin:39 "
        "-Wignore:::pytest_asyncio.plugin:45 "
        "--verbose "
        "--color=yes "
        "--durations=10 "
        "--doctest-modules "
        "--cov={source} --cov-report html --cov-report term "
        "{test} {source}".format(source=SOURCE_PATH, test=TEST_PATH)
    )


@task
def doctest(ctx):
    """Runs codebase's doctests."""
    ctx.run(
        "pytest "
        "--verbose "
        "--color=yes "
        "--doctest-modules {}".format(SOURCE_PATH)
    )


@task(pytest)
def test(ctx):
    """Runs all tests against codebase."""
    pass
