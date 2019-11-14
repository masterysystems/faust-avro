[![Maintainability](https://api.codeclimate.com/v1/badges/13676d472164ce8b3699/maintainability)](https://codeclimate.com/github/masterysystems/faust-avro/maintainability)

[![Test Coverage](https://api.codeclimate.com/v1/badges/13676d472164ce8b3699/test_coverage)](https://codeclimate.com/github/masterysystems/faust-avro/test_coverage)

# Faust Avro

Avro codec and schema registry support for Faust.

## Getting Started

### First Time Setup

* Get [asdf](https://asdf-vm.com/#/core-manage-asdf-vm?id=install-asdf-vm)
* Install python: `asdf install python`
* Get [poetry](https://poetry.eustace.io/docs/)
  * `curl -sSL https://raw.githubusercontent.com/sdispater/poetry/master/get-poetry.py | python`
  * As with any code asking you to do this from the internet, you should really first download get-poetry.py and inspect it to make sure it doesn't do anything bad, then run the copy you inspected.
  * Or you can just `pip install poetry` to stick it inside the main python install by asdf. If you switch python versions and get errors about reshimming poetry, that version of python doesn't have poetry installed.

### Install needed packages

`poetry install`

### Activate environment

Most directions here assume you have the poetry-created virtualenv active. Some form of `source .venv/bin/activate` (or `source .venv/bin/activate.(csh|fish)`) should activate the environment for you, or you can configure your shell to auto-activate, eg:

```
$ cat ~/.config/fish/functions/cd.fish
#!/bin/env fish

function cd -d "change directory, and activate virtualenvs, if available"
    # first and foremost, change directory
    builtin cd $argv

    # find a parent git directory
    if git rev-parse --show-toplevel >/dev/null ^/dev/null
        set gitdir (realpath (git rev-parse --show-toplevel))
    else
        set gitdir ""
    end

    # if that directory contains a virtualenv in a ".venv" directory, activate it
    if test \( -z "$VIRTUAL_ENV" -o "$VIRTUAL_ENV" != "$gitdir/.venv" \) -a -f "$gitdir/.venv/bin/activate.fish"
        source $gitdir/.venv/bin/activate.fish
    end

    # deactivate an active virtualenv if not int a git directory with an ".venv"
    if test -n "$VIRTUAL_ENV" -a "$VIRTUAL_ENV" != "$gitdir/.venv"
        deactivate
    end
end
```

### Add pre-commit hooks

Install git pre-commit hooks via `pre-commit install`. These will auto-run black and isort to ensure your code is pretty no matter what editor you use. This must be done inside an activated environment.

### Running tests

`pytest`
