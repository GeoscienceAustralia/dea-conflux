from setuptools import setup

# All package metadata now lives in pyproject.toml ([project] table).
# This shim is kept only so that legacy `python setup.py` invocations work.
if __name__ == "__main__":
    setup()
