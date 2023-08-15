from setuptools import setup

if __name__ == "__main__":
    setup(
        # setuptools_scm
        use_scm_version=True,
        setup_requires=["setuptools_scm"],
        # package metadata
        name="deafrica-conflux",
        packages=["deafrica_conflux"],
        python_requires=">=3.6",
        install_requires=[
            "click",
            "pytest",
            "coverage",
            "geopandas",
            "datacube",
            "boto3",
            "botocore",
            "pyarrow",
            "fsspec",
            "s3fs",
            "moto[s3]",
            "tqdm",
            "SQLAlchemy",
            # "python-geohash",
        ],
        entry_points={
            "console_scripts": ["deafrica-conflux=deafrica_conflux.cli:main"],
        },
    )
