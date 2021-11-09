from setuptools import setup

if __name__ == "__main__":
    setup(
        # setuptools_scm
        use_scm_version=True,
        setup_requires=["setuptools_scm"],
        # package metadata
        name="dea-conflux",
        packages=["dea_conflux"],
        python_requires=">=3.6",
        install_requires=[
            "click",
            "pytest",
            "geopandas",
            "datacube",
            "boto3",
            "botocore",
            "pyarrow",
            "fsspec",
            "s3fs",
            "moto[s3]",
            "tqdm",
        ],
        entry_points={
            "console_scripts": ["dea-conflux=dea_conflux.__main__:main"],
        },
    )
