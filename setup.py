from setuptools import setup

if __name__ == '__main__':
    setup(
        # setuptools_scm
        use_scm_version=True,
        setup_requires=['setuptools_scm'],
        # package metadata
        name='dea-conflux',
        packages=['dea_conflux'],
        install_requires=[
        ],
    )