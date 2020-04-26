from setuptools import setup, find_packages

__version__ = "1.0"
setup(
    name="acme.fraudcop",
    version=__version__,
    python_requires="==3.7.*",
    install_requires=["apache-beam[gcp]==2.20.*"],
    extras_require={"tests": ["black==19.*", "mypy>=0.770", "pytest>=5.4.1"]},
    packages=find_packages("src", exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    package_dir={"": "src"},
    include_package_data=True,
    description="acme fraudcop",
    license="All rights reserved",
    author="seahrh",
    author_email="seahrh@gmail.com",
    url="https://github.com/seahrh/acme.fraudcop",
)
