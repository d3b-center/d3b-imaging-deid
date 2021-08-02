from os import path

from setuptools import find_packages, setup

root_dir = path.dirname(path.abspath(__file__))

short_description = "D3b Image Deidentification Scripts"

requirements = []
try:
    # requirements from requirements.txt
    with open(path.join(root_dir, "requirements.txt"), "r") as f:
        requirements = f.read().splitlines()
except:
    pass

long_description = short_description
try:
    # long description from README
    with open(path.join(root_dir, "README.md"), encoding="utf-8") as f:
        long_description = f.read()
except:
    pass

setup(
    use_scm_version={
        "local_scheme": "dirty-tag",
        "version_scheme": "post-release",
    },
    setup_requires=["setuptools_scm"],
    name="d3b-imaging-deid",
    description=short_description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/d3b-center/d3b-utils-imaging-deid",
    packages=setuptools.find_packages(),
    python_requires=">=3.6, <4",
    install_requires=requirements,
)
