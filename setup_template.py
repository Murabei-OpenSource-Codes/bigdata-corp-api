"""setup."""
import os
import re
from setuptools import setup, find_packages


def parse_requirements(filename):
    """Load requirements from a requirements.txt file."""
    with open(filename) as f:
        lines = f.read().splitlines()

    requirements = []
    for line in lines:
        # Skip comments and empty lines
        if line.startswith('#') or not line.strip():
            continue

        # Remove whitespace and any trailing comments
        line = re.sub(r'\s*#.*$', '', line).strip()
        if line:  # Add if not empty after cleanup
            requirements.append(line)

    return requirements


# Read README
with open(os.path.join(os.path.dirname(__file__), 'README.md'),
          encoding='utf-8') as f:
    README = f.read()

# Parse requirements.txt
requirements_path = os.path.join(os.path.dirname(__file__), 'requirements.txt')
requirements = parse_requirements(requirements_path)

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name='bigdata-corp-api',
    version='{VERSION}',
    include_package_data=True,
    license='BSD-3-Clause License',
    description=(
        'Python API to facilitate interaction with BigDataCorp end-points'),
    long_description=README,
    long_description_content_type="text/markdown",
    url='https://github.com/Murabei-OpenSource-Codes/bigdata-corp-api',
    author='Murabei Data Science',
    author_email='a.baceti@murabei.com',
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    install_requires=requirements,
    packages=find_packages(where="src"),
    python_requires=">=3.6",
)
