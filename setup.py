"""
Setup script for NAR Database package
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="nar-database",
    version="0.1.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="Download and process Statistics Canada's National Address Register into a local SQLite database",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/GCOrgName/nar_database",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Database",
        "Topic :: Scientific/Engineering :: GIS",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=22.0.0",
            "flake8>=5.0.0",
            "mypy>=0.991",
        ],
    },
    entry_points={
        "console_scripts": [
            "nar-db=nar_database.cli:main",
        ],
    },
    project_urls={
        "Bug Reports": "https://github.com/GCOrgName/nar_database/issues",
        "Source": "https://github.com/GCOrgName/nar_database",
    },
)
