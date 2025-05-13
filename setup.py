from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="arcfs",
    version="0.1.0",
    author="Tim Hosking",
    author_email="github.com/Munger",
    description="A transparent archive file system library for Python",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Munger/arcfs",
    project_urls={
        "Bug Tracker": "https://github.com/Munger/arcfs/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: System :: Filesystems",
        "Topic :: System :: Archiving",
        "Topic :: System :: Archiving :: Compression",
    ],
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.7",
    install_requires=[
        "typing-extensions;python_version<'3.8'",
    ],
)
