"""Setup configuration for CORE package."""

from setuptools import setup, find_packages

setup(
    name="core-platform",
    version="1.0.0",
    description="CORE platform for insurance rate filing analytics",
    author="Agent Insider",
    author_email="team@agentinsider.com",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "pandas>=1.5.0",
        "duckdb>=0.8.0",
        "pyairtable>=2.0.0",
        "python-dotenv>=0.19.0",
        "tenacity>=8.0.0",
        "jinja2>=3.0.0",
        "click>=8.0.0",
        "tabulate>=0.9.0",
        "dash>=2.0.0",
        "plotly>=5.0.0",
        "postmarker>=0.15.0",
        "requests>=2.25.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "black>=22.0.0",
            "flake8>=4.0.0",
            "mypy>=0.950",
        ]
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    entry_points={
        "console_scripts": [
            "core-cli=core.cli.cli:cli",
            "core-workflow=core.workflows.monthly_workflow:main",
        ],
    },
)
