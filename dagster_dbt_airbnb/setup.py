# pylint: disable=C0114

from setuptools import find_packages, setup

setup(
    name="dagster_dbt_airbnb",
    version="0.0.1",
    packages=find_packages(),
    package_data={
        "dagster_dbt_airbnb": [
            "dbt-project/**/*",
        ],
    },
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-dbt",
        "dbt-snowflake<1.10",
        "dbt-snowflake<1.10",
    ],
    extras_require={
        "dev": [
            "dagster-webserver",
        ]
    },
)