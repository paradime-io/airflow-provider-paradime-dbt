__version__ = "1.0.0"

## This is needed to allow Airflow to pick up specific metadata fields it needs for certain features.
def get_provider_info():
    return {
        "package-name": "airflow-provider-paradime-dbt",  # Required
        "name": "Paradime",  # Required
        "description": "Paradime provider for Apache Airflow.",  # Required
        "connection-types": [
            {
                "connection-type": "paradime",
                "hook-class-name": "paradime_dbt_provider.hooks.paradime.ParadimeHook",
            }
        ],
        "extra-links": [],
        "versions": [__version__],  # Required
    }
