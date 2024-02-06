__version__ = "1.0.5"


## This is needed to allow Airflow to pick up specific metadata fields it needs for certain features.
def get_provider_info():
    return {
        "package-name": "airflow-provider-paradime-dbt",  # Required
        "name": "Paradime",  # Required
        "description": "The Airflow provider for Paradime to run and manage dbt™ jobs in production. The provider enables interaction with Paradime's Bolt scheduler and management APIs.",  # Required
        "connection-types": [
            {
                "connection-type": "paradime",
                "hook-class-name": "paradime_dbt_provider.hooks.paradime.ParadimeHook",
            }
        ],
        "extra-links": [],
        "versions": [__version__],  # Required
    }
