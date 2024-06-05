from dagster import define_asset_job

from src.assets import (
    latest_release,
    overture_places_cleaned,
    overture_places_table,
    uk_places,
)

latest_release_job = define_asset_job("latest_release_job", selection=[latest_release])
overture_places_file_job = define_asset_job(
    "overture_places_file_job",
    selection=[overture_places_table, overture_places_cleaned, uk_places],
)
