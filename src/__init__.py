from dagster import Definitions, ScheduleDefinition, load_assets_from_modules

from src import assets
from src.jobs import latest_release_job, overture_places_file_job
from src.resources import database_resource, s3_resource
from src.sensors import release_sensor

release_schedule = ScheduleDefinition(job=latest_release_job, cron_schedule="0 0 * * *")
asset_defs = load_assets_from_modules([assets])

defs = Definitions(
    assets=[*asset_defs],
    jobs=[latest_release_job, overture_places_file_job],
    schedules=[release_schedule],
    sensors=[release_sensor],
    resources={"database": database_resource, "s3": s3_resource},
)
