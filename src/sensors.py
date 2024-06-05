from pathlib import Path

from dagster import RunRequest, SensorEvaluationContext, SensorResult, sensor

from src.jobs import overture_places_file_job
from src.partitions import release_partition


@sensor(jobs=[overture_places_file_job])
def release_sensor(context: SensorEvaluationContext):
    releases_file = Path("data/releases.txt")
    if not releases_file.exists():
        raise Exception(f"Releases file {releases_file} does not exist")
    with open(releases_file) as f:
        releases = f.read().splitlines()

    new_releases = [
        release
        for release in releases
        if not release_partition.has_partition_key(
            release, dynamic_partitions_store=context.instance
        )
    ]

    return SensorResult(
        run_requests=[RunRequest(partition_key=release) for release in new_releases],
        dynamic_partitions_requests=[release_partition.build_add_request(new_releases)],
    )
