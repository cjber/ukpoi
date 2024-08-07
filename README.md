# UKPOI

Overture Point of Interest (POI) data for the United Kingdom. Automatically queries the latest Overture AWS data using Dagster for ETL orchestration. The project is containerised using Docker (or Podman) Compose for easy deployment and management. Uses DuckDB with the spatial plugin to query only the UK bounding box.

## Requirements

- [Docker](https://www.docker.com) or [Podman](https://podman.io)
- [Docker Compose](https://docs.docker.com/compose/) or [Podman Compose](https://github.com/containers/podman-compose)

## Installation

1. Clone this repository:

   ```bash
   git clone git@github.com:cjber/ingestion-checks.git
   ```

2. Navigate to the project directory:

   ```bash
   cd overture-uk
   ```
## Usage


1. **Run Project**

To run the project, execute the following command:

_**NOTE:** all `docker` commands can be substituted with `podman`_

```bash
docker compose up
```
> _Add `-d` to this command if you would prefer to run it in the background._

This starts the Docker containers for the Dagster Web server, Dagster Daemon, and the code container. Navigate to `localhost:3000` to manage the Dagster orchestration pipeline. To initiate the automation; 'Automation→Schedules' toggle on to find new releases daily, and 'Automation→Sensors' to process new releases when found.


### Delete and Stop all Containers

If you want to stop all containers, execute:

```bash
docker compose down
```

To also remove the containers, execute:

```bash
docker compose down --rmi all  # (does not work with podman)
```
