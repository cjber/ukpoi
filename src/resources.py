from dagster import EnvVar
from dagster_aws.s3 import S3Resource
from dagster_duckdb import DuckDBResource

database_resource = DuckDBResource(database=EnvVar("DUCKDB_DATABASE"))
s3_resource = S3Resource(region_name="us-west-2", use_unsigned_session=True)
