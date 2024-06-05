import base64
import geopandas as gpd
import h3
from duckdb.typing import DOUBLE, INTEGER, VARCHAR

import colorcet
import datashader as ds
import duckdb
from dagster import MaterializeResult, MetadataValue, asset
from dagster_aws.s3 import S3Resource
from dagster_duckdb import DuckDBResource

from src.partitions import release_partition
from src.utils import Constants, Paths


@asset
def latest_release(s3: S3Resource):
    """
    Retrieves the list of releases from a specified S3 bucket and writes them to a local file.

    Args:
        s3 (S3Resource): The S3 resource to connect to.
    """
    files = s3.get_client().list_objects_v2(
        Bucket=Constants.BUCKET,
        Prefix=Constants.PREFIX,
        Delimiter=Constants.DELIMITER,
    )
    releases = [file["Prefix"].split("/")[1] for file in files["CommonPrefixes"]]
    with open(Paths.DATA / "releases.txt", "w") as f:
        for release in releases:
            f.write(f"{release}\n")


@asset(deps=[latest_release], partitions_def=release_partition)
def overture_places_table(context, database: DuckDBResource) -> MaterializeResult:
    """
    Creates a table in the DuckDB database with UK places data from a specified S3 bucket, and returns a MaterializeResult containing the number of rows and a preview of the table.

    Args:
        context: The context in which the function is being called.
        database (DuckDBResource): The database resource to connect to.

    Returns:
        MaterializeResult: A result object containing the number of rows and a preview of the table.
    """
    release = context.partition_key
    table_name = f"places_uk_{release.replace('-', '_').split('.')[0]}"

    bounding_box = (
        """
        bbox.xmin > -9.0
        AND bbox.xmax < 2.01
        AND bbox.ymin > 49.75
        AND bbox.ymax < 61.01
    """
        if "beta" in release
        else """
        bbox.minx > -9.0
        AND bbox.maxx < 2.01
        AND bbox.miny > 49.75
        AND bbox.maxy < 61.01
    """
    )

    query_buildings = f"""
    INSTALL httpfs;
    INSTALL spatial;

    LOAD httpfs;
    LOAD spatial;

    CREATE OR REPLACE TABLE {table_name} AS
    (
    SELECT
       id,
       CAST(names AS JSON) AS names,
       CAST(categories AS JSON) AS categories,
       CAST(websites AS JSON) AS websites,
       CAST(socials AS JSON) AS socials,
       CAST(emails AS JSON) AS emails,
       CAST(phones AS JSON) AS phones,
       CAST(brand AS JSON) AS brand,
       CAST(addresses AS JSON) AS addresses,
       CAST(sources AS JSON) AS sources,
       ST_GeomFromWKB(geometry) AS geometry
    FROM
       read_parquet('s3://overturemaps-us-west-2/release/{release}/theme=places/type=*/*', hive_partitioning=1)
    WHERE
        {bounding_box}
    )
    """
    with database.get_connection() as conn:
        conn.execute(query_buildings)

        return MaterializeResult(
            metadata={
                "num_rows": MetadataValue.md(
                    conn.sql(f"SELECT COUNT(*) FROM {table_name}").df().to_markdown()
                ),
                "preview": MetadataValue.md(
                    conn.sql(f"SELECT * FROM {table_name} LIMIT 10").df().to_markdown()
                ),
            }
        )


@asset(deps=[overture_places_table], partitions_def=release_partition)
def overture_places_cleaned(context, database: DuckDBResource) -> MaterializeResult:
    """
    Cleans the 'overture_places' table and creates a new table with selected fields and additional latitude and longitude columns.

    Args:
        context: The context in which the function is being called.
        database (DuckDBResource): The database resource where the 'overture_places' table is stored.

    Returns:
        MaterializeResult: A result object containing metadata about the cleaned table such as number of rows and a preview of the table.
    """
    release = context.partition_key
    table_name = f"places_uk_{release.replace('-', '_').split('.')[0]}"

    names = (
        "names.primary"
        if release
        not in [
            "2023-07-26-alpha.0",
            "2023-10-19-alpha.0",
            "2023-11-14-alpha.0",
            "2023-12-14-alpha.0",
            "2024-01-17-alpha.0",
        ]
        else "names.common[0].value"
    )

    query = f"""
    INSTALL spatial;
    LOAD SPATIAL;

    CREATE OR REPLACE TABLE {table_name}_cleaned AS
    (
        SELECT
            id,
            CAST({names} AS STRING) AS primary_name,
            CAST(categories.main AS STRING) AS main_category,
            list_aggregate(CAST(categories.alternate AS STRING[]), 'string_agg', '|') AS alternate_category,
            CAST(addresses[0].freeform AS STRING) AS address,
            CAST(addresses[0].locality AS STRING) AS locality,
            CAST(addresses[0].postcode AS STRING) AS postcode,
            CAST(addresses[0].region AS STRING) AS region,
            CAST(addresses[0].country AS STRING) AS country,
            CAST(sources[0].dataset AS STRING) AS source,
            CAST(sources[0].record_id AS STRING) AS source_record_id,
            geometry,
            ST_X(geometry) as lat,
            ST_Y(geometry) as long,
        FROM 
            {table_name}
    )
    """

    with database.get_connection() as conn:
        conn.execute(query)

        return MaterializeResult(
            metadata={
                "num_rows": MetadataValue.md(
                    conn.sql(f"SELECT COUNT(*) FROM {table_name}").df().to_markdown()
                ),
                "preview": MetadataValue.md(
                    conn.sql(f"SELECT * FROM {table_name} LIMIT 10").df().to_markdown()
                ),
            }
        )

@asset 
def lsoa2021(database: DuckDBResource):
    with database.get_connection() as conn:
        conn.query(
            """
            INSTALL spatial;
            LOAD spatial;

            CREATE OR REPLACE TABLE lsoa2021 AS
                (
            SELECT *, ST_Transform(geom, 'EPSG:27700', 'EPSG:4326') AS MERC 
            FROM ST_Read('./data/raw/LSOA2021/LSOA_2021_EW_BFC_V8.shp')
            );
            """
    )


@asset
def sgdz2011(database: DuckDBResource):
    with database.get_connection() as conn:
        conn.query(
            """
            INSTALL spatial;
            LOAD spatial;

            CREATE OR REPLACE TABLE sgdz2011 AS
            (
            SELECT *, ST_Transform(geom, 'EPSG:27700', 'EPSG:4326') AS MERC 
            FROM ST_Read('./data/raw/SG_DataZone/SG_DataZone_Bdry_2011.shp')
            );
            """
    )

@asset
def nidz2021(database: DuckDBResource):
    with database.get_connection() as conn:
        conn.query(
            """
            INSTALL spatial;
            LOAD spatial;

            CREATE OR REPLACE TABLE nidz2021 AS
            (
            SELECT *, ST_Transform(geom, 'EPSG:29902', 'EPSG:4326') AS MERC 
            FROM ST_Read('./data/raw/NIDZ2021/DZ2021.shp')
            );
            """
    )


@asset(deps=[overture_places_cleaned, lsoa2021, sgdz2011, nidz2021], partitions_def=release_partition)
def uk_places(context, database: DuckDBResource) -> MaterializeResult:
    """
    Retrieves and processes UK places data from a specified URL, generates a visualization of the data, and returns a MaterializeResult containing the image data and the number of rows in the dataframe.

    Args:
        database (DuckDBResource): The database resource to connect to.

    Returns:
        MaterializeResult: A result object containing the image data and the number of rows in the dataframe.
    """
    release = context.partition_key
    table_name = f"places_uk_{release.replace('-', '_').split('.')[0]}"

    with database.get_connection() as conn:
        conn.create_function("add_h3", h3.geo_to_h3, [DOUBLE, DOUBLE, INTEGER], VARCHAR)
        conn.sql(
        f"""
        INSTALL spatial;
        LOAD spatial;

        COPY (
        SELECT
            places.*,
            lsoa2021.LSOA21NM,
            lsoa2021.LSOA21CD,
            nidz2021.DZ2021_cd AS NI_DZ2021CD,
            sgdz2011.DataZone AS SG_DZ2011CD,
            add_h3(places.lat, places.long, 15) AS h3_15
        FROM
            {table_name}_cleaned AS places
        LEFT JOIN lsoa2021 ON
            ST_Intersects(places.geometry, lsoa2021.MERC)
        LEFT JOIN sgdz2011 ON
            ST_Intersects(places.geometry, sgdz2011.MERC)
        LEFT JOIN nidz2021 ON
            ST_Intersects(places.geometry, nidz2021.MERC)
            ) TO {table_name}.gpkg (FORMAT 'GDAL', DRIVER 'GPKG')
        """
        )

    df = duckdb.read_parquet(str(Paths.OUT / f"{table_name}.parquet"))
    cvs = ds.Canvas(plot_width=1000, plot_height=1000)
    agg = cvs.points(df.df(), "lat", "long")
    img = ds.tf.shade(agg, cmap=colorcet.fire, how="log")
    ds.utils.export_image(img, filename=str(Paths.STAGING / f"lad2023-{table_name}"))  # type: ignore

    image_data = base64.b64encode(
        open(Paths.STAGING / f"lad2023-{table_name}.png", "rb").read()
    )
    return MaterializeResult(
        metadata={
            "preview": MetadataValue.md(
                f"![img](data:image/png;base64,{image_data.decode()})"
            ),
            "num_rows": len(df),
            # "lad_counts": MetadataValue.md(
            #     df["LAD23NM"].value_counts("LAD23NM").df().to_markdown()  # type: ignore
            # ),
        }
    )
