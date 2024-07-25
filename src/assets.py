import base64

import colorcet
import datashader as ds
import geopandas as gpd
import pandas as pd
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
        if "alpha" not in release
        else """
        bbox.minx > -9.0
        AND bbox.maxx < 2.01
        AND bbox.miny > 49.75
        AND bbox.maxy < 61.01
    """
    )

    query = f"""
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
        conn.sql(query)

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
    categories = (
        "categories.main"
        if any(x in release for x in ["alpha", "beta"])
        else "categories.primary"
    )

    query = f"""
    INSTALL spatial;
    LOAD SPATIAL;

    CREATE OR REPLACE TABLE {table_name}_cleaned AS
    (
        SELECT
            id,
            CAST(trim({names}, '"') AS STRING) AS primary_name,
            CAST(trim({categories}, '"') AS STRING) AS main_category,
            list_aggregate(CAST(categories.alternate AS STRING[]), 'string_agg', '|') AS alternate_category,
            CAST(trim(addresses[0].freeform, '"') AS STRING) AS address,
            CAST(trim(addresses[0].locality, '"') AS STRING) AS locality,
            CAST(trim(addresses[0].postcode, '"') AS STRING) AS postcode,
            CAST(trim(addresses[0].region, '"') AS STRING) AS region,
            CAST(trim(addresses[0].country, '"') AS STRING) AS country,
            CAST(trim(sources[0].dataset, '"') AS STRING) AS source,
            CAST(trim(sources[0].record_id, '"') AS STRING) AS source_record_id,
            geometry,
            ST_Y(geometry) as lat,
            ST_X(geometry) as long,
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
def oa2021():
    return gpd.read_file(Paths.RAW / "OA_2021_BGC.gpkg")[["OA21CD", "geometry"]]


@asset
def oa_lookup2021():
    return pd.read_csv(Paths.RAW / "OA_lookup-2021.csv")[
        ["OA21CD", "LSOA21CD", "MSOA21CD", "LAD22CD"]
    ]


@asset
def sgdz2011():
    return gpd.read_file(Paths.RAW / "SG_DataZone" / "SG_DataZone_Bdry_2011.shp")[
        ["DataZone", "geometry"]
    ].rename(columns={"DataZone": "SG_DZ11CD"})


@asset
def nidz2021():
    return (
        gpd.read_file(Paths.RAW / "NIDZ2021" / "DZ2021.shp")[["DZ2021_cd", "geometry"]]
        .rename(columns={"DZ2021_cd": "NI_DZ21CD"})
        .to_crs("EPSG: 27700")
    )


@asset(deps=[overture_places_cleaned], partitions_def=release_partition)
def uk_places(
    context,
    database: DuckDBResource,
    oa2021: gpd.GeoDataFrame,
    oa_lookup2021: pd.DataFrame,
    sgdz2011: gpd.GeoDataFrame,
    nidz2021: gpd.GeoDataFrame,
) -> MaterializeResult:
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
        df = conn.sql(f"SELECT * FROM {table_name}_cleaned").df()

    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df.long, df.lat), crs="EPSG:4326"
    ).to_crs("EPSG: 27700")
    gdf["easting"], gdf["northing"] = gdf.geometry.x, gdf.geometry.y

    gdf = gpd.sjoin(gdf, oa2021, how="left").drop(columns=["index_right"])
    gdf = gdf.merge(oa_lookup2021, on="OA21CD", how="left")
    gdf = gpd.sjoin(gdf, sgdz2011, how="left").drop(columns=["index_right"])
    gdf = gpd.sjoin(gdf, nidz2021, how="left").drop(columns=["index_right"])

    gdf.to_parquet(Paths.OUT / f"{table_name}.parquet", index=False)

    cvs = ds.Canvas(plot_width=1000, plot_height=1000)
    agg = cvs.points(df, "long", "lat")
    img = ds.tf.shade(agg, cmap=colorcet.fire, how="log")
    ds.utils.export_image(img, filename=str(Paths.STAGING / f"{table_name}"))

    image_data = base64.b64encode(
        open(Paths.STAGING / f"{table_name}.png", "rb").read()
    )
    return MaterializeResult(
        metadata={
            "preview": MetadataValue.md(
                f"![img](data:image/png;base64,{image_data.decode()})"
            ),
            "num_rows": len(df),
        }
    )
