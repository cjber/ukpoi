from pathlib import Path


class Paths:
    DATA = Path("data")
    RAW = DATA / Path("raw")
    STAGING = DATA / Path("staging")
    OUT = DATA / Path("out")


class Constants:
    BUCKET = "overturemaps-us-west-2"
    PREFIX = "release/"
    DELIMITER = "/theme"
