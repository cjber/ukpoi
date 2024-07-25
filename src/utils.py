from pathlib import Path


class Paths:
    DATA = Path("data")
    RAW = DATA / "raw"
    STAGING = DATA / "staging"
    OUT = DATA / "out"


class Constants:
    BUCKET = "overturemaps-us-west-2"
    PREFIX = "release/"
    DELIMITER = "/theme"
