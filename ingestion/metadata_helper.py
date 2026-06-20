import json
import os
from adls_helper import (
    read_json_from_adls,
    write_json_to_adls
)

METADATA_CONTAINER = "metadata"

METADATA_FILE = "metadata.json"


def read_metadata():
    return read_json_from_adls(
        METADATA_CONTAINER,
        METADATA_FILE
    )


def write_metadata(metadata):
      write_json_to_adls(
        METADATA_CONTAINER,
        METADATA_FILE,
        metadata
    )


def get_table_config(table_name):

    metadata = read_metadata()

    return metadata[table_name]


def update_offset(
    table_name,
    offset
):

    metadata = read_metadata()

    metadata[table_name]["last_offset"] = offset

    write_metadata(metadata)

def update_watermark(
    table_name,
    watermark
):

    metadata = read_metadata()

    metadata[table_name]["last_watermark"] = watermark

    write_metadata(metadata)


def mark_full_load_complete(
    table_name,
    watermark
):

    metadata = read_metadata()

    metadata[table_name]["load_type"] = "incremental"

    metadata[table_name]["full_load_completed"] = True

    metadata[table_name]["last_offset"] = 0

    metadata[table_name]["last_watermark"] = watermark

    write_metadata(metadata)