import dask.dataframe as dd

from pathlib import Path
from loguru import logger


def convert_csv_to_dask_dataframe(filepath: Path) -> dd.DataFrame:
    """
    Load a CSV file as a Dask DataFrame.

    :param filepath: A CSV file path.
    :return: The corresponding Dask DataFrame object.
    """
    assert (
        str(filepath)[-4:] == ".csv"
    ), f"Specified path {str(filepath)} is not in format .csv"
    return dd.read_csv(filepath)


def get_valid_dataframe(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Filters the products of the input DataFrame that contain an image.

    :param ddf: A Dask DataFrame object with an "image" column.
    :return: The corresponding Dask DataFrame object with the products containing an image only.
    """
    assert "image" in list(
        ddf.columns
    ), "The Dask DataFrame provided doesn't have any 'image' column."
    return ddf[ddf["image"].notnull()]


def get_invalid_dataframe(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Filters the products of the input DataFrame that contain no image.

    :param ddf: A Dask DataFrame object with an "image" column.
    :return: The corresponding Dask DataFrame object with the products not containing an image only.
    """
    assert "image" in list(
        ddf.columns
    ), "The Dask DataFrame provided doesn't have any 'image' column."
    return ddf[ddf["image"].notnull()]


def save_dataframe_to_parquet(ddf: dd.DataFrame, output_filepath: Path) -> None:
    """
    Save a Dask DataFrame into a Parquet file.

    :param ddf: A Dask DataFrame object.
    :param output_filepath: The Parquet file path where the Dask DataFrame will be stored.
    """
    assert (
        str(output_filepath)[-8:] == ".parquet"
    ), f"Specified output path {str(output_filepath)} is not in format .parquet"
    assert (
        not output_filepath.exists()
    ), f"The file was not created because the following output filepath already exists : '{output_filepath}'"
    ddf.to_parquet(output_filepath, compression="gzip")
    logger.info(f"File saved successfully at '{output_filepath}'.")
