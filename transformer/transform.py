import dask.dataframe as dd
import argparse

from pathlib import Path
from loguru import logger
from timer import timeit


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


@timeit
def main(
    csv_input_path: str,
    valid_parquet_output_path: str,
    invalid_parquet_output_path: str,
) -> None:
    """
    Split the CSV input files into two Parquet files :
        - one containing products with an image provided
        - one containing products with no image provided

    :param csv_input_path: The product catalog CSV file path.
    :param valid_parquet_output_path: The Parquet file path where the products containing an image will be stored.
    :param invalid_parquet_output_path: The Parquet file path where the products containing no image will be stored.
    :return:
    """
    # todo : compress the invalid parquet file
    assert (
        csv_input_path[-4:] == ".csv"
    ), f"Specified path {csv_input_path} is not in format .csv"
    assert (
        valid_parquet_output_path[-8:] == ".parquet"
    ), f"Specified path {valid_parquet_output_path} is not in format .parquet"
    assert (
        invalid_parquet_output_path[-8:] == ".parquet"
    ), f"Specified path {invalid_parquet_output_path} is not in format .parquet"

    ddf = convert_csv_to_dask_dataframe(filepath=Path(csv_input_path))

    valid_ddf = get_valid_dataframe(ddf)
    invalid_ddf = get_invalid_dataframe(ddf)

    save_dataframe_to_parquet(
        ddf=valid_ddf, output_filepath=Path(valid_parquet_output_path)
    )
    save_dataframe_to_parquet(
        ddf=invalid_ddf, output_filepath=Path(invalid_parquet_output_path)
    )
    