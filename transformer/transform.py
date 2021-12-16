import dask.dataframe as dd
import pandas as pd
import argparse

from pathlib import Path
from loguru import logger
from timer import timeit
from typing import Union


def convert_csv_to_dataframe(filepath: Path, library: str = "pandas") -> dd.DataFrame:
    """
    Load a CSV file as a DataFrame.

    :param filepath: A CSV file path.
    :param library: Library to use : only "pandas" and "dask" are accepted.
    :return: The corresponding DataFrame object.
    """
    assert (
        str(filepath)[-4:] == ".csv"
    ), f"Specified path {str(filepath)} is not in format .csv"
    assert (
            (library == "pandas") or (library == "dask")
    ), f"Specified library {library} not valid : should be 'pandas' or 'dask'"
    if library == "dask":
        return dd.read_csv(filepath)
    else:
        return pd.read_csv(filepath)


def get_valid_dataframe(df: Union[pd.DataFrame, dd.DataFrame]) -> dd.DataFrame:
    """
    Filters the products of the input DataFrame that contain an image.

    :param df: A DataFrame object with an "image" column.
    :return: The corresponding DataFrame object with the products containing an image only.
    """
    assert "image" in list(
        df.columns
    ), "The DataFrame provided doesn't have any 'image' column."
    return df[df["image"].notnull()]


def get_invalid_dataframe(df: Union[pd.DataFrame, dd.DataFrame]) -> dd.DataFrame:
    """
    Filters the products of the input DataFrame that contain no image.

    :param df: A DataFrame object with an "image" column.
    :return: The corresponding DataFrame object with the products not containing an image only.
    """
    assert "image" in list(
        df.columns
    ), "The DataFrame provided doesn't have any 'image' column."
    return df[df["image"].notnull()]


def save_dataframe_to_parquet(
    df: Union[pd.DataFrame, dd.DataFrame],
    output_filepath: Path,
) -> None:
    """
    Save a DataFrame into a Parquet file.

    :param df: A DataFrame object.
    :param output_filepath: The Parquet file path where the DataFrame will be stored.
    """
    assert (
        str(output_filepath)[-8:] == ".parquet"
    ), f"Specified output path {str(output_filepath)} is not in format .parquet"
    assert (
        not output_filepath.exists()
    ), f"The file was not created because the following output filepath already exists : '{output_filepath}'"
    df.to_parquet(output_filepath)
    logger.info(f"File saved successfully at '{output_filepath}'.")


@timeit
def main(
    csv_input_path: str,
    valid_parquet_output_path: str,
    invalid_parquet_output_path: str,
    library: str = "pandas",
) -> None:
    """
    Split the CSV input files into two Parquet files :
        - one containing products with an image provided
        - one containing products with no image provided

    :param csv_input_path: The product catalog CSV file path.
    :param valid_parquet_output_path: The Parquet file path where the products containing an image will be stored.
    :param invalid_parquet_output_path: The Parquet file path where the products containing no image will be stored.
    :param library: Library to use : only "pandas" and "dask" are accepted.
    :return:
    """
    assert (
        csv_input_path[-4:] == ".csv"
    ), f"Specified path {csv_input_path} is not in format .csv"
    assert (
        valid_parquet_output_path[-8:] == ".parquet"
    ), f"Specified path {valid_parquet_output_path} is not in format .parquet"
    assert (
        invalid_parquet_output_path[-8:] == ".parquet"
    ), f"Specified path {invalid_parquet_output_path} is not in format .parquet"

    ddf = convert_csv_to_dataframe(filepath=Path(csv_input_path), library=library)

    valid_ddf = get_valid_dataframe(ddf)
    invalid_ddf = get_invalid_dataframe(ddf)

    save_dataframe_to_parquet(
        df=valid_ddf, output_filepath=Path(valid_parquet_output_path)
    )
    save_dataframe_to_parquet(
        df=invalid_ddf, output_filepath=Path(invalid_parquet_output_path),
    )


if __name__ == "__main__":
    # Parser setup
    parser = argparse.ArgumentParser(
        description="CSV to Parquet converter, partitioning the outputs into a file containing products with image and the other one products without image"
    )
    parser.add_argument("csv_input_path", help="Product catalog CSV file path.")
    parser.add_argument(
        "valid_parquet_output_path", help="The corresponding valid Parquet file path"
    )
    parser.add_argument(
        "invalid_parquet_output_path",
        help="The corresponding invalid Parquet file path",
    )
    parser.add_argument(
        "--library", default="pandas", help="Which library to use : can be pandas or dask"
    )
    args = parser.parse_args()

    # Call main function
    main(
        csv_input_path=args.csv_input_path,
        valid_parquet_output_path=args.valid_parquet_output_path,
        invalid_parquet_output_path=args.invalid_parquet_output_path,
        library=args.library
    )
