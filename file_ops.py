import csv
import glob
import os
import pandas as pd

import log as log

logger = log.setup_custom_logger(__name__)


def get_absolute_path(**kwargs):
    """
    Returns the absolute path based on the current working directory
    and keyword arguments passed in.

    Args:

    Returns:
        str
    """

    cwd = os.getcwd()
    return os.path.join(cwd, *kwargs.values())


def get_file_paths(directory, filter_pattern):
    """
    Create a list of filepaths for files matching specified filter_pattern.

    Args:
        directory (str): Full path of directory containing files to match.
        filter_pattern (str): Regex filter_pattern to match files.

    Returns:
        list
    """

    for root, dirs, files in os.walk(directory):
        file_paths = glob.glob(os.path.join(root, filter_pattern))
        logger.info(f'Sub directories found: {len(dirs)}')
        logger.info(f'Files found: {len(files)}')

        return file_paths


def create_csv_from_dataframe(df, out_file, header=True):
    """
    Create a .csv file from a pandas DataFrame in a specified filepath.
    If the destination file exists, it will be deleted before the new
    file is written.

    Args:
        df (pandas.DataFrame): The pandas DataFrame to write to .csv file.
        out_file (str): Full path of the destination .csv file.

    Returns:
        None
    """

    df_row_count = len(df)

    if os.path.isfile(out_file):
        os.remove(out_file)

    try:
        df.to_csv(
            out_file,
            header=header,
            index=False,
            mode='a',
            quoting=csv.QUOTE_ALL,
            encoding='utf8',
        )
    except Exception as e:
        logger.info('Error: Unable to write DataFrame to .csv!')
        raise e

    with open(out_file, mode='r', encoding='utf8', newline='') as f:
        csv_reader = csv.reader(f)
        # subtract 1 to exclude header
        csv_row_count = sum(1 for line in csv_reader) - 1

        assert df_row_count == csv_row_count, (
            f"""
            Dataframe rows: {df_row_count} | .csv rows: {csv_row_count}
            Error: Dataframe and .csv row counts do not match!
            """
        )

    logger.info(f'Output .csv rows: {csv_row_count}')


def create_dataframe_from_csv(file_paths):
    """
    Create a pandas DataFrame from a list of filepaths. Ensure the data
    files supplied all have the same table schema.

    Args:
        file_paths (list): List of full paths to source data files.

    Returns:
        pandas.DataFrame
    """

    csv_opts = {
        'header': 0,
        'skip_blank_lines': True,
        'encoding': 'utf8',
    }

    dataframe_generator = (
        pd.read_csv(csv_file, **csv_opts) for csv_file in file_paths
    )

    try:
        df = pd.concat(dataframe_generator, ignore_index=True)
        logger.info(f'Input .csv rows: {len(df)}')
    except Exception as e:
        logger.info('Error: Could not create dataframe from .csv files!')
        raise e

    return df


def create_partitioned_csvs_from_dataframe(df,
                                           output_path,
                                           filename,
                                           time_dimension):

    cols = df.columns

    df['year'] = pd.DatetimeIndex(df[time_dimension]).year
    df['month'] = pd.DatetimeIndex(df[time_dimension]).month

    for year in set(df['year']):

        for month in set(df['month']):

            outfile = f'{output_path}/{filename}_{year}_{month:02}.csv'

            df.loc[(df['year'] == year) & (df['month'] == month)].to_csv(
                outfile,
                index=False,
                columns=cols,
                encoding='utf-8',
                quoting=csv.QUOTE_ALL,
            )

            logger.info(f'Partitioned CSV written: {outfile}')


def main(input_path,
         filter_pattern,
         time_dimension,
         output_path,
         filename):

    # get directory paths
    input_path = get_absolute_path(dir=input_path)
    output_path = get_absolute_path(dir=output_path)

    # create a list of the csv file paths
    file_paths = get_file_paths(
        directory=input_path,
        filter_pattern=f'{filter_pattern}*.csv',
    )
    for f in file_paths:
        logger.info(f)

    # concatenate all csv files into a single dataframe
    df = create_dataframe_from_csv(file_paths)

    # create partitioned csv files from dataframe
    create_partitioned_csvs_from_dataframe(
        df=df,
        output_path=output_path,
        filename=filename,
        time_dimension=time_dimension,
    )


if __name__ == '__main__':

    main(
        input_path='local_path_to_source_files',
        filter_pattern='fact_transactions',
        time_dimension='transaction_date',
        output_path='exports/fact_transactions',
        filename='fact_transactions',
    )
