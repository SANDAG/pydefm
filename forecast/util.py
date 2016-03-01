"""
Utilities used within the Demographic and Economic Forecasting Model
Includes pandas DataFrame reshaping (pivot) and parsing yaml file

"""
import pandas as pd
import yaml


def apply_pivot(df):
    """
    Pivot the migration rates DataFrame such that rates for each of the 4 mig
    rate types are in separate columns instead of in a single rate column.

    Parameters
    ----------
    df : pandas DataFrame with migration rates for each of the four migration
        rate types in a single column

    Returns
    -------
    df : pandas DataFrame with rate values in 4 columns corresponding to 4
        rate types (Domestic In, Domestic Out, Foreign In, Foreign Out)

    """

    df = df.reset_index(drop=False)
    df = pd.pivot_table(df,
                              values='rate',
                              index=['age','race_ethn','sex','yr'],
                              columns=['migration'])
    df = df.reset_index(level=3,drop=False) # rm yr from index, keep as column
    return df


def yaml_to_dict(yaml_file, yaml_section):
    """
    Load YAML from a file
    Read specific section to dictionary

    Parameters
    ----------
    yaml_file : File name from which to load YAML
    yaml_section : Section of YAML file to process

    Returns
    -------
    dict
        Conversion from YAML for a specific section.

    """

    with open(yaml_file,'r') as f:
            d = yaml.load(f)[yaml_section]

    return d
