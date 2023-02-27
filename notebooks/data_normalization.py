import numpy as np
import pandas as pd
import subprocess

def dostuff():
    subprocess.run(["ls", "-la"])

def replace_dots_with_underscores_in_df_column_names(df):
    # rename columns without having to duplicate columns
    df.columns = [c.replace(".", "_") for c in list(df.columns)]
    return df


def normalize_column_names(df):
    df = replace_dots_with_underscores_in_df_column_names(df)
    return df


def normalize_conn_log_df(df):
    df = normalize_column_names(df)
    # replace string values
    df = df.replace('-', np.nan)
    df = df.fillna(0)

    # We changed '-' strings from three columns into zeroes.  We expect that these columns no longer contain mixed types.  Therefore, we can change column dtypes from object to relevant numerical dtype.  This should eliminate any 'mixed type' warnings. 
    df['orig_bytes'] = df['orig_bytes'].astype('int64')
    df['resp_bytes'] = df['resp_bytes'].astype('int64')
    df['duration'] = df['duration'].astype('float')
    return df


def normalize_http_log_df(df):
    df = normalize_column_names(df)
    # replace '-' string values
    df = df.replace('-', np.nan)
    df = df.fillna(0)
    # subprocess.run(["pwd"])
    return df


def normalize_zeek_csv_log_df(df, type=None):
    if type==None:
        print("Requires type parameter, i.e., type='conn' or type='http'")    
        raise ValueError
    else:
        df = normalize_column_names(df)
        # replace '-' string values
        df = df.replace('-', np.nan)
        df = df.fillna(0)
        if type.lower == 'conn':
            # We changed '-' strings from three columns in the conn log into zeroes.  We expect that these nominally numerical columns no longer contain mixed types.  Therefore, we can change column dtypes from object to relevant numerical dtype.  This should eliminate any 'mixed type' warnings. 
            df['orig_bytes'] = df['orig_bytes'].astype('int64')
            df['resp_bytes'] = df['resp_bytes'].astype('int64')
            df['duration'] = df['duration'].astype('float')
  
    return df


def test_http_log_normalization():
    http_file_name_csv = "http.16_00_00-17_00_00.csv"
    http_df = pd.read_csv('notebooks/' + http_file_name_csv)
    df = normalize_http_log_df(http_df)
    for col in df.columns:
        assert "." not in col

    # print(df.columns)
    print("ran successfully, apparently.")


if __name__ == "__main__":
    test_http_log_normalization()
