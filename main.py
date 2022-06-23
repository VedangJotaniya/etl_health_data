# Loading DB modules
import pandas as pd
import os


def extract(src_df, start_index, end_index, selected_cols):
    # verify id src_df is not null
    if src_df.empty:
        raise Exception("src_dataframe object is null.")

    # verify that start and end indexes
    if start_index < 0 or end_index < 0:
        raise Exception("start_index or end_index is less than zero")
    if start_index > end_index:
        raise Exception("Index range is not proper")

    print("Extracting rows= ",start_index," : ", end_index, "Total= ", end_index - start_index)
    # extract data rows in a row object
    data_rows = src_df.iloc[start_index:end_index, selected_cols]
    print("data_rows in extract")
    print(data_rows.head())
    print("----"*20)
    return transform(data_rows)


def transform(rows):

    # data cleaning
    # remove invalid values
    # Normalization
    print(rows.head())
    print("----" * 20)
    return


def load():
    return


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('Bombardier Aero')

    # Assuming that source csv file is in current directory
    src_dir = "."
    src_file_name = "patient_data.csv"
    selected_cols = [0, 5, 6, 7, 8, 9]
    # Create and load source csv into an object and save the object
    print(os.path.join(src_dir, src_file_name))
    src_df = pd.read_csv(src_file_name,
                         encoding='ISO-8859-1')  # Assuming a standard encoding of ISO-8859-1 due to presence of UnicodeDecodeError

    extract(src_df=src_df, start_index=0, end_index=2, selected_cols=selected_cols)  # range is inclusive of start and end
