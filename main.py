# Loading DB modules
import pandas as pd
import numpy as np
import os
pd.set_option('display.max_colwidth', None)
pd.options.mode.chained_assignment = None  # default='warn'

def extract(src_df, start_index, end_index, selected_cols):
    # verify id src_df is not null
    if src_df.empty:
        raise Exception("src_dataframe object is null.")

    # verify that start and end indexes
    if start_index < 0 or end_index < 0:
        raise Exception("start_index or end_index is less than zero")
    if start_index > end_index:
        raise Exception("Index range is not proper")

    # print("Extracting rows= ",start_index," : ", end_index, "Total= ", end_index - start_index)
    # extract data rows in a row object
    data_rows = src_df.iloc[start_index:end_index, selected_cols]
    # print("data_rows in extract")
    # print(data_rows.head())
    # print("----"*20)
    return transform(data_rows)


def transform(rows):

    # data cleaning : removed PHI info, duplicates to be handled later,
    rows.fillna(0, inplace=True)
    rows['glucose_mg/dl_t1'] = rows['glucose_mg/dl_t1'].astype('float64')
    rows.replace(-1, 0, inplace=True)
    rows['cancerPresent'].replace(0, -1, inplace=True)
    rows['cancerPresent'].replace({False : 0, True : 1}, inplace=True)
    rows.loc[rows['glucose_mg/dl_t1'] > 350, "glucose_mg/dl_t1"] = 0
    rows.loc[rows['glucose_mg/dl_t2'] > 350, "glucose_mg/dl_t2"] = 0
    rows.loc[rows['glucose_mg/dl_t3'] > 350, "glucose_mg/dl_t3"] = 0

    # Normalization - unknown range for blood glucose hence not normalizing

    # Adding average glucose level column to the rows
    rows['glucose_average'] = rows[['glucose_mg/dl_t1', 'glucose_mg/dl_t2', 'glucose_mg/dl_t3']].replace(0, np.nan).mean(axis=1, skipna=True)
    rows.loc[rows['glucose_average'] < 140, 'patient_state'] = 'normal'
    rows.loc[rows['glucose_average'] > 200, 'patient_state'] = 'diabeties'
    rows['patient_state'].fillna('prediabeties', inplace=True)
    rows['patient_state']= rows['patient_state'].astype('string')

    print(rows)
    # print("----" * 20)


    return load(rows)


def load(rows):
    try:
        output_path = 'dest.csv'
        rows.to_csv('dest.csv',index=False, mode='a', header=not os.path.exists(output_path))
    except Exception as e:
        print(type(e))
        print(e.args)
        print(e)
        return False

    return True



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

    extract(src_df=src_df, start_index=0, end_index=1000, selected_cols=selected_cols)  # range is inclusive of start and end
