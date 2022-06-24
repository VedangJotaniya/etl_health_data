# Loading DB modules
import pandas as pd
import numpy as np
import concurrent.futures

pd.set_option('display.max_colwidth', None)
pd.options.mode.chained_assignment = None  # default='warn'


def extract(df, start_index, end_index, selected_cols):
    # verify id src_df is not null
    if df is None:
        print("src_dataframe object is null.")
        return False
    # verify that start and end indexes
    if start_index < 0 or end_index < 0:
        print("start_index or end_index is less than zero")
        return False
    if start_index > end_index:
        print("Index range is not proper")
        return False

    # extract data rows in a row object
    data_rows = df.iloc[start_index:end_index, selected_cols]

    return data_rows


def transform(rows):
    try:
        # data cleaning : removed PHI info, duplicates to be handled later,
        rows.fillna(0, inplace=True)
        rows['glucose_mg/dl_t1'] = rows['glucose_mg/dl_t1'].astype('float64')
        rows.replace(-1, 0, inplace=True)
        rows['cancerPresent'].replace(0, -1, inplace=True)
        rows['cancerPresent'].replace({False: 0, True: 1}, inplace=True)
        rows.loc[rows['glucose_mg/dl_t1'] > 350, "glucose_mg/dl_t1"] = 0
        rows.loc[rows['glucose_mg/dl_t2'] > 350, "glucose_mg/dl_t2"] = 0
        rows.loc[rows['glucose_mg/dl_t3'] > 350, "glucose_mg/dl_t3"] = 0

        # Normalization - unknown range for blood glucose hence not normalizing

        # Adding average glucose level column to the rows
        rows['glucose_average'] = rows[['glucose_mg/dl_t1', 'glucose_mg/dl_t2', 'glucose_mg/dl_t3']].replace(0,
                                                                                                             np.nan).mean(
            axis=1, skipna=True)
        rows.loc[rows['glucose_average'] < 140, 'patient_state'] = 'normal'
        rows.loc[rows['glucose_average'] > 200, 'patient_state'] = 'diabeties'
        rows['patient_state'].fillna('prediabeties', inplace=True)
        rows['patient_state'] = rows['patient_state'].astype('string')
    except Exception as e:
        print(type(e))
        print(e.args)
        print(e)
        return False

    return rows


def load(rows):
    try:
        rows.to_csv('dest.csv', index=False, mode='a', header=False)
    except Exception as e:
        print(type(e))
        print(e.args)
        print(e)
        return False

    return True


def etl(src_df, start, end):
    selected_cols = [0, 5, 6, 7, 8, 9]
    try:
        rows = extract(df=src_df, start_index=start, end_index=end,
                       selected_cols=selected_cols)
        if rows is False:
            return False

        rows = transform(rows)

        if rows is False:
            return False

        if load(rows):
            print("successful in etl for indexes ", start, " : ", end)
        else:
            print("Error in etl process")
            return False

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
    src_file_name = "patient_data.csv"
    n_workers = 4

    # Create and load source csv into an object and save the object
    src_df = pd.read_csv(src_file_name,
                         encoding='ISO-8859-1')  # Assuming a standard encoding of ISO-8859-1 due to presence of UnicodeDecodeError

    with open('dest.csv', 'w') as file:  # writing header for csv file
        file.write(
            'patient_id,glucose_mg/dl_t1,glucose_mg/dl_t,glucose_mg/dl_t3,cancerPresent,atrophy_present,glucose_average,patient_state\n')

    starts = range(0, 1000, 250)
    ends = range(250, 1001, 250)

    with concurrent.futures.ThreadPoolExecutor(max_workers=n_workers) as executor:
        executor.map(etl, (src_df, src_df, src_df, src_df), starts, ends)
