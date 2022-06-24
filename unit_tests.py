import unittest
from main import *
import numpy as np


class TestExtractMethod(unittest.TestCase):
    def test_verify_src_not_empty(self):
        self.assertEqual(extract(None, 0, 10, [10, 2, 3, 4]), False)

    def test_verify_size(self):
        src_file_name = "patient_data.csv"
        src_df = pd.read_csv(src_file_name, encoding='ISO-8859-1')
        self.assertEqual(extract(src_df,0,5, [0, 5, 6, 7, 8, 9]).shape, (5, 6))

class TestTransformMethod(unittest.TestCase):
    def test_verify_size(self):
        src_file_name = "patient_data.csv"
        src_df = pd.read_csv(src_file_name, encoding='ISO-8859-1')
        self.assertEqual(transform(extract(src_df,0,5, [0, 5, 6, 7, 8, 9])).shape, (5, 8))

    def test_no_na_values_present(self):
        src_file_name = "patient_data.csv"
        src_df = pd.read_csv(src_file_name, encoding='ISO-8859-1')
        df = transform(extract(src_df, 0, 5, [0, 5, 6, 7, 8, 9]))
        self.assertEqual(len(df['glucose_mg/dl_t1'].index[df['glucose_mg/dl_t1'].apply(np.isnan)]), 0)
        self.assertEqual(len(df['glucose_mg/dl_t2'].index[df['glucose_mg/dl_t2'].apply(np.isnan)]), 0)
        self.assertEqual(len(df['glucose_mg/dl_t3'].index[df['glucose_mg/dl_t3'].apply(np.isnan)]), 0)
        self.assertEqual(len(df['glucose_average'].index[df['glucose_average'].apply(np.isnan)]), 0)


if __name__ == '__main__':
    unittest.main()