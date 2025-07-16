import unittest
from unittest.mock import patch, mock_open
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta

from pruebaml import extraer_datos_json, extraer_datos_csv, filtrar_semanas, iterar_datos


class TestFuncionesMain(unittest.TestCase):

    def setUp(self):
        self.json_data = '''{"day":"2023-07-01", "user_id":1, "event_data":{"value_prop":"A"}}\n{"day":"2023-07-02", "user_id":2, "event_data":{"value_prop":"B"}}'''
        self.csv_data = '''pay_date,total,user_id,value_prop
2023-07-01,100.5,1,A
2023-07-02,200.0,2,B
'''

    @patch("builtins.open", new_callable=mock_open)
    @patch("pandas.read_json")
    def test_extraer_datos_json(self, mock_read_json, mock_file):
        mock_df = pd.DataFrame({
            "day": ["2023-07-01"],
            "user_id": [1],
            "event_data": [{"value_prop": "A"}]
        })
        mock_read_json.return_value = mock_df

        df = extraer_datos_json("fake_path.json")

        self.assertIsNotNone(df)
        self.assertIn("value_prop", df.columns)
        self.assertIn("date", df.columns)
        self.assertEqual(df.loc[0, "value_prop"], "A")

    @patch("builtins.open", new_callable=mock_open)
    @patch("pandas.read_csv")
    def test_extraer_datos_csv(self, mock_read_csv, mock_file):
        mock_df = pd.DataFrame({
            "pay_date": ["2023-07-01"],
            "total": ["100.5"],
            "user_id": [1],
            "value_prop": ["A"]
        })
        mock_read_csv.return_value = mock_df

        df = extraer_datos_csv("fake_path.csv")

        self.assertIsNotNone(df)
        self.assertIn("total", df.columns)
        self.assertIsInstance(df["total"].iloc[0], float)

    def test_filtrar_semanas(self):
        today = datetime.today()
        dates = pd.date_range(end=today - timedelta(days=7), periods=30)
        df = pd.DataFrame({
            "date": dates,
            "user_id": [1]*30,
            "value_prop": ["A"]*30
        })
        result = filtrar_semanas(df)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertGreater(len(result), 0)
        self.assertTrue((result["date"] <= today - timedelta(days=7)).all())

    def test_iterar_datos(self):
        base_date = datetime(2023, 7, 1)
        df_prints = pd.DataFrame({
            "date": [base_date],
            "user_id": [1],
            "value_prop": ["A"]
        })
        df_taps = pd.DataFrame({
            "date": [base_date],
            "user_id": [1],
            "value_prop": ["A"]
        })
        df_payments = pd.DataFrame({
            "date": [base_date],
            "user_id": [1],
            "value_prop": ["A"],
            "total": [100.0]
        })

        df_result = iterar_datos(df_prints, df_taps, df_payments)
        self.assertIsInstance(df_result, pd.DataFrame)
        self.assertEqual(df_result.loc[0, "user_id"], 1)
        self.assertEqual(df_result.loc[0, "count_print"], 1)
        self.assertEqual(df_result.loc[0, "count_taps"], 1)
        self.assertEqual(df_result.loc[0, "count_payments"], 1)
        self.assertEqual(df_result.loc[0, "total_payments"], 100.0)


if __name__ == "__main__":
    unittest.main()
