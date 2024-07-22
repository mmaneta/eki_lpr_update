import unittest
from unittest.mock import MagicMock, patch

import os
import pandas as pd

from lrp_update import query_openet


class TestOpenetApi(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.open_et = query_openet.OpenetApi.from_file('data', 'data/test_api_key.txt')
        cls.vars = ("ET",
                    "2018-01-01",
                    "2023-09-30",
                    "monthly",
                    "ensemble",
                    "mean",
                    "cimis",
                    "in",
                    "EKIfld",
                    "projects/ee-csheppner/assets/Year1_enrolled_nonrepurposed",
                    "multipolygon",
                    "JSON")

    def test_query_openet(self):
        open_et_obj = query_openet.OpenetApi('data', '12349834pnrvqnp32uewrjvn2p2039ruvn')
        assert open_et_obj.api_key == '12349834pnrvqnp32uewrjvn2p2039ruvn'

    def test_query_openet_fromfile(self):
        open_et_obj = query_openet.OpenetApi.from_file('data', 'data/test_api_key.txt')
        assert open_et_obj.api_key == '12349834pnrvqnp32uewrjvn2p2039ruvn'

    def test_build_query(self):
        out = self.open_et._build_query(*("ET",
                    "2018-01-01",
                    "2023-09-30",
                    "monthly",
                    "ensemble",
                    "mean",
                    "cimis",
                    "in",
                    "EKIfld",
                    "projects/ee-csheppner/assets/Year1_enrolled_nonrepurposed",
                    "JSON"))

        self.assertEqual(out[0]["Authorization"], self.open_et.api_key)
        self.assertEqual(out[1]["attributes"], "EKIfld")

    @patch("pandas.DataFrame.to_csv") #prevents the creation of file
    @patch("requests.post")
    def test_update_local_dataset_empty_local_database(self, mock_post, mock_to_csv):
        vars = ("pr",
         "2018-01-01",
         "2023-09-30",
         "monthly",
         "ensemble",
         "mean",
         "cimis",
         "in",
         "EKIfld",
         "projects/ee-csheppner/assets/Year1_enrolled_nonrepurposed",
         "multipolygon",
         "JSON")

        response = MagicMock(status_code=200)
        response.json.return_value = {'url': 'data/Yr1_nonrepurp_ET.csv'}
        mock_post.return_value = response

        obj_api = query_openet.OpenetApi('data', 'cKF3SgsGxmGERTbqtuoUFh8gu35FcVAtGpaAtnjfmV32EHl2Jur05wT9BPWz')
        obj_api.update_local_dataset(*vars)
        assert isinstance(obj_api.df_data, pd.DataFrame)

    # @patch("pandas.DataFrame.to_parquet") #prevents the creation of file
    # @patch("requests.post")
    # def test_update_local_dataset_request_within_range(self, mock_post, pd_toparquet):
    #
    #     response = MagicMock(status_code=200)
    #     response.json.return_value = {'url': 'data/Yr1_nonrepurp_ET.csv'}
    #     mock_post.return_value = response
    #
    #     obj_api = query_openet.OpenetApi('data', 'cKF3SgsGxmGERTbqtuoUFh8gu35FcVAtGpaAtnjfmV32EHl2Jur05wT9BPWz')
    #     obj_api.update_local_dataset(*self.vars)
    #     assert isinstance(obj_api.df_data, pd.DataFrame)


class TestCalculateWaterBalance(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.report = query_openet.CalculateWaterBalance(
            fn_pp='data/Year1_enrolled_repurposed_pr.csv',
            fn_et='data/Year1_enrolled_repurposed_ET.csv',
            fn_fld_key='data/EKIfld_IDs_key.csv',
            end_date="2023-09-30",
        )

    def test_constructor_bad_name_fn_pp(self):
        with self.assertRaises(ValueError):
            query_openet.CalculateWaterBalance(fn_pp='not_existent.csv',
            fn_et='data/Year1_enrolled_repurposed_ET.csv',
            fn_fld_key='data/EKIfld_IDs_key.csv',
            end_date="2023-09-30")


    def test_constructor_file_not_found(self):
        with self.assertRaises(FileNotFoundError):
            query_openet.CalculateWaterBalance(fn_pp='Year1_nexistent_repurposed_ET.csv',
            fn_et='data/Year1_enrolled_repurposed_ET.csv',
            fn_fld_key='data/EKIfld_IDs_key.csv',
            end_date="2023-09-30")

    def test_run_consumptive_use_calcs_single_user(self):
        self.report.calculate_consumptive_use(concat_appl_id="00001")

    def test_run_consumptive_use_calcs_all_users(self):
        self.report.calculate_consumptive_use()

    def test_run_consumptive_save_to_file_one_user(self):
        self.report.calculate_consumptive_use(concat_appl_id="00001").save_consumptive_use_to_csv('data')

    def test_run_consumptive_save_to_file_all_users(self):
        self.report.calculate_consumptive_use().save_consumptive_use_to_csv('data')


class TestCalculateLrpReport(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        fn_pdf = 'data/WY2023_Q1_qtrly_report_00001_Maradani.pdf'
        cls.report = query_openet.GenerateLrpReport.from_pdf_template(fn_pdf)

    def test_constructor_from_pdf(self):
        fn_pdf = 'data/WY2023_Q1_qtrly_report_00001_Maradani.pdf'
        self.report = query_openet.GenerateLrpReport.from_pdf_template(fn_pdf)
        assert self.report.lrp_participant_name is not None

    def test_generate_lrp_report(self):
        self.report.generate_lrp_report('data/Year1_enrolled_repurposed_pr.csv',
                                        'data/Year1_enrolled_repurposed_ET.csv',
                                        'data/EKIfld_IDs_key.csv',
                                        2024,
                                        "Q4",
                                        "test.pdf"
                                        )


# class TestPdf(unittest.TestCase):
#     def test_pdf(self):
#         pdf = query_openet.Pdf()
#         df = pd.read_parquet('./data/test_df.pq').reset_index()
#         pdf.print_page(df=df, quarter='Q4', water_year=2023, **{"LRPAgreementNumber": "0001",
#                                                          "LRPParticipantName": "Syam Maradani",
#                                                          "AreaofLandRepurposed": 480.63,
#                                                          "MinimumWaterUseReduction": 939.69,
#                                                          "BaselineWaterUse": 1326.25,
#                                                          "MaximumConsumptiveUse": 383.56},
#                        fn_pdf_report_out="test.pdf")

    def test_table(self):
        pdf = query_openet.Pdf()
        df = pd.read_parquet('./data/test_df.pq')

        table = pdf._table(df.reset_index())







if __name__ == '__main__':
    unittest.main()
