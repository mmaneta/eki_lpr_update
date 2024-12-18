"""
Author: Marco Maneta
email: mmaneta@ekiconsult.com
"""
import datetime
import os

import fpdf
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pypdf
import requests
from PIL import Image
from fpdf import FPDF
from fpdf.fonts import CoreFont
# Import Chris Heppner's SMB functions
from lrp_update.smb_for_LRP import calc_SMB_for_time_series as smb
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas

INCHES_TO_FEET = 1. / 12.
FEET_TO_INCHES = 12.


class OpenetApi:
    """Manages connections with the OpenET
    server and the retrieval of precipitation and ET datasets.
     """

    def __init__(self,
                 path_dataset: str,
                 api_key: str):
        """
        Initializes the OpenetApi instance.

        Args:
               path_dataset (str): Path to the folder with the dataset.
               api_key (str): API key for authentication.
        """
        self.api_key = api_key
        self.path_dataset = path_dataset
        self.df_data = None

    @classmethod
    def from_file(cls,
                  path_dataset: str,  # path to master csv datasets
                  fn_key: str):
        """
        Creates an instance of OpenetApi from a file containing the API key.

        Args:
            path_datasets (str): Path to the folder with master datasets.
            fn_key (str): Path to the file containing the API key.

        Returns:
            OpenetApi: An instance of the OpenetApi class.
        """

        with open(fn_key) as f:
            key = f.read()

        return cls(path_dataset, key)

    def update_local_dataset(self,
                             variable,
                             start_date,
                             end_date,
                             interval,
                             model,
                             reducer,
                             reference_et,
                             units,
                             attributes,
                             asset_id=None,
                             query_type="multipolygon",
                             file_format="JSON",
                             ):
        """
        Updates the local dataset with new data from the OpenET server.

        Args:
            variable (str): The variable to update.
            start_date (str): The start date for the data retrieval.
            end_date (str): The end date for the data retrieval.
            interval (str): The interval for the data retrieval.
            model (str): The model to use for data retrieval.
            reducer (str): The reducer to use for data retrieval.
            reference_et (str): The reference ET to use.
            units (str): The units for the data.
            attributes (list): The attributes to retrieve.
            asset_id (str, optional): The asset ID. Defaults to None.
            query_type (str, optional): The type of query. Defaults to "multipolygon".
            file_format (str, optional): The file format for the data. Defaults to "JSON".

        Raises:
            Exception: If the query type is not supported.
        """
        # e.g. Year1_enrolled_nonrepurposed
        year, enrolled, repurposed = asset_id.split('/')[-1].split('_')

        match query_type:
            case "multipolygon":
                url = "https://openet-api.org/raster/timeseries/multipolygon"
                fn_ds = os.path.join(self.path_dataset, asset_id.split('/')[-1] + f"_{variable}.csv")
            case _:
                raise Exception(f"query type {query_type} not supported")

        df_dataset = None
        try:
            df_dataset = pd.read_csv(fn_ds)
            df_dataset['time'] = pd.to_datetime(df_dataset['time'])
        except FileNotFoundError as e:
            pass

        # if pd_dataset exists, check if the requested dates are already there
        if df_dataset is not None:
            is_within_range = df_dataset['time'].min().asm8 <= pd.to_datetime(
                start_date) and df_dataset['time'].max().asm8 >= pd.to_datetime(end_date)
            if is_within_range:
                print(f"Requested data range {start_date}:{end_date} already in {fn_ds}")
                return
            else:
                print(f"Requested data for period {start_date}:{end_date} extends "
                      f"beyond data available locally for variable {variable}")

        print(f"Requesting new available data for {variable}...")

        header, args = self._build_query(variable,
                                         start_date,
                                         end_date,
                                         interval,
                                         model,
                                         reducer,
                                         reference_et,
                                         units,
                                         attributes,
                                         asset_id,
                                         file_format,
                                         )

        resp = requests.post(
            headers=header,
            json=args,
            url=url
        )
        if resp.status_code != 200:
            raise Exception(resp.text)

        print(f"Request Successful. Retrieving data")
        r = resp.json()

        self.df_data = pd.read_csv(r['url'])
        self.df_data['time'] = pd.to_datetime(self.df_data['time'])

        if df_dataset is not None:
            df_dataset = pd.concat([df_dataset, self.df_data], ignore_index=True).drop_duplicates(subset=['time', 'EKIfld'],
                                                                                                  keep='first')
        else:
            df_dataset = self.df_data

        df_dataset.to_csv(fn_ds, index=False)

        return df_dataset

    def _build_query(self,
                     variable,
                     start_date,
                     end_date,
                     interval,
                     model,
                     reducer,
                     reference_et,
                     units,
                     attributes,
                     asset_id,
                     file_format,
                     ):

        if not isinstance(attributes, list):
            attributes = [attributes]

        header = {"Authorization": self.api_key}
        dct_query = {
            "variable": variable,
            "date_range": [start_date, end_date],
            "interval": interval,
            "model": model,
            "reducer": reducer,
            "reference_et": reference_et,
            "units": units,
            "attributes": attributes,
            "asset_id": asset_id,
            "file_format": file_format
        }
        return header, dct_query


class _ConsumptiveUse:
    """Internal Class"""

    def __init__(self, df_smb, year, end_date, repurposed):
        self.df_smb = df_smb
        self.year = year
        self.end_date = end_date
        self.repurposed = repurposed

    def save_consumptive_use_to_csv(self, path_database):
        for concat_appl_id, df in self.df_smb.groupby(level=0):
            df.to_csv(os.path.join(path_database, concat_appl_id + '_' + self.year + '_' + self.repurposed + ".csv"))


class CalculateWaterBalance:
    def __init__(self,
                 fn_pp: str,
                 fn_et: str,
                 fn_fld_key: str,
                 end_date: str):
        """
        Calculates the water balance for selected land parcels.
        Args:
            fn_pp (str): Path to the csv precipitation file.
            fn_et (str): Path to the csv ET file.
            fn_fld_key (str): Path to the fld key file.
            end_date (str): The end date for the data retrieval.
        """

        end_date = pd.to_datetime(end_date)
        try:
            year, _, repurposed, _ = os.path.split(fn_pp)[1].split('_')
        except ValueError as e:
            print(f"File name {fn_pp} is not valid. It needs to be in the format of `Year_enrolled_repurposed_var.csv`")
            print(f"where `Year [Year1, Year2]`, `repurposed ['repurposed, nonrepurposed']`, `var [pr, ET]`")
            raise

        self.lpr_year = year
        if self.lpr_year != os.path.split(fn_et)[1].split('_')[0]:
            print(f"years in {fn_pp} and {fn_et} do not match")
            raise Exception(f"years in {fn_pp} and {fn_et} do not match")

        if repurposed != os.path.split(fn_et)[1].split('_')[2]:
            print(f"repurposed in {fn_pp} and {fn_et} do not match")
            raise Exception(f"repurposed in {fn_pp} and {fn_et} do not match")

        self.eki_fld_id_keys = pd.read_csv(fn_fld_key)
        try:
            self.df_et = pd.read_csv(fn_et)
        except FileNotFoundError as e:
            print(f"File {fn_et} not found.")
            raise

        try:
            self.df_pp = pd.read_csv(fn_pp)
        except FileNotFoundError as e:
            print(f"File {fn_pp} not found.")
            raise

        self.df_et['time'] = pd.to_datetime(self.df_et['time'])
        self.df_pp['time'] = pd.to_datetime(self.df_pp['time'])

        self.df_et = self.df_et[self.df_et['time'] <= end_date]
        self.df_pp = self.df_pp[self.df_pp['time'] <= end_date]

        # Filter keys
        is_repurposed = 'Y' if repurposed == 'repurposed' else 'N'
        self.eki_fld_id_keys = self.eki_fld_id_keys[(self.eki_fld_id_keys['LRP_Yr'] == 'Yr' + str(year[-1])) & (
                self.eki_fld_id_keys['Repurp'] == is_repurposed)]

        self.year = year
        self.repurposed = repurposed
        self.end_date = end_date

    def calculate_consumptive_use(self,
                                  concat_appl_id: str = None,
                                  ):
        """Consumptive use for parcel with id `concat_appl_id`
        Args:
            concat_appl_id (str): The concat_appl_id for the parcel.

        Returns:
            An object of type _ConsumptiveUse that with method to save results to a csv file
        """
        if concat_appl_id is None:
            self.df_smb = self.eki_fld_id_keys.groupby('concat_appl_ID').apply(self._run_consumptive_use_calcs)
        else:
            fld_keys = self.eki_fld_id_keys[self.eki_fld_id_keys['concat_appl_ID'] == concat_appl_id]
            self.df_smb = self._run_consumptive_use_calcs(fld_keys)
            self.df_smb.index = pd.MultiIndex.from_product([[concat_appl_id], self.df_smb.index],
                                                           names=['concat_appl_id', 'time'])

        return _ConsumptiveUse(self.df_smb, self.year, self.end_date, self.repurposed)

    def _run_consumptive_use_calcs(self, fld_keys):
        df_et = self.df_et[self.df_et['EKIfld'].isin(fld_keys['EKIfld'])]
        if df_et.size == 0:
            msg = f"The evapotranspiration dataset does not contain information for fields with ids {fld_keys['EKIfld']}"
            print(msg)
            raise Exception(msg)
        df_et_sum = df_et.groupby('time').sum()
        df_et_av = df_et_sum['acre-feet'] * FEET_TO_INCHES / df_et_sum['acres']

        df_pp = self.df_pp[self.df_pp['EKIfld'].isin(fld_keys['EKIfld'])]
        if df_pp.size == 0:
            msg = f"The precipitation dataset does not contain information for fields with ids {fld_keys['EKIfld']}"
            print(msg)
            raise Exception(msg)

        df_pp_sum = df_pp.groupby('time').sum()
        df_pp_av = df_pp_sum['acre-feet'] * FEET_TO_INCHES / df_pp_sum['acres']

        print(f"calculating consumptive use for {fld_keys['EKIfld']}")
        df_smb = pd.DataFrame(
            smb(df_pp_av.to_numpy(), df_et_av.to_numpy()),
            index=["ss", "ppt_eff", "runoff", "cons_use_ss", "cons_use_AW", "cons_use_ppt"]
        ).T
        df_smb["pp_wght_av"] = df_pp_av.to_numpy()
        df_smb["et_wght_av"] = df_et_av.to_numpy()
        df_smb.index = df_pp_av.index

        return df_smb


class GenerateLrpReport:
    """Handles reading and writing pdf's for report"""

    def __init__(self, lrp_agreement_number,
                 lrp_participant_name,
                 area_of_land_repurposed,
                 minimum_water_use_reduction,
                 baseline_water_use,
                 maximum_consumptive_use):
        """
        Args:
            lrp_agreement_number (str): The LRP agreement number for the report
            lrp_participant_name (str): The participant name for the report
            area_of_land_repurposed (str): The area of land repurposed (ac)
            minimum_water_use_reduction (str): The minimum water usage reduction (ac-ft per year)
            baseline_water_use (str): The baseline water use (ac-ft per year)
            maximum_consumptive_use (str): The maximum consumptive use (ac-ft per year)
        """

        self.lrp_participant_name = lrp_participant_name
        self.lrp_agreement_number = lrp_agreement_number
        self.area_of_land_repurposed = area_of_land_repurposed
        self.minimum_water_use_reduction = minimum_water_use_reduction
        self.baseline_water_user = baseline_water_use
        self.maximum_consumptive_use = maximum_consumptive_use

        self.pdf = Pdf()

    @classmethod
    def from_pdf_template(cls, fn_pdf_template):
        """Initializes the object from information extracted from an
        older pdf report
        args:
        fn_pdf_template (str): The pdf report to be used as template
        """
        pdf = pypdf.PdfReader(fn_pdf_template)
        dct_info = cls._parse_pdf_contents(pdf)

        info = [dct_info["LRPAgreementNumber"],
                dct_info["LRPParticipantName"],
                dct_info["AreaofLandRepurposed"],
                dct_info["MinimumWaterUseReduction"],
                dct_info["BaselineWaterUse"],
                dct_info['MaximumConsumptiveUse']]

        print("Creating report with the following information")
        for key, value in dct_info.items():
            print(f"{key} {value}")

        return cls(*info)

    @staticmethod
    def _parse_pdf_contents(pdf):

        pages = [p for p in pdf.pages]
        key_info = {}

        for p in pages:
            lines = p.extract_text().splitlines()
            for line in lines:

                try:
                    key, value = line.split(':')
                except ValueError:
                    key = value = "default"
                key2 = ''.join(c for c in key if c.isalnum())
                if key2 in ["LRPAgreementNumber",
                            "LRPParticipantName",
                            "AreaofLandRepurposed",
                            "MinimumWaterUseReduction",
                            "BaselineWaterUse",
                            "MaximumConsumptiveUse"]:
                    key_info[key2] = value

            return key_info

    def generate_lrp_report(self,
                            fn_pp,
                            fn_et,
                            fn_fld_key,
                            water_year,
                            quarter,
                            fn_report_out
                            ):
        """
        Calculates water consumption and generates the LRP report

        Args:
            fn_pp (str): path to the precipitation file from open et
            fn_et (str): path to the evapotranspiration file from open et
            fn_fld_key (str): path to the files with fld keys
            water_year (int): water year of interest for the report
            quarter (str): quarter of interest for the report
            fn_report_out (str): filename of the pdf report with results
        """

        year = water_year
        match quarter:
            case "Q1":
                month, day = 12, 31
                year = year - 1
            case "Q2":
                month, day = 3, 31
            case "Q3":
                month, day = 6, 30
            case "Q4":
                month, day = 9, 30
            case _:
                print(f"quarter needs to be on of Q1, Q2, Q3, or Q4, not {quarter}")
                return

        end_date = datetime.date(year, month, day)
        # wy = datetime.strptime(water_year, "%Y")

        app_id = self.lrp_agreement_number.strip()
        obj_smb = CalculateWaterBalance(fn_pp,
                                        fn_et,
                                        fn_fld_key,
                                        end_date.strftime("%m-%d-%Y"),
                                        ).calculate_consumptive_use(app_id)

        obj_smb.df_smb['water_year'] = (
            obj_smb.df_smb.index.levels[1].year.where(obj_smb.df_smb.index.levels[1].month < 10,
                                                      obj_smb.df_smb.index.levels[1].year + 1))
        df_wy = obj_smb.df_smb[obj_smb.df_smb['water_year'] == water_year]
        df_wy.loc[:, ['Q']] = pd.cut(df_wy.index.get_level_values(1).month,
                                     bins=[0, 3, 6, 9, 12],
                                     labels=["Q2", "Q3", "Q4", "Q1"],
                                     right=True)

        df_sum = df_wy.groupby('Q', observed=True).sum().reindex(["Q1", "Q2", "Q3", "Q4"]).fillna(0)
        df_sum["cons_use_AW_af"] = df_sum["cons_use_AW"] * INCHES_TO_FEET * float(
            self.area_of_land_repurposed.split()[0])
        df_sum["total_cons_use_AW_af"] = df_sum["cons_use_AW_af"].cumsum()
        fig = self._plot(obj_smb.df_smb)

        self.pdf.print_page(fn_pdf_report_out=fn_report_out,
                            df=df_sum.reset_index(),
                            fig=fig,
                            quarter=quarter,
                            water_year=water_year,
                            **{"LRPAgreementNumber": self.lrp_agreement_number,
                               "LRPParticipantName": self.lrp_participant_name,
                               "AreaofLandRepurposed": self.area_of_land_repurposed.split()[0],
                               "MinimumWaterUseReduction": self.minimum_water_use_reduction.split()[0],
                               "BaselineWaterUse": self.baseline_water_user.split()[0],
                               "MaximumConsumptiveUse": self.maximum_consumptive_use.split()[0]})

        return obj_smb

    @staticmethod
    def _plot(df, fn_out=None):
        font = {
            'weight': 'bold',
            'size': 16}

        plt.rc('font', **font)

        fig = plt.figure(figsize=(10, 6), dpi=300)
        plt.plot(df.index.levels[1], df["et_wght_av"], label="OpenET", marker='o')
        plt.plot(df.index.levels[1], df["pp_wght_av"], label="Precipitation", marker='o')
        plt.plot(df.index.levels[1], df["ppt_eff"], label="Effective Precipitation", marker='o')
        plt.plot(df.index.levels[1], df["cons_use_AW"], label="Consumptive Use of Applied Water", marker='o')
        plt.ylabel("Amount (inches)")
        plt.xlabel("Month/Year")
        plt.grid(True)
        plt.legend(bbox_to_anchor=(0.5, -0.5), ncols=2, loc='lower center', borderaxespad=0.)
        plt.title("ET, Consumptive Use, Precipitation, and Effective Precipitation\n on Repurposed Fields")
        plt.tight_layout()

        return fig


class Pdf(FPDF):
    def __init__(self):
        super().__init__(format='Letter', orientation='L')
        self.add_page()

    def header(self):
        pass
        # self.image('assets/logo.png', 10, 8, 33)
        # self.set_font('Arial', 'B', 11)
        # self.cell(self.WIDTH - 80)
        # self.ln(20)

    def footer(self):
        # Page numbers in the footer
        self.set_y(-15)
        self.set_font('Helvetica', 'I', 8)
        self.set_text_color(128)
        self.cell(0, 10, 'Page ' + str(self.page_no()), 0)

    def page_body(self,
                  df,
                  fig,
                  quarter,
                  water_year,
                  LRPAgreementNumber,
                  LRPParticipantName,
                  AreaofLandRepurposed,
                  MinimumWaterUseReduction,
                  BaselineWaterUse,
                  MaximumConsumptiveUse,
                  user_comments=''):
        self.set_font('Helvetica', 'B', 11.64)
        self.multi_cell(0, 5, 'QUARTERLY CONSUMPTIVE WATER USE STATEMENT\n PHASE 1 LAND REPURPOSING PROGRAM (LRP)',
                        align='C', )
        self.ln(10)
        self.set_font('Helvetica', '', 9.96)
        self.cell(0, 5, f'LRP Agreement Number: \t\t\t\t {LRPAgreementNumber}', align='L')
        self.ln(5)
        self.cell(0, 5, f'LRP Participant Name: \t\t\t\t {LRPParticipantName}', align='L')
        self.ln(5)
        self.cell(0, 5, f'Reporting Period:', align='L')
        self.ln(5)
        self.cell(0, 5, f'\t\t Quarter: \t\t {quarter}', align='L')
        self.ln(5)
        self.cell(0, 5, f'\t\t Water Year: \t\t {water_year}', align='L')
        self.ln(5)
        self.cell(0, 5, f'Area of Land Repurposed: \t\t\t\t {AreaofLandRepurposed} acres', align='L')
        self.ln(5)
        self.cell(0, 5, f'Minimum Water Use Reduction: \t\t\t\t {MinimumWaterUseReduction} AFY', align='L')
        self.ln(5)
        self.cell(0, 5, f'Baseline Water Use: \t\t\t\t {BaselineWaterUse} AFY', align='L')
        self.ln(5)
        self.cell(0, 5, f'Maximum Consumptive Use: \t\t\t\t {MaximumConsumptiveUse} AFY', align='L')
        self.ln(6)
        self._table(df, water_year)
        self.set_font('Helvetica', '', 7.5)
        self.ln(2)
        self.cell(0, 5, f'in=inches;   AF=acre-feet;    AFY=acre-feet per year', align='L')
        self.ln(3)
        self.cell(0, 5, f'{user_comments}', align='L')
        self.set_font('Helvetica', 'B', 9.12)
        self.ln(3)
        self.cell(0, 5, 'Based on the Minimum Water Use Reduction under the above LRP Agreement'
                        'and the Water Year Cumulative Consumptive Water Use shown in the table above: ', align='L')
        self.ln(10)

        is_compliant = False
        if df["total_cons_use_AW_af"].max() <= float(MaximumConsumptiveUse.split()[0]):
            is_compliant = True

        self.set_font('Helvetica', '', 9.12)
        self.set_fill_color(255, 255, 255)
        # self.set_fallback_fonts(["dingbats"])
        self.set_line_width(0.1)
        if is_compliant:
            self.set_fill_color(0, 0, 0)
            self.rect(self.get_x() + 10, self.get_y(), 3, -3, 'DF')
            self.text(self.get_x() + 20, self.get_y() - 2,
                      "This LRP Agreement is in compliance with the Minimum Water Use Reduction requirement")
            self.ln(4)
            self.set_fill_color(255, 255, 255)
            self.rect(self.get_x() + 10, self.get_y(), 3, -3, 'DF')
            self.text(self.get_x() + 20, self.get_y() - 2,
                      "This LRP Agreement is NOT in compliance with the Minimum Water Use Reduction requirement")
        else:

            self.rect(self.get_x() + 10, self.get_y(), 3, -3, 'DF')
            self.text(self.get_x() + 20, self.get_y(),
                      "This LRP Agreement is in compliance with the Minimum Water Use Reduction requirement")
            self.ln(4)
            self.set_fill_color(0, 0, 0)
            self.rect(self.get_x() + 10, self.get_y(), 3, -3, 'DF')
            self.text(self.get_x() + 20, self.get_y(),
                      "This LRP Agreement is NOT in compliance with the Minimum Water Use Reduction requirement")

        self.add_page()

        canvas = FigureCanvas(fig)
        canvas.draw()
        img = Image.fromarray(np.asarray(canvas.buffer_rgba()))
        self.image(img, w=self.epw * 0.8, x=fpdf.Align.C)

    def print_page(self, fn_pdf_report_out, **info):
        self.page_body(**info)
        self.output(fn_pdf_report_out)

    def _table(self, df, wy=2023):
        self.set_font('Helvetica', '', 11.64)
        headings_style = CoreFont(emphasis="ITALICS", fill_color=(128, 128, 128))
        with self.table(text_align="CENTER", headings_style=headings_style, num_heading_rows=2) as table:
            row = table.row()
            row.cell("Reporting Period", align='C', colspan=2)
            row.cell("OpenEt (in)", align='C', rowspan=2)
            row.cell("Precipitation (in)", align='C', rowspan=2)
            row.cell("Effective Precipitation (in)", align='C', rowspan=2)
            row.cell("Applied surface Water (in)", align='C', rowspan=2)
            row.cell("Consumptive Groundwater Use (in)", align='C', rowspan=2)
            row.cell("Consumptive Groundwater Use (AF)", align='C', rowspan=2)
            row.cell("Water Year Total Consumptive Groundwater Use (AF)", align='C', rowspan=2)
            row = table.row()
            row.cell("Quarter", align='C')
            row.cell("Months, Year", align='C')

            for months, (i, df_row) in zip([f"Oct-Dec, {wy - 1}", f"Jan-Mar, {wy}", f"Apr-Jun, {wy}", f"Jul-Sep, {wy}"],
                                           df.iterrows()):
                row = table.row()
                row.cell(df_row["Q"], align='C')
                row.cell(f"{months}", align='C')
                row.cell("{:.2f}".format(df_row["et_wght_av"]))
                row.cell("{:.2f}".format(df_row["pp_wght_av"]))
                row.cell("{:.2f}".format(df_row["cons_use_ppt"]))
                #row.cell("{:.2f}".format(df_row["cons_use_AW"]))
                row.cell("{:.2f}".format(0))  # because applied surface water is zero
                row.cell("{:.2f}".format(df_row["cons_use_AW"]))
                row.cell("{:.2f}".format(df_row["cons_use_AW_af"]))
                row.cell("{:.2f}".format(df_row["total_cons_use_AW_af"]))
            row = table.row()
            self.set_font('Helvetica', 'B', 11.64)
            row.cell("Water Year Total", colspan=2)
            row.cell("{:.2f}".format(df["et_wght_av"].sum()))
            row.cell("{:.2f}".format(df["pp_wght_av"].sum()))
            row.cell("{:.2f}".format(df["cons_use_ppt"].sum()))
            row.cell("{:.2f}".format(0))
            row.cell("{:.2f}".format(df["cons_use_AW"].sum()))
            row.cell("{:.2f}".format(df["cons_use_AW_af"].sum()))
            row.cell("{:.2f}".format(df["cons_use_AW_af"].sum()))

            return table
