import pandas as pd
import requests
from gspread_pandas import Spread, Client
from datetime import datetime
from functools import lru_cache


class Config:
    def __init__(self):
        self.URL = "https://covidapi.info/api/v1/"


class GoogleSheet:
    def __init__(self):
        self.__spread__ = Spread("COVID2020")

    def update_sheet(self, country, df):
        self.__spread__.df_to_sheet(df, sheet=country, index=False)


class APIManager:
    def __init__(self):
        self.config = Config()
        self.gspread = GoogleSheet()

    @lru_cache(maxsize=32)
    def request_api(self, endpoint):
        URL = f"{self.config.URL}{endpoint}"
        response = requests.get(URL)
        if response.status_code == 200:
            return response.json()

    def main(self):
        todays_date = datetime.today().strftime("%Y-%m-%d")
        endpoint = f"global/timeseries/2020-01-01/{todays_date}"
        data_raw = self.request_api(endpoint)
        count = data_raw["count"]
        for country, country_data in data_raw["result"].items():
            print(f"Country Name: {country}, Total Data: {len(country_data)}")
            country_data = pd.DataFrame(country_data)
            country_data = self.get_statistics(country_data)
            self.gspread.update_sheet(country, country_data)

        global_data_raw = pd.DataFrame(
            self.request_api("global/count")["result"]
        ).T.reset_index()
        country_data = self.get_statistics(global_data_raw)
        print(country_data)
        self.gspread.update_sheet("GLOBAL", global_data_raw)

    def get_active_cases(self, df):
        df["active_cases"] = df["confirmed"] - (df["deaths"] + df["recovered"])
        return df

    def get_mortality_rate(self, df):
        df["mortality_rate"] = round(df["deaths"].divide(df["confirmed"]), 5)
        df["mortality_rate"] = df["mortality_rate"] * 100
        return df

    def get_recovery_rate(self, df):
        df["recovery_rate"] = round(df["recovered"] / df["confirmed"], 5)
        df["recovery_rate"] = df["recovery_rate"] * 100
        return df

    def get_active_cases_rate(self, df):
        df["active_cases_rate"] = round(df["active_cases"] / df["confirmed"], 5)
        df["active_cases_rate"] = df["active_cases_rate"] * 100
        return df

    def get_statistics(self, df):

        df = self.get_active_cases(df)
        df = self.get_mortality_rate(df)
        df = self.get_recovery_rate(df)
        df = self.get_active_cases_rate(df)

        df["change_in_mortality"] = df["mortality_rate"].diff()

        df["change_in_recovery"] = df["recovery_rate"].diff()

        df["change_in_active_cases"] = df["active_cases_rate"].diff()

        return df


if __name__ == "__main__":
    data = APIManager().main()
