
import json
import pathlib
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from sqlalchemy import create_engine

def generate_connection_string():
    current_fold = pathlib.Path(__file__).parent.resolve()
    mariadb_json = open(f"{current_fold}/../data/config/mariadb.json", "r").read()
    mariadb = json.loads(mariadb_json)
    username = mariadb["UserName"]
    password = mariadb["Password"]
    hostname = mariadb["HostName"]
    return f"mysql+pymysql://{username}:{password}@{hostname}:3306/housing"

engine = create_engine(generate_connection_string())

def plot_zip_code(metric_name, zip_code):
    data = pd.read_sql_query(f"call proc_view_zip_code('{metric_name}', '{zip_code}');", engine)

    def add_line(colName):
        p = plt.plot(data["PeriodEnd"], data[colName], label = colName)
        dtNum = mdates.date2num(data["PeriodEnd"])
        colFit = np.polyfit(dtNum, data[colName], 1)
        colPoly = np.poly1d(colFit)
        plt.plot(data["PeriodEnd"], colPoly(dtNum), linestyle = "--", color = p[0].get_color())

    add_line("AllTypes")
    add_line("Condo")
    add_line("SingleFamily")
    add_line("Townhouse")

    plt.title(f"{zip_code}: {metric_name}")
    plt.ylim(ymin=0)
    plt.legend()
    plt.show()

zip_codes = ["98052"]

for zip_code in zip_codes:
    plot_zip_code("median_sale_price", zip_code)
