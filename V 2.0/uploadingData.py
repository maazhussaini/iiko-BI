from sqlite3 import Cursor
import pandas as pd
import numpy as np
import mysql.connector as sql
import json


def DatabaseConnection():
    conn = sql.connect(host="rawiikodata.cpi0e90jeqda.us-east-2.rds.amazonaws.com",
                            user="admin", password="MySql12$34#", database="rawDataTestV2")

    return conn


conn = DatabaseConnection()
cursor = conn.cursor()

f = open("config.json")
configData = json.load(f)

CurrencyMappingSheet = pd.read_excel(
    configData['SourceFolder2']+"transHeader_Transformed_Fields_v5.1.xlsx", sheet_name="CurrencyMappingSheet")

CurrencyMappingSheet.rename(
    columns={
        "transHeader Value (Region)": "region",
        "CurrencyConvFactor": "factor",
    }, inplace=True
)
CurrencyMappingSheet.fillna('', inplace=True)


CurrencyMappingSheet_df = pd.DataFrame(
    CurrencyMappingSheet)
CurrencyMappingSheet_df_to_list = list(tuple(row)
                                       for row in CurrencyMappingSheet_df.values)


insert_query = """INSERT INTO `rawDataTestV2`.`CurrencyMappingSheet`
                (`region`,
                `factor`)
                VALUES (%s, %s)"""
cursor.executemany(insert_query, CurrencyMappingSheet_df_to_list)
conn.commit()
print("DONE")


# delivery -> DeliveryYN
# virtualStoreNo
# region
