import pandas as pd
import numpy as np
import mysql.connector as sql
import json


def DatabaseConnection():
    conn = sql.connect(host="SERVER_NAME",
                            user="admin", password="PASSWORD", database="database")

    return conn


conn = DatabaseConnection()


f = open("config.json")
configData = json.load(f)

iikoStoreMapping = pd.read_excel(
    configData['SourceFolder2']+"Copy-Transformation_rawHeader.xlsx", sheet_name="iikoStoreMapping")

iikoStoreMapping.rename(
    columns={
        'rawHeaderValue': 'store',
        "transHeaderValuePhysicalStoreNo": "storeNo"
    }, inplace=True
)

iikoBrandMapping = pd.read_excel(
    configData['SourceFolder2']+"Copy-Transformation_rawHeader.xlsx", sheet_name="iikoBrandMapping")

iikoBrandMapping.rename(
    columns={
        'rawHeaderValue': 'conception'
    }, inplace=True
)

iikoVirtualStoreMapping = pd.read_excel(
    configData['SourceFolder2']+"Copy-Transformation_rawHeader.xlsx", sheet_name="iikoVirtualStoreMapping")

iikoVirtualStoreMapping.rename(
    columns={
        'rawHeaderValueDepartment': 'store',
        'rawHeaderValueConception': 'conception'
    }, inplace=True
)

rawHeaderFetchData = pd.read_sql(
    "SELECT pk_rawHeader, store, conception, DATE_FORMAT(accountingDay,'%d/%m/%Y') as accountingDay FROM `rawDataTest`.`rawHeader` WHERE orderDeleted = 'NOT_DELETED' and store != 'Test Lab'", conn)

print(rawHeaderFetchData.shape)

iikoStoreTransformData = pd.merge(
    rawHeaderFetchData, iikoStoreMapping.store, how='inner')

# print(iikoStoreMapping.loc[iikoStoreMapping.store.isin(
#     list(iikoStoreTransformData.store))])

iikoStoreTransformData['PhysicalStoreNo'] = ''

for ind1, val1 in iikoStoreMapping.iterrows():

    print(val1['store'])
    break
    # for ind2, val2 in iikoStoreTransformData.iterrows():


"""
iikoBrandMappingTransformData = pd.merge(
    iikoStoreTransformData, iikoBrandMapping.conception, how='inner')

iikoVirtualStoreMappingTransformData = iikoBrandMappingTransformData[(
    iikoBrandMappingTransformData.store.isin(list(iikoVirtualStoreMapping.store))) & (
    iikoBrandMappingTransformData.conception.isin(list(iikoVirtualStoreMapping.conception)))]


print(iikoVirtualStoreMappingTransformData.dtypes)
"""
