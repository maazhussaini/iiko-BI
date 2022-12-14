from ast import While
from distutils.command.config import config
from lib2to3.pgen2 import token
import requests
import json
import pandas as pd
import numpy as np
import time
from datetime import datetime
from datetime import date, timedelta
import logging
import os
import mysql.connector as sql

from prefect import flow, task, Task, Flow
from prefect_dask import DaskTaskRunner
from prefect.task_runners import SequentialTaskRunner

current_date_time_obj = datetime.now().today()
current_date_time_str = str(
    current_date_time_obj.strftime('%Y-%m-%d %H-%M-%S'))
logging.basicConfig(filename="Logs/log-"+current_date_time_str+".txt")

"""
RawHeader
"""

f = open("config.json")
configData = json.load(f)

conn = sql.connect(host=configData['dbConnection']['host'],
                   user=configData['dbConnection']['user'],
                   password=configData['dbConnection']['password'],
                   database=configData['dbConnection']['dbName'])


token = None


@task(name="Authenticate",
      description="Authenticate Api.",
      tags=["GENERAL", "Authenticate"])
def Authenticate():

    global token
    if token is not None:
        main_url = "https://yo-sushi-gourmet-gulf-co.iikoweb.co.uk"
        url = main_url+"/api/stores/list"
        payload = {}
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer '+str(token)
        }
        response = requests.request(
            "POST", url, headers=headers, data=payload)

        response = json.loads(response.text)
        if response['error'] == True:
            main_url = "https://yo-sushi-gourmet-gulf-co.iikoweb.co.uk"
            url = main_url+"/api/auth/login"
            payload = "{\n    \"login\": \"navision\",\n    \"password\": \"75KuJSiyXWrzJU1i\"\n}"
            headers = {}
            s = requests.Session()
            response = requests.request(
                "POST", url, headers=headers, data=payload)

            response = json.loads(response.text)
            if response['error'] == False:
                return response['token']
        else:
            return token
    else:
        main_url = "https://yo-sushi-gourmet-gulf-co.iikoweb.co.uk"
        url = main_url+"/api/auth/login"
        payload = "{\n    \"login\": \"navision\",\n    \"password\": \"75KuJSiyXWrzJU1i\"\n}"
        headers = {}
        s = requests.Session()
        response = requests.request(
            "POST", url, headers=headers, data=payload)

        response = json.loads(response.text)
        if response['error'] == False:
            return response['token']


"""
Raw Header
"""


class RawHeader:

    @task(name="SelectStore",
          description="RawHeader SelectStore.",
          tags=["RawHeader", "selectStore"])
    def RawHeader_selectStore(token, store):
        try:
            url = "https://yo-sushi-gourmet-gulf-co.iikoweb.co.uk/api/stores/select/"+store
            payload = {}
            headers = {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer '+str(token)
            }
            response = requests.request(
                "POST", url, headers=headers, data=payload)

            return response
        except Exception as err:
            logging.error('Error - '+str(err))

    @task(name="ExportRawHeader",
          description=" RawHeader ExportRawHeader.",
          tags=["RawHeader", "ExportRawHeader"])
    def RawHeader_ExportRawHeader(token, selectStoreCookie, dateFrom, dateTo):
        try:
            url = "https://yo-sushi-gourmet-gulf-co.iikoweb.co.uk/api/olap/init"

            payload = {
                "olapType": "SALES",
                "categoryFields": [],
                "groupFields": [
                    "UniqOrderId.Id",
                    "Department",
                    "Conception",
                    "OpenDate.Typed",
                    "OrderNum",
                    "TableNum",
                    "SessionNum",
                    "OrderDeleted",
                    "Delivery.IsDelivery",
                    "Delivery.ServiceType",
                    "RestaurantSection",
                    "Delivery.SourceKey",
                    "OrderType",
                    "ExternalNumber",
                    "Delivery.CustomerPhone",
                    "Delivery.CustomerName",
                    "Delivery.DeliveryComment",
                    "OrderWaiter.Name",
                    "OpenTime",
                    "Delivery.PrintTime",
                    "Delivery.ExpectedTime",
                    "Delivery.SendTime",
                    "Delivery.ActualTime",
                    "Delivery.CloseTime",
                    "PrechequeTime",
                    "Delivery.BillTime",
                    "CloseTime",
                    "Delivery.CancelCause",
                    "Delivery.CancelComment",
                    "PriceCategory"
                ],
                "stackByDataFields": False,
                "dataFields": [
                    "DishSumInt",
                    "DiscountSum",
                    "IncreaseSum",
                    "DishDiscountSumInt",
                    "VAT.Sum",
                    "DishDiscountSumInt.withoutVAT",
                    "GuestNum",
                    "DishAmountInt",
                    "ItemSaleEventDiscountType.DiscountAmount",
                    "ItemSaleEventDiscountType.ComboAmount",
                    "DishDiscountSumInt.averageByGuest",
                    "DishServicePrintTime.Max",
                    "Cooking.CookingDuration.Avg",
                    "Cooking.Cooking1Duration.Avg",
                    "Cooking.Cooking2Duration.Avg",
                    "Cooking.Cooking3Duration.Avg",
                    "Cooking.Cooking4Duration.Avg",
                    "Cooking.CookingLateTime.Avg",
                    "Cooking.ServeTime.Avg",
                    "Cooking.GuestWaitTime.Avg",
                    "Cooking.FeedLateTime.Avg"
                ],
                "calculatedFields": [
                ],
                "filters": [
                    {
                        "field": "OpenDate.Typed",
                        "filterType": "date_range",
                        "dateFrom": dateFrom,
                        "dateTo": dateTo,
                        "valueMin": None,
                        "valueMax": None,
                        "valueList": [],
                        "includeLeft": True,
                        "includeRight": False,
                        "inclusiveList": True
                    }

                ],
                "includeVoidTransactions": False,
                "includeNonBusinessPaymentTypes": True
            }
            headers = {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer '+str(token),
            }
            response = requests.request(
                "POST", url, headers=headers, data=json.dumps(payload), cookies=selectStoreCookie)

            return json.loads(response.text)
        except Exception as err:
            logging.error(
                'olap_ExportPayment - Error - ' + str(datetime.now().today()) + ' - ' + str(err))

    def RawHeader_FetchStatus(self, token, dataId):
        global count
        try:
            url = "https://yo-sushi-gourmet-gulf-co.iikoweb.co.uk/api/olap/fetch-status/" + dataId

            payload = {}
            headers = {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer '+str(token),
            }
            response = requests.request(
                "GET", url, headers=headers, data=payload)
            response = json.loads(response.text)
            print(response, dataId)
            if response['data'] == 'SUCCESS':
                return response
            else:
                del response
                time.sleep(30)

                if count == 4:
                    return {"data": "Fail"}

                count += 1
                return self.RawHeader_FetchStatus(token, dataId)

        except Exception as err:
            logging.error(
                'Error - ' + str(datetime.now().today()) + ' - ' + str(err))

    @task(name="FetchData",
          description="RawHeader FetchData.",
          tags=["RawHeader", "FetchData"])
    def RawHeader_FetchData(token, dataId, status):
        if dataId['error']:
            logging.error(
                'FetchData(RawHeader_ExportRawHeader) - Error - ' + str(datetime.now().today()) + ' | ' + str(dataId))

        if status['error']:
            logging.error(
                'FetchData(RawHeader_FetchStatus) - Error - ' + str(datetime.now().today()) + ' | ' + str(status))

        if status['data'] == 'SUCCESS':
            url = "https://yo-sushi-gourmet-gulf-co.iikoweb.co.uk/api/olap/fetch/" + \
                dataId['data']+"/json"

            payload = "{\n    \"olapType\": \"SALES\",\n    \"categoryFields\": [],\n    \"groupFields\": [\n        \"UniqOrderId.Id\",\n        \"Department\",\n        \"Conception\",\n        \"OpenDate.Typed\",\n        \"OrderNum\",\n        \"Delivery.IsDelivery\",\n        \"OrderType\",\n        \"DishCategory.Accounting\",\n        \"DishCode\",\n        \"DishName\",\n        \"ItemSaleEventDiscountType\"\n    ],\n    \"stackByDataFields\": false,\n    \"dataFields\": [\n        \"DishSumInt\",\n        \"IncreaseSum\",\n        \"DiscountSum\",\n        \"DishDiscountSumInt\",\n        \"VAT.Sum\"\n    ],\n    \"calculatedFields\": [],\n    \"filters\": [\n        {\n            \"field\": \"OpenDate.Typed\",\n            \"filterType\": \"date_range\",\n            \"dateFrom\": \"2022-06-20T00:00:00\",\n            \"dateTo\": \"2022-06-21T00:00:00\",\n            \"valueMin\": null,\n            \"valueMax\": null,\n            \"valueList\": [],\n            \"includeLeft\": true,\n            \"includeRight\": false,\n            \"inclusiveList\": true\n        }\n    ],\n    \"includeVoidTransactions\": false,\n    \"includeNonBusinessPaymentTypes\": true\n}"
            # payload = {}
            headers = {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer '+str(token),
            }

            response = requests.request(
                "POST", url, headers=headers, data=payload)

            fetchData = json.loads(response.text)
            # print("fetchData: ", fetchData)
            if fetchData['error'] == False:
                # StoreID_StoreCode_DateFrom_DateTo

                fetchData_df = pd.DataFrame(
                    fetchData['result']['rawData'])

                if not fetchData_df.empty:

                    return fetchData_df
                else:
                    fetchData_df = pd.DataFrame()
                    return fetchData_df
            else:
                fetchData_df = pd.DataFrame()
                return fetchData_df
        else:
            fetchData_df = pd.DataFrame()
            return fetchData_df

    @ task(name="transformData",
           description="RawHeader transformData.",
           tags=["RawHeader", "transformData"])
    def RawHeader_transformData(rawHeader_df, StoreId):
        if not rawHeader_df.empty:
            rawHeader_df.rename(
                columns={
                    'UniqOrderId.Id': 'uniqueOrderId',
                    'Department': 'store',
                    'Conception': 'conception',
                    'OpenDate.Typed': 'accountingDay',
                    'OrderNum': 'receiptNo',
                    'TableNum': 'tableNo',
                    'SessionNum': 'shiftNo',
                    'OrderDeleted': 'orderDeleted',
                    'Delivery.IsDelivery': 'delivery',
                    'Delivery.ServiceType': 'deliveryType',
                    'RestaurantSection': 'section',
                    'Delivery.SourceKey': 'deliverySource',
                    'OrderType': 'orderType',
                    'ExternalNumber': 'externalOrderNo',
                    'Delivery.CustomerPhone': 'customerPhoneNumber',
                    'Delivery.CustomerName': 'customerFullName',
                    'Delivery.DeliveryComment': 'deliveryComment',
                    'OrderWaiter.Name': 'waiterForTheOrder',
                    'OpenTime': 'openingTime',
                    'Delivery.PrintTime': 'deliveryPrintTime',
                    'Delivery.ExpectedTime': 'plannedDeliveryTime',
                    'Delivery.SendTime': 'deliveryDispatchTime',
                    'Delivery.ActualTime': 'actualDeliveryTime',
                    'Delivery.CloseTime': 'deliveryClosingTime',
                    'PrechequeTime': 'guestBillTime',
                    'Delivery.BillTime': 'deliveryInvoicePrintTime',
                    'CloseTime': 'closingTime',
                    'Delivery.CancelCause': 'deliveryCancellationReason',
                    'Delivery.CancelComment': 'deliveryCancellationComment',
                    'PriceCategory': 'customerPriceCategory',
                    'DishSumInt': 'GrossSalesBD',
                    'DiscountSum': 'discountAmount',
                    'IncreaseSum': 'surchargeAmount',
                    'DishDiscountSumInt': 'grossSalesAD',
                    'VAT.Sum': 'VATByBillAmount',
                    'DishDiscountSumInt.withoutVAT': 'NetSalesAD',
                    'GuestNum': 'numberOfGuests',
                    'DishAmountInt': 'numberOfIitems',
                    'ItemSaleEventDiscountType.DiscountAmount': 'numberOfDiscountedItems',
                    'ItemSaleEventDiscountType.ComboAmount': 'numberOfComboDiscounts',
                    'DishDiscountSumInt.averageByGuest': 'averageRevenuePerGuest',
                    'DishServicePrintTime.Max': 'lastServPrintTime',
                    'Cooking.CookingDuration.Avg': 'AvgCookingTime',
                    'Cooking.Cooking1Duration.Avg': 'AvgCookingTime1',
                    'Cooking.Cooking2Duration.Avg': 'AvgCookingTime2',
                    'Cooking.Cooking3Duration.Avg': 'AvgCookingTime3',
                    'Cooking.Cooking4Duration.Avg': 'AvgCookingTime4',
                    "Cooking.CookingLateTime.Avg": "AverageCookingDelay",
                    "Cooking.ServeTime.Avg": "AverageServingTime",
                    "Cooking.GuestWaitTime.Avg": "AverageWaitingTimeToBeServed",
                    "Cooking.FeedLateTime.Avg": "AverageServingDelay"
                }, inplace=True)
            rawHeader_df['transformFlag'] = False
            rawHeader_df['storeID'] = StoreId
            rawHeader_df['fetchSales'] = False
            rawHeader_df['fetchPayment'] = False
            rawHeader_df = rawHeader_df[['uniqueOrderId', 'store', 'conception', 'accountingDay', 'receiptNo', 'tableNo', 'shiftNo', 'orderDeleted',
                                        'delivery', 'deliveryType', 'section', 'deliverySource', 'orderType', 'externalOrderNo', 'customerPhoneNumber',
                                         'customerFullName', 'deliveryComment', 'waiterForTheOrder', 'openingTime', 'deliveryPrintTime', 'plannedDeliveryTime',
                                         'deliveryDispatchTime', 'actualDeliveryTime', 'deliveryClosingTime', 'guestBillTime', 'deliveryInvoicePrintTime',
                                         'closingTime', 'deliveryCancellationReason', 'deliveryCancellationComment', 'customerPriceCategory', 'GrossSalesBD',
                                         'discountAmount', 'surchargeAmount', 'grossSalesAD', 'VATByBillAmount', 'NetSalesAD', 'numberOfGuests', 'numberOfIitems',
                                         'numberOfDiscountedItems', 'numberOfComboDiscounts', 'averageRevenuePerGuest', 'lastServPrintTime',
                                         'transformFlag', 'storeID', 'fetchSales', 'fetchPayment', "AvgCookingTime",
                                         "AvgCookingTime1", "AvgCookingTime2", "AvgCookingTime3", "AvgCookingTime4",
                                         "AverageCookingDelay", "AverageServingTime", "AverageWaitingTimeToBeServed", "AverageServingDelay"]]

            rawHeader_df.fillna("", inplace=True)
            return rawHeader_df
        else:
            fetchData_df = pd.DataFrame()
            return fetchData_df

    @ task(name="findDuplicate",
           description="RawHeader findDuplicate.",
           tags=["RawHeader", "findDuplicate"])
    def findDuplicate(rawHeader_df, conn, StoreId):

        if not rawHeader_df.empty:
            extractRawHeader = pd.read_sql_query(
                f"SELECT * FROM `rawHeader` WHERE storeId = {StoreId} and  transformFlag = 0", conn)

            print(extractRawHeader.shape)
            if not extractRawHeader.empty:
                duplicateRecord = pd.merge(
                    rawHeader_df, extractRawHeader.uniqueOrderId, how='inner')
                print("duplicateRecord Raw Header: ", duplicateRecord.shape)
                rawHeader_df = rawHeader_df[~rawHeader_df.uniqueOrderId.isin(
                    list(duplicateRecord.uniqueOrderId))]

            return rawHeader_df
        else:
            fetchData_df = pd.DataFrame()
            return fetchData_df

    @ task(name="loadData",
           description="RawHeader loadData.",
           tags=["RawHeader", "loadData"])
    def RawHeader_loadData(rawHeader_df, conn):
        try:
            if not rawHeader_df.empty:
                cursor = conn.cursor()
                paymentEntry_df_to_list = list(tuple(row)
                                               for row in rawHeader_df.values)
                insert_query = """INSERT INTO `rawHeader`
                            (`uniqueOrderId`,
                            `store`,
                            `conception`,
                            `accountingDay`,
                            `receiptNo`,
                            `tableNo`,
                            `shiftNo`,
                            `orderDeleted`,
                            `delivery`,
                            `deliveryType`,
                            `section`,
                            `deliverySource`,
                            `orderType`,
                            `externalOrderNo`,
                            `customerPhoneNumber`,
                            `customerFullName`,
                            `deliveryComment`,
                            `waiterForTheOrder`,
                            `openingTime`,
                            `deliveryPrintTime`,
                            `plannedDeliveryTime`,
                            `deliveryDispatchTime`,
                            `actualDeliveryTime`,
                            `deliveryClosingTime`,
                            `guestBillTime`,
                            `deliveryInvoicePrintTime`,
                            `closingTime`,
                            `deliveryCancellationReason`,
                            `deliveryCancellationComment`,
                            `customerPriceCategory`,
                            `GrossSalesBD`,
                            `discountAmount`,
                            `surchargeAmount`,
                            `grossSalesAD`,
                            `VATByBillAmount`,
                            `NetSalesAD`,
                            `numberOfGuests`,
                            `numberOfIitems`,
                            `numberOfDiscountedItems`,
                            `numberOfComboDiscounts`,
                            `averageRevenuePerGuest`,
                            `lastServPrintTime`,
                            `transformFlag`,
                            `storeID`,
                            `fetchSales`,
                            `fetchPayment`,
                            `AvgCookingTime`,
                            `AvgCookingTime1`,
                            `AvgCookingTime2`,
                            `AvgCookingTime3`,
                            `AvgCookingTime4`,
                            `AverageCookingDelay`,
                            `AverageServingTime`,
                            `AverageWaitingTimeToBeServed`,
                            `AverageServingDelay`)
                            VALUES
                            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

                cursor.executemany(insert_query, paymentEntry_df_to_list)
                conn.commit()
                print("INSERTED INTO Raw Header")
            else:
                print("Empty Raw Header")
        except Exception as e:
            print(str(e))


@ flow(name="RawHeader",
       description="Running all the task which are associated with Raw Header.",)
def callingRawHeader(token, conn, storeDetails, previousDateStr, dateFromStr):
    rawHeader = RawHeader()
    try:
        if token:
            print("Authenticate")

        session = rawHeader.RawHeader_selectStore(
            token, storeDetails['StoreId'])
        dataId = rawHeader.RawHeader_ExportRawHeader(
            token, session.cookies.get_dict(), previousDateStr, dateFromStr)
        status = rawHeader.RawHeader_FetchStatus(token, dataId['data'])
        print(dataId, status)

        fetchData_df = rawHeader.RawHeader_FetchData(
            token, dataId, status)
        print(fetchData_df.head(), storeDetails['StoreId'])
        print(fetchData_df.shape, storeDetails['StoreId'])
        rawHeader_df = rawHeader.RawHeader_transformData(
            fetchData_df, storeDetails['StoreId'])
        rawHeader_df = rawHeader.findDuplicate(
            rawHeader_df, conn, storeDetails['StoreId'])
        print("findDuplicate: ", storeDetails['StoreId'])
        print(rawHeader_df)
        rawHeader.RawHeader_loadData(rawHeader_df, conn)

    except Exception as err:
        logging.error(
            'FLOW-RawHeader - Error - ' + str(datetime.now().today()) + ' - ' + str(err))


"""
Raw Payments
"""


class RawPayment(RawHeader):

    @ task(name="ExportRawPayment",
           description=" RawPayment ExportRawPayment.",
           tags=["RawPayment", "ExportRawPayment"])
    def RawPayment_ExportRawPayment(token, selectStoreCookie, distinctData, dateFromStr, dateToStr):
        try:
            url = "https://yo-sushi-gourmet-gulf-co.iikoweb.co.uk/api/olap/init"

            payload = {
                "olapType": "SALES",
                "categoryFields": [],
                "groupFields": [
                    "UniqOrderId.Id",
                    "OpenDate.Typed",
                    "OrderNum",
                    "PayTypes",
                    "TableNum",
                    "SessionNum",
                    "PayTypes.Combo",
                    "CardNumber",
                    "Currencies.Currency",
                    "NonCashPaymentType",
                    "CreditUser",
                    "Currencies.CurrencyRate",
                    "AuthUser",
                    "AuthUser.Id",
                    "Cashier",
                    "Cashier.Id"
                ],
                "stackByDataFields": False,
                "dataFields": [
                    "DishDiscountSumInt",
                    "VAT.Sum",
                    "DishDiscountSumInt.withoutVAT",
                    "PayTypes.VoucherNum"
                ],
                "calculatedFields": [
                ],
                "filters": [
                    {
                        "field": "OpenDate.Typed",
                        "filterType": "date_range",
                        "dateFrom": dateFromStr,
                        "dateTo": dateToStr,
                        "valueMin": None,
                        "valueMax": None,
                        "valueList": [],
                        "includeLeft": True,
                        "includeRight": False,
                        "inclusiveList": True
                    },
                    {
                        "field": "UniqOrderId.Id",
                        "filterType": "value_list",
                        "valueList": distinctData,
                        "includeLeft": True,
                        "includeRight": False,
                        "inclusiveList": True
                    }
                ],
                "includeVoidTransactions": False,
                "includeNonBusinessPaymentTypes": True
            }
            headers = {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer '+str(token),
            }
            with open('paymentPayload.json', 'w') as f:
                json.dump(payload, f)
            response = requests.request(
                "POST", url, headers=headers, data=json.dumps(payload), cookies=selectStoreCookie)

            return json.loads(response.text)
        except Exception as err:
            logging.error(
                'olap_ExportPayment - Error - ' + str(datetime.now().today()) + ' - ' + str(err))

    @ task(name="transformData",
           description="RawPayment transformData.",
           tags=["RawPayment", "transformData"])
    def RawPayment_transformData(rawPayment_df):
        if not rawPayment_df.empty:
            rawPayment_df.rename(
                columns={
                    'UniqOrderId.Id': 'uniqueOrderId',
                    'PayTypes': 'paymentType',
                    'OpenDate.Typed': 'accountingDay',
                    'OrderNum': 'receiptNo',
                    'PayTypes.Combo': 'paymentTypeSplit',
                    'CardNumber': 'paymentCardNo',
                    'Currencies.Currency': 'paymentCurrency',
                    'NonCashPaymentType': 'nonCashPaymentType',
                    'CreditUser': 'creditedTo',
                    'Currencies.CurrencyRate': 'paymentExchangeRateAED',
                    'AuthUser': 'authorisedBy',
                    'AuthUser.Id': 'authorisedByID',
                    'Cashier': 'cashier',
                    'Cashier.Id': 'cashierID',
                    'DishDiscountSumInt': 'grossSalesAD',
                    'VAT.Sum': 'VATByBillAmount',
                    'DishDiscountSumInt.withoutVAT': 'netSalesAD',
                    'PayTypes.VoucherNum': 'numberOfVouchers'
                }, inplace=True
            )
            rawPayment_df['uid'] = rawPayment_df['uniqueOrderId'] + \
                "-" + rawPayment_df['paymentType']
            rawPayment_df = rawPayment_df[['uid', 'uniqueOrderId', 'paymentType', 'accountingDay', 'receiptNo', 'paymentTypeSplit',
                                           'paymentCardNo', 'paymentCurrency', 'nonCashPaymentType', 'creditedTo',
                                           'paymentExchangeRateAED', 'authorisedBy', 'authorisedByID', 'cashier',
                                           'cashierID', 'grossSalesAD', 'VATByBillAmount', 'netSalesAD', 'numberOfVouchers']]
            rawPayment_df.fillna("", inplace=True)
            return rawPayment_df
        else:
            rawPayment_df = pd.DataFrame()
            return rawPayment_df

    @ task(name="findDuplicate",
           description="RawPayment findDuplicate.",
           tags=["RawPayment", "findDuplicate"])
    def findDuplicate(rawPayment_df, conn):
        extractRawPayment = pd.read_sql_query(
            f"SELECT * FROM `rawPayment` WHERE uid in {tuple(rawPayment_df.uid)}", conn)

        print(extractRawPayment.shape)
        if not extractRawPayment.empty:
            duplicateRecord = pd.merge(
                rawPayment_df, extractRawPayment.uid, how='inner')

            print("duplicateRecord Raw Payment: ", duplicateRecord.shape)
            rawPayment_df = rawPayment_df[~rawPayment_df.uid.isin(
                list(duplicateRecord.uid))]

        return rawPayment_df

    @ task(name="loadData",
           description="RawPayment loadData.",
           tags=["RawPayment", "loadData"])
    def RawPayment_loadData(rawPayment_df, conn, distinctData):
        try:
            if not rawPayment_df.empty:
                cursor = conn.cursor()
                rawPayment_df_to_list = list(tuple(row)
                                             for row in rawPayment_df.values)
                insert_query = """INSERT INTO `rawPayment`
                                (`uid`, `uniqueOrderId`,`paymentType`,`accountingDay`,`receiptNo`,`paymentTypeSplit`,
                                `paymentCardNo`,`paymentCurrency`,`nonCashPaymentType`,`creditedTo`,
                                `paymentExchangeRateAED`,`authorisedBy`,`authorisedByID`,`cashier`,`cashierID`,
                                `grossSalesAD`,`VATByBillAmount`,`netSalesAD`,`numberOfVouchers`)
                                VALUES
                            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                cursor.executemany(insert_query, rawPayment_df_to_list)
                conn.commit()
                print("INSERTED INTO Raw Payment")
                updateQuery = f"""
                    UPDATE `rawHeader` SET fetchPayment = True
                    WHERE uniqueOrderId IN {tuple(distinctData)};
                """
                cursor.execute(updateQuery)
                conn.commit()
            else:
                print("Raw Payment Empty")
        except Exception as e:
            print(str(e))


@ flow(name="RawPayment",
       description="Running all the task which are associated with Raw Payment.",)
def callingRawPayment(token, conn, storeDetails):
    rawPayment = RawPayment()
    rawPaymentFetchData = pd.read_sql(
        "SELECT uniqueOrderId FROM `rawHeader` WHERE fetchPayment = 0;", conn)

    if not rawPaymentFetchData.empty:
        data_df = pd.read_sql(
            """SELECT max(cast(accountingDay as date)) as maxDate, min(cast(accountingDay as date)) as minDate 
                FROM `rawHeader` WHERE fetchPayment = 0;""", conn)
        dateFrom = data_df.minDate.values[0]
        dateTo = data_df.maxDate.values[0]
        dateFromStr = str(
            dateFrom.strftime('%Y-%m-%dT%H:%M:%S'))
        dateToStr = str(
            dateTo.strftime('%Y-%m-%dT%H:%M:%S'))

        distinctData = list(
            rawPaymentFetchData.uniqueOrderId.values)
        try:

            session = rawPayment.RawHeader_selectStore(
                token, storeDetails['StoreId'])
            dataId = rawPayment.RawPayment_ExportRawPayment(
                token, session.cookies.get_dict(), distinctData, dateFromStr, dateToStr)
            status = rawPayment.RawHeader_FetchStatus(token, dataId['data'])
            fetchData_df = rawPayment.RawHeader_FetchData(
                token, dataId, status)
            logging.info(
                'FLOW-RawPayment - DETAIL - ' + str(storeDetails['StoreId']) + ' - ' + str(dataId) + ' - ' + str(status) + '-' +
                str(fetchData_df.shape))
            rawPayment_df = rawPayment.RawPayment_transformData(fetchData_df)
            rawPayment_df = rawPayment.findDuplicate(rawPayment_df, conn)
            rawPayment.RawPayment_loadData(rawPayment_df, conn, distinctData)
        except Exception as err:
            logging.error(
                'FLOW-RawHeader - Error - ' + str(datetime.now().today()) + ' - ' + str(err))


"""
Raw Sales
"""


class RawSales(RawHeader):

    @ task(name="ExportRawSales",
           description=" RawSales ExportRawSales.",
           tags=["RawSales", "ExportRawSales"])
    def RawSales_ExportRawSales(token, selectStoreCookie, distinctData, dateFromStr, dateToStr):
        try:
            url = "https://yo-sushi-gourmet-gulf-co.iikoweb.co.uk/api/olap/init"

            payload = {
                "olapType": "SALES",
                "categoryFields": [],
                "groupFields": [
                    "UniqOrderId.Id",
                    "Department",
                    "Conception",
                    "OpenDate.Typed",
                    "OrderNum",
                    "TableNum",
                    "DeletedWithWriteoff",
                    "RemovalType",
                    "DeletionComment",
                    "DishId",
                    "DishCode",
                    "DishName",
                    "DishFullName",
                    "DishCategory.Accounting",
                    "DishCategory",
                    "DishGroup",
                    "DishGroup.TopParent",
                    "DishGroup.SecondParent",
                    "DishGroup.ThirdParent",
                    "Cooking.ServeNumber",
                    "WaiterName",
                    "DishServicePrintTime",
                    "Comment",
                    "SoldWithDish",
                    "OrderDiscount.Type",
                    "OrderIncrease.Type",
                    "ItemSaleEventDiscountType",
                    "Delivery.CookingFinishTime"
                ],
                "stackByDataFields": False,
                "dataFields": [
                    "DishAmountInt",
                    "DishSumInt.averagePriceWithVAT",
                    "DishDiscountSumInt.averagePriceWithVAT",
                    "DishSumInt",
                    "DiscountSum",
                    "IncreaseSum",
                    "DishDiscountSumInt",
                    "VAT.Sum",
                    "DishDiscountSumInt.withoutVAT",
                    "Cooking.CookingDuration.Avg",
                    "Cooking.Cooking1Duration.Avg",
                    "Cooking.Cooking2Duration.Avg",
                    "Cooking.Cooking3Duration.Avg",
                    "Cooking.Cooking4Duration.Avg",
                    "Cooking.CookingLateTime.Avg",
                    "Cooking.ServeTime.Avg",
                    "Cooking.GuestWaitTime.Avg",
                    "Cooking.FeedLateTime.Avg"
                ],
                "calculatedFields": [
                ],
                "filters": [
                    {
                        "field": "OpenDate.Typed",
                        "filterType": "date_range",
                        "dateFrom": dateFromStr,
                        "dateTo": dateToStr,
                        "valueMin": None,
                        "valueMax": None,
                        "valueList": [],
                        "includeLeft": True,
                        "includeRight": False,
                        "inclusiveList": True
                    },
                    {
                        "field": "UniqOrderId.Id",
                        "filterType": "value_list",
                        "valueList": distinctData,
                        "includeLeft": True,
                        "includeRight": False,
                        "inclusiveList": True
                    }

                ],
                "includeVoidTransactions": False,
                "includeNonBusinessPaymentTypes": True
            }
            headers = {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer '+str(token),
            }

            response = requests.request(
                "POST", url, headers=headers, data=json.dumps(payload), cookies=selectStoreCookie)

            return json.loads(response.text)
        except Exception as err:
            logging.error(
                'olap_ExportPayment - Error - ' + str(datetime.now().today()) + ' - ' + str(err))

    @ task(name="transformData",
           description="RawSales transformData.",
           tags=["RawSales", "transformData"])
    def RawSales_transformData(rawSale_df):
        if not rawSale_df.empty:
            rawSale_df.rename(
                columns={
                    'UniqOrderId.Id': 'uniqueOrderId',
                    'Department': 'store',
                    'Conception': 'concept',
                    'OpenDate.Typed': 'accountingDay',
                    'OrderNum': 'receiptNo',
                    'TableNum': 'tableNo',
                    'DeletedWithWriteoff': 'itemDeleted',
                    'RemovalType': 'itemDeletionReason',
                    'DeletionComment': 'itemDeletionComment',
                    'DishCode': 'itemCode',
                    'DishId': 'itemId',
                    'DishName': 'item',
                    'DishFullName': 'itemFullName',
                    'DishCategory.Accounting': 'itemAccountingCategory',
                    'DishCategory': 'itemCategory',
                    'DishGroup': 'itemGroup',
                    'DishGroup.TopParent': 'level1_itemGroup',
                    'DishGroup.SecondParent': 'level2_itemGroup',
                    'DishGroup.ThirdParent': 'level3_itemGroup',
                    'Cooking.ServeNumber': 'courseNo',
                    'WaiterName': 'itemWaiter',
                    'DishServicePrintTime': 'itemServicePrinting',
                    'Comment': 'itemComment',
                    'SoldWithDish': 'soldWithItem',
                    'OrderDiscount.Type': 'discountType',
                    'OrderIncrease.Type': 'surchargeType',
                    'ItemSaleEventDiscountType': 'nameOfDiscountOrSurcharge',
                    'Delivery.CookingFinishTime': 'cookingFinishedAt',
                    'DishAmountInt': 'numberOfItems',
                    'DishSumInt.averagePriceWithVAT': 'averagePriceBeforeDiscount',
                    'DishDiscountSumInt.averagePriceWithVAT': 'averagePrice',
                    'DishSumInt': 'grossSalesBD',
                    'DiscountSum': 'discountAmount',
                    'IncreaseSum': 'surchargeAmountAED',
                    'DishDiscountSumInt': 'grossSalesAD',
                    'VAT.Sum': 'VATByBillAmount',
                    'DishDiscountSumInt.withoutVAT': 'netSalesAD',
                    'Cooking.CookingDuration.Avg': 'cookingTimeAverage',
                    'Cooking.Cooking1Duration.Avg': 'cookingTimeAverage1',
                    'Cooking.Cooking2Duration.Avg': 'cookingTimeAverage2',
                    'Cooking.Cooking3Duration.Avg': 'cookingTimeAverage3',
                    'Cooking.Cooking4Duration.Avg': 'cookingTimeAverage4',
                    "Cooking.CookingLateTime.Avg": "AverageCookingDelay",
                    "Cooking.ServeTime.Avg": "AverageServingTime",
                    "Cooking.GuestWaitTime.Avg": "AverageWaitingTimeToBeServed",
                    "Cooking.FeedLateTime.Avg": "AverageServingDelay"
                }, inplace=True
            )
            rawSale_df = rawSale_df[['uniqueOrderId', 'store', 'concept', 'accountingDay', 'receiptNo', 'tableNo',
                                     'itemDeleted', 'itemDeletionReason', 'itemDeletionComment', 'itemCode', 'itemId',
                                    'item', 'itemFullName', 'itemAccountingCategory', 'itemCategory', 'itemGroup', 'level1_itemGroup',
                                     'level2_itemGroup', 'level3_itemGroup', 'courseNo', 'itemWaiter', 'itemServicePrinting',
                                     'itemComment', 'soldWithItem', 'discountType', 'surchargeType', 'nameOfDiscountOrSurcharge',
                                     'cookingFinishedAt', 'numberOfItems', 'averagePriceBeforeDiscount', 'averagePrice',
                                     'grossSalesBD', 'discountAmount', 'surchargeAmountAED', 'grossSalesAD', 'VATByBillAmount',
                                     'netSalesAD', 'cookingTimeAverage', 'cookingTimeAverage1', 'cookingTimeAverage2',
                                     'cookingTimeAverage3', 'cookingTimeAverage4', 'AverageCookingDelay',
                                     'AverageServingTime', 'AverageWaitingTimeToBeServed', 'AverageServingDelay']]
            rawSale_df.fillna("", inplace=True)
            return rawSale_df
        else:
            rawSale_df = pd.DataFrame()
            return rawSale_df

    @ task(name="findDuplicate",
           description="RawSales findDuplicate.",
           tags=["RawSales", "findDuplicate"])
    def findDuplicate(rawSale_df, conn):
        if not rawSale_df.empty:
            extractRawSales = pd.read_sql_query(
                f"SELECT * FROM `rawSales` WHERE uniqueOrderId in {tuple(rawSale_df.uniqueOrderId)}", conn)

            print(extractRawSales.shape)
            if not extractRawSales.empty:
                duplicateRecord = pd.merge(
                    rawSale_df, extractRawSales.uniqueOrderId, how='inner')
                print("duplicateRecord Raw Sale: ", duplicateRecord.shape)
                rawSale_df = rawSale_df[~rawSale_df.uniqueOrderId.isin(
                    list(duplicateRecord.uniqueOrderId))]

            print("Raw Sale: ", rawSale_df.shape)
            return rawSale_df
        else:
            fetchData_df = pd.DataFrame()
            return fetchData_df

    @ task(name="loadData",
           description="RawSales loadData.",
           tags=["RawSales", "loadData"])
    def RawSales_loadData(rawSale_df, conn, distinctData):
        if not rawSale_df.empty:
            cursor = conn.cursor()
            rawSale_df_to_list = list(tuple(row)
                                      for row in rawSale_df.values)
            insert_query = """INSERT INTO `rawSales`
                (`uniqueOrderId`, `store`, `concept`, `accountingDay`, `receiptNo`, `tableNo`, `itemDeleted`,
                `itemDeletionReason`, `itemDeletionComment`, `itemCode`, `itemId`, `item`, `itemFullName`,
                `itemAccountingCategory`, `itemCategory`, `itemGroup`, `level1_itemGroup`, `level2_itemGroup`,
                `level3_itemGroup`, `courseNo`, `itemWaiter`, `itemServicePrinting`, `itemComment`, `soldWithItem`,
                `discountType`, `surchargeType`, `nameOfDiscountOrSurcharge`, `cookingFinishedAt`,
                `numberOfItems`, `averagePriceBeforeDiscount`, `averagePrice`, `grossSalesBD`, `discountAmount`,
                `surchargeAmountAED`, `grossSalesAD`, `VATByBillAmount`, `netSalesAD`, `cookingTimeAverage`,
                `cookingTimeAverage1`, `cookingTimeAverage2`, `cookingTimeAverage3`, `cookingTimeAverage4`,
                `AverageCookingDelay`, `AverageServingTime`, `AverageWaitingTimeToBeServed`, `AverageServingDelay`)
                VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(insert_query, rawSale_df_to_list)
            conn.commit()
            print("INSERTED INTO Raw Sales")

            updateQuery = f"""
                UPDATE `rawHeader` SET fetchSales = True
                WHERE uniqueOrderId IN {tuple(distinctData)};
            """
            cursor.execute(updateQuery)
            conn.commit()


@ flow(name="RawSales",
       description="Running all the task which are associated with Raw Payment.",)
def callingRawSales(token, conn, storeDetails):
    rawSale = RawSales()
    rawSalesFetchData = pd.read_sql(
        "SELECT uniqueOrderId FROM `rawHeader` WHERE fetchSales = 0;", conn)

    if not rawSalesFetchData.empty:
        data_df = pd.read_sql(
            """SELECT max(cast(accountingDay as date)) as maxDate, min(cast(accountingDay as date)) as minDate 
                FROM `rawHeader` WHERE fetchSales = 0;""", conn)
        dateFrom = data_df.minDate.values[0]
        dateTo = data_df.maxDate.values[0]
        dateFromStr = str(
            dateFrom.strftime('%Y-%m-%dT%H:%M:%S'))
        dateToStr = str(
            dateTo.strftime('%Y-%m-%dT%H:%M:%S'))

        distinctData = list(
            rawSalesFetchData.uniqueOrderId.values)
        try:

            print("distinctData")
            print(len(distinctData))
            session = rawSale.RawHeader_selectStore(
                token, storeDetails['StoreId'])
            dataId = rawSale.RawSales_ExportRawSales(
                token, session.cookies.get_dict(), distinctData, dateFromStr, dateToStr)
            status = rawSale.RawHeader_FetchStatus(token, dataId['data'])

            if status['data'] == 'SUCCESS':
                print(dataId, status)
                fetchData_df = rawSale.RawHeader_FetchData(
                    token, dataId, status)
                print("RAW SALES DATA")
                print(fetchData_df)
                rawSale_df = rawSale.RawSales_transformData(fetchData_df)
                print("rawSale_df transformData: ", rawSale_df.shape)
                rawSale_df = rawSale.findDuplicate(rawSale_df, conn)
                rawSale.RawSales_loadData(rawSale_df, conn, distinctData)
            else:
                logging.error('FLOW-RawSale - DETAIL - ' +
                              str(storeDetails['StoreId']) + ' - ' + str(dataId) + ' - ' + str(status))
        except Exception as err:
            logging.error(
                'FLOW-RawHeader - Error - ' + str(datetime.now().today()) + ' - ' + str(err))


@ flow(name="iiko-Flows",
       description="Running all the flow which are associated with iiko.",
       task_runner=SequentialTaskRunner(),)
def runFlows(conn):
    f = open("config.json")
    configData = json.load(f)

    StoreData_df = pd.read_csv(
        configData['SourceFolder']+configData['StoreMaster'])
    StoreData_df.StoreId = StoreData_df.StoreId.astype(str)
    StoreData_dict = StoreData_df.to_dict(orient='records')

    if configData['CustomDate']['status']:

        storeAttempts = 0
        dateFrom = configData['CustomDate']['DateFrom']
        dateTo = configData['CustomDate']['DateTo']
        for storeDetails in StoreData_dict:
            token = Authenticate()
            callingRawHeader(token,
                             conn, storeDetails, dateFrom, dateTo)
            callingRawPayment(token, conn, storeDetails)
            callingRawSales(token, conn, storeDetails)

            storeAttempts += 1
            if configData['testing']:
                if storeAttempts >= configData['maxStoreTest']:
                    break

    else:
        for storeDetails in StoreData_dict:
            token = Authenticate()
            data_df = pd.read_sql(
                f"""SELECT MAX(CAST(accountingDay as date)) AS "maxDate" FROM `rawHeader` WHERE storeID = {storeDetails['StoreId']};""", conn)
            dateFrom = data_df.maxDate.values[0]
            dateTo = date.today()

            diffDate = dateTo - dateFrom
            diffDay = int(str(diffDate).split(',')[0].split(' ')[0])

            if diffDay > configData['maxDays']:

                daysMax = int(diffDay / configData['maxDays'])
                daysMin = int(diffDay % configData['maxDays'])

                for dataValue in range(daysMax):

                    previousDate = dateFrom + timedelta(days=1)
                    dateFrom = dateFrom + timedelta(days=configData['maxDays'])

                    dateFromStr = str(
                        dateFrom.strftime('%Y-%m-%dT%H:%M:%S'))
                    previousDateStr = str(
                        previousDate.strftime('%Y-%m-%dT%H:%M:%S'))

                    callingRawHeader(token,
                                     conn, storeDetails, previousDateStr, dateFromStr)
                    callingRawPayment(token, conn, storeDetails)
                    callingRawSales(token, conn, storeDetails)

                previousDate = dateFrom + timedelta(days=1)
                dateFrom = dateFrom + timedelta(days=daysMin)
                dateFromStr = str(
                    dateFrom.strftime('%Y-%m-%dT%H:%M:%S'))
                previousDateStr = str(
                    previousDate.strftime('%Y-%m-%dT%H:%M:%S'))

                # for storeDetails in StoreData_dict:
                #     token = Authenticate()
                callingRawHeader(token,
                                 conn, storeDetails, previousDateStr, dateFromStr)
                callingRawPayment(token, conn, storeDetails)
                callingRawSales(token, conn, storeDetails)
                if configData['testing']:
                    break

            else:
                dateFromStr = str(
                    dateFrom.strftime('%Y-%m-%dT%H:%M:%S'))
                dateToStr = str(
                    dateTo.strftime('%Y-%m-%dT%H:%M:%S'))

                # for storeDetails in StoreData_dict:
                callingRawHeader(token,
                                 conn, storeDetails, dateFromStr, dateToStr)
                callingRawPayment(token, conn, storeDetails)
                callingRawSales(token, conn, storeDetails)
                if configData['testing']:
                    break


runFlows(conn)


# ADD STOREID in rawPayment and rawSales
