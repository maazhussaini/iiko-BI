from ast import While
from distutils.command.config import config
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
# @task(name="DatabaseConnection",
#       description="DatabaseConnection.",
#       tags=["GENERAL", "DatabaseConnection"])

count = 0


def DatabaseConnection():
    conn = sql.connect(host="rawiikodata.cpi0e90jeqda.us-east-2.rds.amazonaws.com",
                            user="admin", password="MySql12$34#", database="rawDataTest")

    return conn


conn = sql.connect(host="rawiikodata.cpi0e90jeqda.us-east-2.rds.amazonaws.com",
                   user="admin", password="MySql12$34#", database="rawDataTest")


@task(name="Authenticate",
      description="Authenticate Api.",
      tags=["GENERAL", "Authenticate"])
def Authenticate():
    main_url = "https://yo-sushi-gourmet-gulf-co.iikoweb.co.uk"
    url = main_url+"/api/auth/login"
    payload = "{\n    \"login\": \"navision\",\n    \"password\": \"75KuJSiyXWrzJU1i\"\n}"
    headers = {}
    s = requests.Session()
    response = requests.request("POST", url, headers=headers, data=payload)

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

            """
            if configData['CustomDate']['status']:
                payload = {
                    "olapType": "SALES",
                    "categoryFields": [],
                    "groupFields": [
                                "UniqOrderId.Id",
                                "Department",
                                "Conception",
                                "OpenDate.Typed",
                                "OrderNum",
                                "Delivery.IsDelivery",
                                "PayTypes",
                                "Delivery.SourceKey",
                                "OrderType",
                                "ExternalNumber",
                                "CreditUser"
                    ],
                    "stackByDataFields": False,
                    "dataFields": [
                        "DishDiscountSumInt"
                    ],
                    "calculatedFields": [
                    ],
                    "filters": [
                        {
                            "field": "OpenDate.Typed",
                            "filterType": "date_range",
                            "dateFrom": configData['CustomDate']['DateFrom'],
                            "dateTo": configData['CustomDate']['DateTo'],
                            "valueMin": None,
                            "valueMax": None,
                            "valueList": [],
                            "includeLeft": True,
                            "includeRight": False,
                            "inclusiveList": True
                        },
                        {
                            "field": "Conception",
                            "filterType": "value_list",
                            "valueList": ["YoSushi"],
                            "includeLeft": True,
                            "includeRight": False,
                            "inclusiveList": True
                        }

                    ],
                    "includeVoidTransactions": False,
                    "includeNonBusinessPaymentTypes": True
                }
            else:
                payload = {
                    "olapType": "SALES",
                    "categoryFields": [],
                    "groupFields": [
                                "UniqOrderId.Id",
                                "Department",
                                "Conception",
                                "OpenDate.Typed",
                                "OrderNum",
                                "Delivery.IsDelivery",
                                "PayTypes",
                                "Delivery.SourceKey",
                                "OrderType",
                                "ExternalNumber",
                                "CreditUser"
                    ],
                    "stackByDataFields": False,
                    "dataFields": [
                        "DishDiscountSumInt"
                    ],
                    "calculatedFields": [
                    ],
                    "filters": [
                        {
                            "field": "OpenDate.Typed",
                            "filterType": "date_range",
                            "dateFrom": self.previous_dateTime,
                            "dateTo": self.previous_dateTime,
                            "valueMin": None,
                            "valueMax": None,
                            "valueList": [],
                            "includeLeft": True,
                            "includeRight": False,
                            "inclusiveList": True
                        },
                        {
                            "field": "Conception",
                            "filterType": "value_list",
                            "valueList": ["YoSushi"],
                            "includeLeft": True,
                            "includeRight": False,
                            "inclusiveList": True
                        }

                    ],
                    "includeVoidTransactions": False,
                    "includeNonBusinessPaymentTypes": True
                }
            """

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
                    "DishServicePrintTime.Max"
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
            # response = requests.request(
            #     "POST", url, headers=headers, data=json.dumps(payload), cookies=selectStoreCookie.cookies.get_dict())

            response = requests.request(
                "POST", url, headers=headers, data=json.dumps(payload), cookies=selectStoreCookie)

            return json.loads(response.text)
        except Exception as err:
            logging.error(
                'olap_ExportPayment - Error - ' + str(datetime.now().today()) + ' - ' + str(err))

    # @task(name="FetchStatus",
    #       description="RawHeader FetchStatus.",
    #       tags=["RawHeader", "FetchStatus"],
    #       retries=3, retry_delay_seconds=60)
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
                    'DishServicePrintTime.Max': 'lastServPrintTime'
                }, inplace=True)

            rawHeader_df = rawHeader_df[['uniqueOrderId', 'store', 'conception', 'accountingDay', 'receiptNo', 'tableNo', 'shiftNo', 'orderDeleted',
                                        'delivery', 'deliveryType', 'section', 'deliverySource', 'orderType', 'externalOrderNo', 'customerPhoneNumber',
                                         'customerFullName', 'deliveryComment', 'waiterForTheOrder', 'openingTime', 'deliveryPrintTime', 'plannedDeliveryTime',
                                         'deliveryDispatchTime', 'actualDeliveryTime', 'deliveryClosingTime', 'guestBillTime', 'deliveryInvoicePrintTime',
                                         'closingTime', 'deliveryCancellationReason', 'deliveryCancellationComment', 'customerPriceCategory', 'GrossSalesBD',
                                         'discountAmount', 'surchargeAmount', 'grossSalesAD', 'VATByBillAmount', 'NetSalesAD', 'numberOfGuests', 'numberOfIitems',
                                         'numberOfDiscountedItems', 'numberOfComboDiscounts', 'averageRevenuePerGuest', 'lastServPrintTime']]
            rawHeader_df['transformFlag'] = False
            rawHeader_df['storeID'] = StoreId
            rawHeader_df['fetchSales'] = False
            rawHeader_df['fetchPayment'] = False
            rawHeader_df.fillna("", inplace=True)
            return rawHeader_df
        else:
            fetchData_df = pd.DataFrame()
            return fetchData_df

    @ task(name="findDuplicate",
           description="RawHeader findDuplicate.",
           tags=["RawHeader", "findDuplicate"])
    def findDuplicate(rawHeader_df, conn):

        if not rawHeader_df.empty:
            extractRawHeader = pd.read_sql_query(
                "SELECT * FROM `rawDataTest`.`rawHeader` WHERE transformFlag = 0", conn)

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
                insert_query = """INSERT INTO `rawDataTest`.`rawHeader`
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
                            `fetchPayment`)
                            VALUES
                            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

                cursor.executemany(insert_query, paymentEntry_df_to_list)
                conn.commit()
                print("INSERTED INTO Raw Header")
            else:
                print("Empty Raw Header")
        except Exception as e:
            print(str(e))


@ flow(name="RawHeader",
       description="Running all the task which are associated with Raw Header.",)
def callingRawHeader(conn, storeDetails, previousDateStr, dateFromStr):
    rawHeader = RawHeader()
    try:
        token = Authenticate()
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
        print(fetchData_df.head())
        print(fetchData_df.shape)
        rawHeader_df = rawHeader.RawHeader_transformData(
            fetchData_df, storeDetails['StoreId'])
        rawHeader_df = rawHeader.findDuplicate(rawHeader_df, conn)
        print("findDuplicate")
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
    def RawPayment_ExportRawPayment(token, selectStoreCookie, distinctData):
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
                        "dateFrom": "2022-06-01T00:00:00",
                        "dateTo": "2022-09-01T00:00:00",
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
           description="RawPayment transformData.",
           tags=["RawPayment", "transformData"])
    def RawPayment_transformData(rawPayment_df):
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

    @ task(name="findDuplicate",
           description="RawPayment findDuplicate.",
           tags=["RawPayment", "findDuplicate"])
    def findDuplicate(rawPayment_df, conn):
        extractRawPayment = pd.read_sql_query(
            "SELECT * FROM `rawDataTest`.`rawPayment`", conn)

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
                insert_query = """INSERT INTO `rawDataTest`.`rawPayment`
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
                    UPDATE `rawDataTest`.`rawHeader` SET fetchPayment = True
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
def callingRawPayment(conn, storeDetails):
    rawPayment = RawPayment()
    rawPaymentFetchData = pd.read_sql(
        "SELECT uniqueOrderId FROM `rawDataTest`.`rawHeader` WHERE fetchPayment = 0;", conn)
    distinctData = list(
        rawPaymentFetchData.uniqueOrderId.values)
    try:
        token = Authenticate()

        session = rawPayment.RawHeader_selectStore(
            token, storeDetails['StoreId'])
        dataId = rawPayment.RawPayment_ExportRawPayment(
            token, session.cookies.get_dict(), distinctData)
        status = rawPayment.RawHeader_FetchStatus(token, dataId['data'])
        fetchData_df = rawPayment.RawHeader_FetchData(
            token, dataId, status)
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
    def RawSales_ExportRawSales(token, selectStoreCookie, distinctData):
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
                    "Cooking.Cooking4Duration.Avg"
                ],
                "calculatedFields": [
                ],
                "filters": [
                    {
                        "field": "OpenDate.Typed",
                        "filterType": "date_range",
                        "dateFrom": "2022-06-01T00:00:00",
                        "dateTo": "2022-09-01T00:00:00",
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
                    'Cooking.Cooking4Duration.Avg': 'cookingTimeAverage4'
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
                                     'cookingTimeAverage3', 'cookingTimeAverage4']]
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
                "SELECT * FROM `rawDataTest`.`rawSales`", conn)

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
            insert_query = """INSERT INTO `rawDataTest`.`rawSales`
                (`uniqueOrderId`, `store`, `concept`, `accountingDay`, `receiptNo`, `tableNo`, `itemDeleted`,
                `itemDeletionReason`, `itemDeletionComment`, `itemCode`, `itemId`, `item`, `itemFullName`,
                `itemAccountingCategory`, `itemCategory`, `itemGroup`, `level1_itemGroup`, `level2_itemGroup`,
                `level3_itemGroup`, `courseNo`, `itemWaiter`, `itemServicePrinting`, `itemComment`, `soldWithItem`,
                `discountType`, `surchargeType`, `nameOfDiscountOrSurcharge`, `cookingFinishedAt`,
                `numberOfItems`, `averagePriceBeforeDiscount`, `averagePrice`, `grossSalesBD`, `discountAmount`,
                `surchargeAmountAED`, `grossSalesAD`, `VATByBillAmount`, `netSalesAD`, `cookingTimeAverage`,
                `cookingTimeAverage1`, `cookingTimeAverage2`, `cookingTimeAverage3`, `cookingTimeAverage4`)
                VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(insert_query, rawSale_df_to_list)
            conn.commit()
            print("INSERTED INTO Raw Sales")

            updateQuery = f"""
                UPDATE `rawDataTest`.`rawHeader` SET fetchSales = True
                WHERE uniqueOrderId IN {tuple(distinctData)};
            """
            cursor.execute(updateQuery)
            conn.commit()


@ flow(name="RawSales",
       description="Running all the task which are associated with Raw Payment.",)
def callingRawSales(conn, storeDetails):
    rawSale = RawSales()
    rawSalesFetchData = pd.read_sql(
        "SELECT uniqueOrderId FROM `rawDataTest`.`rawHeader` WHERE fetchSales = 0;", conn)
    distinctData = list(
        rawSalesFetchData.uniqueOrderId.values)
    try:
        token = Authenticate()
        print("distinctData")
        print(len(distinctData))
        session = rawSale.RawHeader_selectStore(
            token, storeDetails['StoreId'])
        dataId = rawSale.RawSales_ExportRawSales(
            token, session.cookies.get_dict(), distinctData)
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
    except Exception as err:
        logging.error(
            'FLOW-RawHeader - Error - ' + str(datetime.now().today()) + ' - ' + str(err))


@ flow(name="iiko-Flows",
       description="Running all the flow which are associated with iiko.",
       task_runner=SequentialTaskRunner(),)
def runFlows(conn):
    f = open("config.json")
    configData = json.load(f)

    StoreData_df = pd.read_csv('Requirements/Data/StoreMasterv2,0.csv')
    StoreData_df.StoreId = StoreData_df.StoreId.astype(str)
    StoreData_dict = StoreData_df.to_dict(orient='records')

    if configData['CustomDate']['status']:
        storeAttempts = 0
        dateFrom = configData['CustomDate']['DateFrom']
        dateTo = configData['CustomDate']['DateTo']
        for storeDetails in StoreData_dict:
            callingRawHeader(
                conn, storeDetails, dateFrom, dateTo)
            callingRawPayment(conn, storeDetails)
            callingRawSales(conn, storeDetails)

            storeAttempts += 1
            if configData['testing']:
                if storeAttempts >= configData['maxStoreTest']:
                    break

    else:
        data_df = pd.read_sql(
            """SELECT MAX(CAST(accountingDay as date)) AS "maxDate" FROM `rawDataTest`.`rawHeader`;""", conn)
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

                for storeDetails in StoreData_dict:
                    callingRawHeader(
                        conn, storeDetails, previousDateStr, dateFromStr)
                    callingRawPayment(conn, storeDetails)
                    callingRawSales(conn, storeDetails)

            previousDate = dateFrom + timedelta(days=1)
            dateFrom = dateFrom + timedelta(days=daysMin)
            dateFromStr = str(
                dateFrom.strftime('%Y-%m-%dT%H:%M:%S'))
            previousDateStr = str(
                previousDate.strftime('%Y-%m-%dT%H:%M:%S'))

            for storeDetails in StoreData_dict:
                callingRawHeader(
                    conn, storeDetails, previousDateStr, dateFromStr)
                callingRawPayment(conn, storeDetails)
                callingRawSales(conn, storeDetails)
                if configData['testing']:
                    break

        else:
            dateFromStr = str(
                dateFrom.strftime('%Y-%m-%dT%H:%M:%S'))
            dateToStr = str(
                dateTo.strftime('%Y-%m-%dT%H:%M:%S'))

            for storeDetails in StoreData_dict:
                callingRawHeader(
                    conn, storeDetails, dateFromStr, dateToStr)
                callingRawPayment(conn, storeDetails)
                callingRawSales(conn, storeDetails)
                if configData['testing']:
                    break


runFlows(conn)


# ADD STOREID in rawPayment and rawSales
