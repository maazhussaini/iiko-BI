from ast import While
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

current_date_time_obj = datetime.now().today()
current_date_time_str = str(
    current_date_time_obj.strftime('%Y-%m-%d %H-%M-%S'))
logging.basicConfig(filename="Logs/log-"+current_date_time_str+".txt")


class DatabaseConnection:

    def __init__(self):
        self.conn = sql.connect(host="rawiikodata.cpi0e90jeqda.us-east-2.rds.amazonaws.com",
                                user="admin", password="MySql12$34#", database="rawDataTest")
        self.cursor = self.conn.cursor()


class PaymentEntry(Task):

    def __init__(self):
        """
        GET CURRENT DIRECTORY
        """
        self.cwd = os.getcwd()

        """
        READ CONFIG FILE
        """
        f = open("config.json")
        self.configData = json.load(f)

        """
        Transform Current Date
        """
        self.previous_dateTime = date.today() - timedelta(days=1)
        self.previous_dateTime = str(
            self.previous_dateTime.strftime('%Y-%m-%dT%H:%M:%S'))

        self.DumpFolder_csv = self.configData['DumpFolder']['main'] + \
            self.configData['DumpFolder']['csv']
        self.DumpFolder_json = self.configData['DumpFolder']['main'] + \
            self.configData['DumpFolder']['json']
        self.SourceFolder = self.configData['SourceFolder']

        self.main_url = "https://yo-sushi-gourmet-gulf-co.iikoweb.co.uk"
        url = self.main_url+"/api/auth/login"
        payload = "{\n    \"login\": \"navision\",\n    \"password\": \"75KuJSiyXWrzJU1i\"\n}"
        headers = {}
        self.s = requests.Session()
        response = requests.request("POST", url, headers=headers, data=payload)

        response = json.loads(response.text)
        if response['error'] == False:
            self.token = response['token']
        else:
            logging.critical('Critical - ' + str(datetime.now().today()
                                                 ) + ' - ' + str(response['errorMessage']))

    def getStore(self):

        url = "https://yo-sushi-gourmet-gulf-co.iikoweb.co.uk/api/stores/list"
        payload = {}
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer '+str(self.token)
        }
        response = requests.request("GET", url, headers=headers, data=payload)
        return json.loads(response.text)

    def selectStore(self, store):
        try:
            url = "https://yo-sushi-gourmet-gulf-co.iikoweb.co.uk/api/stores/select/"+store
            payload = {}
            headers = {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer '+str(self.token)
            }
            response = requests.request(
                "POST", url, headers=headers, data=payload)

            return response
        except Exception as err:
            logging.error('Error - '+str(err))

    def olap_SalesExport(self):
        # Add Prefix (SalesEntry_)
        selectStoreCookie = self.selectStore()

        url = "https://yo-sushi-gourmet-gulf-co.iikoweb.co.uk/api/olap/init"
        payload = "{\n    \"olapType\": \"SALES\",\n    \"categoryFields\": [],\n    \"groupFields\": [\n        \"UniqOrderId.Id\",\n        \"Department\",\n        \"Conception\",\n        \"OpenDate.Typed\",\n        \"OrderNum\",\n        \"Delivery.IsDelivery\",\n        \"OrderType\",\n        \"DishCategory.Accounting\",\n        \"DishCode\",\n        \"DishName\",\n        \"ItemSaleEventDiscountType\"\n    ],\n    \"stackByDataFields\": false,\n    \"dataFields\": [\n        \"DishSumInt\",\n        \"IncreaseSum\",\n        \"DiscountSum\",\n        \"DishDiscountSumInt\",\n        \"VAT.Sum\"\n    ],\n    \"calculatedFields\": [],\n    \"filters\": [\n        {\n            \"field\": \"OpenDate.Typed\",\n            \"filterType\": \"date_range\",\n            \"dateFrom\": \"2022-06-20T00:00:00\",\n            \"dateTo\": \"2022-06-21T00:00:00\",\n            \"valueMin\": null,\n            \"valueMax\": null,\n            \"valueList\": [],\n            \"includeLeft\": true,\n            \"includeRight\": false,\n            \"inclusiveList\": true\n        }\n    ],\n    \"includeVoidTransactions\": false,\n    \"includeNonBusinessPaymentTypes\": true\n}"
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer '+str(self.token),
        }
        response = requests.request(
            "POST", url, headers=headers, data=payload, cookies=selectStoreCookie.cookies.get_dict())
        return json.loads(response.text)

    def olap_ExportPayment(self, store):
        # Add Prefix (PaymentEntry_)
        try:
            selectStoreCookie = self.selectStore(store)

            url = "https://yo-sushi-gourmet-gulf-co.iikoweb.co.uk/api/olap/init"

            if self.configData['CustomDate']['status']:
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
                            "dateFrom": self.configData['CustomDate']['DateFrom'],
                            "dateTo": self.configData['CustomDate']['DateTo'],
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

            headers = {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer '+str(self.token),
            }
            response = requests.request(
                "POST", url, headers=headers, data=json.dumps(payload), cookies=selectStoreCookie.cookies.get_dict())

            return json.loads(response.text)

        except Exception as err:
            logging.error(
                'olap_ExportPayment - Error - ' + str(datetime.now().today()) + ' - ' + str(err))

    def olap_FetchStatus(self, dataId):

        try:
            url = "https://yo-sushi-gourmet-gulf-co.iikoweb.co.uk/api/olap/fetch-status/" + dataId

            payload = {}
            headers = {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer '+str(self.token),
            }
            response = requests.request(
                "GET", url, headers=headers, data=payload)
            return json.loads(response.text)

        except Exception as err:
            logging.error(
                'Error - ' + str(datetime.now().today()) + ' - ' + str(err))

    def FetchData(self):
        response = ''
        try:
            StoreData_df = pd.read_csv('Requirements/Data/StoreMasterv2,0.csv')
            StoreData_df.StoreId = StoreData_df.StoreId.astype(str)
            StoreData_dict = StoreData_df.to_dict(orient='records')

            for storeDetails in StoreData_dict:

                flag = True
                attempCounts = 0
                while flag:

                    dataId = self.olap_ExportPayment(storeDetails['StoreId'])
                    if dataId['error']:
                        logging.error(
                            'FetchData(olap_ExportPayment) - Error - ' + str(datetime.now().today()) + ' | ' + str(dataId))

                    status = self.olap_FetchStatus(dataId['data'])
                    if status['error']:
                        logging.error(
                            'FetchData(olap_FetchStatus) - Error - ' + str(datetime.now().today()) + ' | ' + str(status))

                    if status['data'] == 'SUCCESS':

                        url = "https://yo-sushi-gourmet-gulf-co.iikoweb.co.uk/api/olap/fetch/" + \
                            dataId['data']+"/json"

                        payload = "{\n    \"olapType\": \"SALES\",\n    \"categoryFields\": [],\n    \"groupFields\": [\n        \"UniqOrderId.Id\",\n        \"Department\",\n        \"Conception\",\n        \"OpenDate.Typed\",\n        \"OrderNum\",\n        \"Delivery.IsDelivery\",\n        \"OrderType\",\n        \"DishCategory.Accounting\",\n        \"DishCode\",\n        \"DishName\",\n        \"ItemSaleEventDiscountType\"\n    ],\n    \"stackByDataFields\": false,\n    \"dataFields\": [\n        \"DishSumInt\",\n        \"IncreaseSum\",\n        \"DiscountSum\",\n        \"DishDiscountSumInt\",\n        \"VAT.Sum\"\n    ],\n    \"calculatedFields\": [],\n    \"filters\": [\n        {\n            \"field\": \"OpenDate.Typed\",\n            \"filterType\": \"date_range\",\n            \"dateFrom\": \"2022-06-20T00:00:00\",\n            \"dateTo\": \"2022-06-21T00:00:00\",\n            \"valueMin\": null,\n            \"valueMax\": null,\n            \"valueList\": [],\n            \"includeLeft\": true,\n            \"includeRight\": false,\n            \"inclusiveList\": true\n        }\n    ],\n    \"includeVoidTransactions\": false,\n    \"includeNonBusinessPaymentTypes\": true\n}"
                        headers = {
                            'Content-Type': 'application/json',
                            'Authorization': 'Bearer '+str(self.token),
                        }

                        response = requests.request(
                            "POST", url, headers=headers, data=payload)

                        fetchData = json.loads(response.text)

                        if fetchData['error'] == False:
                            # StoreID_StoreCode_DateFrom_DateTo

                            fetchData_df = pd.DataFrame(
                                fetchData['result']['rawData'])

                            if not fetchData_df.empty:
                                paymentEntryETL = PaymentEntryETL()
                                paymentEntry_df = paymentEntryETL.transformData(
                                    fetchData_df, storeDetails['StoreId'])
                                paymentEntryETL.loadData(paymentEntry_df)

                        flag = False
                    else:
                        if attempCounts == self.configData['attempCounts']:
                            logging.warning(
                                'Warning - ' + str(datetime.now().today(
                                )) + ' - ' + status['data'] + ' - ' + storeDetails['StoreId'] + ' - ' + storeDetails['StoreCode'] + ' - ' + str(attempCounts)
                            )
                            flag = False

                        attempCounts += 1
                        time.sleep(self.configData['SleepTime'])

        except Exception as err:
            logging.error(
                'FetchData - Error - ' + str(datetime.now().today()) + ' - ' + str(err) + ' | ' + str(response))


class SalesEntry:

    def __init__(self):
        """
        GET CURRENT DIRECTORY
        """
        self.cwd = os.getcwd()

        """
        READ CONFIG FILE
        """
        f = open("config.json")
        self.configData = json.load(f)

        """
        Transform Current Date
        """
        self.previous_dateTime = date.today() - timedelta(days=1)
        self.previous_dateTime = str(
            self.previous_dateTime.strftime('%Y-%m-%dT%H:%M:%S'))

        self.DumpFolder_csv = self.configData['DumpFolder']['main'] + \
            self.configData['DumpFolder']['csv']
        self.DumpFolder_json = self.configData['DumpFolder']['main'] + \
            self.configData['DumpFolder']['json']
        self.SourceFolder = self.configData['SourceFolder']

        self.main_url = "https://yo-sushi-gourmet-gulf-co.iikoweb.co.uk"
        url = self.main_url+"/api/auth/login"
        payload = "{\n    \"login\": \"navision\",\n    \"password\": \"75KuJSiyXWrzJU1i\"\n}"
        headers = {}
        self.s = requests.Session()
        response = requests.request("POST", url, headers=headers, data=payload)

        response = json.loads(response.text)
        if response['error'] == False:
            self.token = response['token']
        else:
            logging.critical('Critical - ' + str(datetime.now().today()
                                                 ) + ' - ' + str(response['errorMessage']))

    def getStore(self):

        url = "https://yo-sushi-gourmet-gulf-co.iikoweb.co.uk/api/stores/list"
        payload = {}
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer '+str(self.token)
        }
        response = requests.request("GET", url, headers=headers, data=payload)
        return json.loads(response.text)

    def selectStore(self, store):
        try:
            url = "https://yo-sushi-gourmet-gulf-co.iikoweb.co.uk/api/stores/select/"+store
            payload = {}
            headers = {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer '+str(self.token)
            }
            response = requests.request(
                "POST", url, headers=headers, data=payload)

            return response
        except Exception as err:
            logging.error('Error - '+str(err))

    def olap_SalesExport(self):
        # Add Prefix (SalesEntry_)
        selectStoreCookie = self.selectStore()

        url = "https://yo-sushi-gourmet-gulf-co.iikoweb.co.uk/api/olap/init"
        payload = "{\n    \"olapType\": \"SALES\",\n    \"categoryFields\": [],\n    \"groupFields\": [\n        \"UniqOrderId.Id\",\n        \"Department\",\n        \"Conception\",\n        \"OpenDate.Typed\",\n        \"OrderNum\",\n        \"Delivery.IsDelivery\",\n        \"OrderType\",\n        \"DishCategory.Accounting\",\n        \"DishCode\",\n        \"DishName\",\n        \"ItemSaleEventDiscountType\"\n    ],\n    \"stackByDataFields\": false,\n    \"dataFields\": [\n        \"DishSumInt\",\n        \"IncreaseSum\",\n        \"DiscountSum\",\n        \"DishDiscountSumInt\",\n        \"VAT.Sum\"\n    ],\n    \"calculatedFields\": [],\n    \"filters\": [\n        {\n            \"field\": \"OpenDate.Typed\",\n            \"filterType\": \"date_range\",\n            \"dateFrom\": \"2022-06-20T00:00:00\",\n            \"dateTo\": \"2022-06-21T00:00:00\",\n            \"valueMin\": null,\n            \"valueMax\": null,\n            \"valueList\": [],\n            \"includeLeft\": true,\n            \"includeRight\": false,\n            \"inclusiveList\": true\n        }\n    ],\n    \"includeVoidTransactions\": false,\n    \"includeNonBusinessPaymentTypes\": true\n}"
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer '+str(self.token),
        }
        response = requests.request(
            "POST", url, headers=headers, data=payload, cookies=selectStoreCookie.cookies.get_dict())
        return json.loads(response.text)

    def olap_SalesEntry(self, store):
        # Add Prefix (SalesEntry_)
        try:
            selectStoreCookie = self.selectStore(store)

            url = "https://yo-sushi-gourmet-gulf-co.iikoweb.co.uk/api/olap/init"

            if self.configData['CustomDate']['status']:
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
                        "OrderType",
                        "DishCategory.Accounting",
                        "DishCode",
                        "ItemSaleEventDiscountType"
                    ],
                    "stackByDataFields": False,
                    "dataFields": [
                        "DishSumInt",
                        "DiscountSum",
                        "IncreaseSum",
                        "DishDiscountSumInt",
                        "VAT.Sum",
                        "DishDiscountSumInt.withoutVAT"
                    ],
                    "calculatedFields": [
                    ],
                    "filters": [
                        {
                            "field": "OpenDate.Typed",
                            "filterType": "date_range",
                            "dateFrom": self.configData['CustomDate']['DateFrom'],
                            "dateTo": self.configData['CustomDate']['DateTo'],
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
                        "OrderType",
                        "DishCategory.Accounting",
                        "DishCode",
                        "ItemSaleEventDiscountType"
                    ],
                    "stackByDataFields": False,
                    "dataFields": [
                        "DishSumInt",
                        "DiscountSum",
                        "IncreaseSum",
                        "DishDiscountSumInt",
                        "VAT.Sum",
                        "DishDiscountSumInt.withoutVAT"
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

            headers = {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer '+str(self.token),
            }
            response = requests.request(
                "POST", url, headers=headers, data=json.dumps(payload), cookies=selectStoreCookie.cookies.get_dict())

            return json.loads(response.text)

        except Exception as err:
            logging.error(
                'olap_SalesEntry - Error - ' + str(datetime.now().today()) + ' - ' + str(err))

    def olap_FetchStatus(self, dataId):

        try:
            url = "https://yo-sushi-gourmet-gulf-co.iikoweb.co.uk/api/olap/fetch-status/" + dataId

            payload = {}
            headers = {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer '+str(self.token),
            }
            response = requests.request(
                "GET", url, headers=headers, data=payload)
            return json.loads(response.text)

        except Exception as err:
            logging.error(
                'Error - ' + str(datetime.now().today()) + ' - ' + str(err))

    def FetchData(self):
        response = ''
        try:
            StoreData_df = pd.read_csv('Requirements/Data/StoreMasterv2,0.csv')
            StoreData_df.StoreId = StoreData_df.StoreId.astype(str)
            StoreData_dict = StoreData_df.to_dict(orient='records')

            for storeDetails in StoreData_dict:

                flag = True
                attempCounts = 0
                while flag:

                    dataId = self.olap_SalesEntry(storeDetails['StoreId'])

                    if dataId['error']:
                        logging.error(
                            'FetchData(olap_SalesEntry) - Error - ' + str(datetime.now().today()) + ' | ' + str(dataId))

                    status = self.olap_FetchStatus(dataId['data'])
                    if status['error']:
                        logging.error(
                            'FetchData(olap_FetchStatus) - Error - ' + str(datetime.now().today()) + ' | ' + str(status))

                    if status['data'] == 'SUCCESS':

                        url = "https://yo-sushi-gourmet-gulf-co.iikoweb.co.uk/api/olap/fetch/" + \
                            dataId['data']+"/json"

                        payload = "{\n    \"olapType\": \"SALES\",\n    \"categoryFields\": [],\n    \"groupFields\": [\n        \"UniqOrderId.Id\",\n        \"Department\",\n        \"Conception\",\n        \"OpenDate.Typed\",\n        \"OrderNum\",\n        \"Delivery.IsDelivery\",\n        \"OrderType\",\n        \"DishCategory.Accounting\",\n        \"DishCode\",\n        \"DishName\",\n        \"ItemSaleEventDiscountType\"\n    ],\n    \"stackByDataFields\": false,\n    \"dataFields\": [\n        \"DishSumInt\",\n        \"IncreaseSum\",\n        \"DiscountSum\",\n        \"DishDiscountSumInt\",\n        \"VAT.Sum\"\n    ],\n    \"calculatedFields\": [],\n    \"filters\": [\n        {\n            \"field\": \"OpenDate.Typed\",\n            \"filterType\": \"date_range\",\n            \"dateFrom\": \"2022-06-20T00:00:00\",\n            \"dateTo\": \"2022-06-21T00:00:00\",\n            \"valueMin\": null,\n            \"valueMax\": null,\n            \"valueList\": [],\n            \"includeLeft\": true,\n            \"includeRight\": false,\n            \"inclusiveList\": true\n        }\n    ],\n    \"includeVoidTransactions\": false,\n    \"includeNonBusinessPaymentTypes\": true\n}"
                        headers = {
                            'Content-Type': 'application/json',
                            'Authorization': 'Bearer '+str(self.token),
                        }

                        response = requests.request(
                            "POST", url, headers=headers, data=payload)

                        fetchData = json.loads(response.text)

                        if fetchData['error'] == False:
                            # StoreID_StoreCode_DateFrom_DateTo
                            pass

                        flag = False
                    else:
                        if attempCounts == self.configData['attempCounts']:
                            # Status, StoreID, StoreCode, timestamp, errorMessage, attempCounts
                            logging.warning(
                                'Warning - ' + str(datetime.now().today(
                                )) + ' - ' + status['data'] + ' - ' + storeDetails['StoreId'] + ' - ' + storeDetails['StoreCode'] + ' - ' + str(attempCounts)
                            )
                            flag = False

                        attempCounts += 1
                        time.sleep(self.configData['SleepTime'])

        except Exception as err:
            logging.error(
                'FetchData(salesEntry) - Error - ' + str(datetime.now().today()) + ' - ' + str(err) + ' | ' + str(response))


class PaymentEntryETL:

    def __init__(self):
        """
        CONNECTING DATABASE
        """
        self.databaseConnection = DatabaseConnection()
        self.conn = self.databaseConnection.conn
        self.cursor = self.databaseConnection.cursor

    def transformData(self, paymentEntry, storeID):

        # paymentEntry['uid'] = paymentEntry['Department'] + paymentEntry['OpenDate.Typed'] + \
        #     paymentEntry['PayTypes'] + paymentEntry['UniqOrderId.Id']

        paymentEntry['storeID'] = storeID

        paymentEntry['OrderNum'] = paymentEntry['OrderNum'].astype('str')

        paymentEntry['uid'] = paymentEntry['UniqOrderId.Id'] + \
            '-' + paymentEntry['PayTypes']

        paymentEntry['OrderNum'] = paymentEntry['OrderNum'].astype('int')
        print(paymentEntry['uid'], storeID)
        paymentEntry = paymentEntry[['uid', 'Conception', 'CreditUser', 'Delivery.IsDelivery', 'Delivery.SourceKey',
                                     'Department', 'DishDiscountSumInt', 'ExternalNumber', 'OpenDate.Typed',
                                     'OrderNum', 'OrderType', 'PayTypes', 'UniqOrderId.Id', 'storeID']]

        paymentEntry.fillna("", inplace=True)

        extractData = pd.read_sql_query(
            "SELECT * FROM `rawDataTest`.`paymentEntry`", self.conn)

        if not extractData.empty:
            duplicateRecord = pd.merge(
                paymentEntry, extractData.uid, how='inner')

            duplicateRecord.to_csv(duplicateRecord.uid+".csv")
            paymentEntry = paymentEntry[~paymentEntry.uid.isin(
                list(duplicateRecord.uid))]

        return paymentEntry

    def loadData(self, paymentEntry_df):

        try:
            paymentEntry_df_to_list = list(tuple(row)
                                           for row in paymentEntry_df.values)
            insert_query = "INSERT INTO `rawDataTest`.`paymentEntry` (`uid`, `conception`, `creditUser`, `isDelivery`, `sourceKey`, `department`, `dishDiscountSumInt`, `externalNumber`, `openDate`, `orderNum`, `orderType`, `payTypes`, `uniqOrderId`, `storeID`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            self.cursor.executemany(insert_query, paymentEntry_df_to_list)
            self.conn.commit()
            print("INSERTED INTO Payment Entry")
        except Exception as e:
            print(str(e))


class SalesEntryETL:

    def __init__(self):
        """
        CONNECTING DATABASE
        """
        self.databaseConnection = DatabaseConnection()
        self.conn = self.databaseConnection.conn
        self.cursor = self.databaseConnection.cursor

    def transformData(self):
        pass

    def loadData(self):
        pass


@flow(name="PaymentEntry ETL", task_runner=DaskTaskRunner)
def paymentETL():
    paymentEntry = PaymentEntry()
    paymentEntry.FetchData()


if __name__ == "__main__":
    paymentETL()
