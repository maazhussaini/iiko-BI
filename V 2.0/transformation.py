import pandas as pd
import numpy as np
import mysql.connector as sql
import json


def DatabaseConnection():
    f = open("config.json")
    configData = json.load(f)
    conn = sql.connect(host=configData['dbConnection']['host'],
                       user=configData['dbConnection']['user'],
                       password=configData['dbConnection']['password'],
                       database=configData['dbConnection']['dbName'])

    return conn


conn = DatabaseConnection()
cursor = conn.cursor()


class TransformationRawData:

    def __init__(self):
        f = open("config.json")
        self.configData = json.load(f)

    def transformHeader(self):

        query = f"""
            with rawHeaderFetchData as (
            SELECT 
                pk_rawHeader as fk_rawHeader, uniqueOrderId, store, conception, DATE_FORMAT(accountingDay,'%d/%m/%Y') as accountingDate, 
                receiptNo, delivery, GrossSalesBD, grossSalesAD, discountAmount, surchargeAmount,
                (GrossSalesBD - grossSalesAD) as effectiveDiscount, VATByBillAmount as VAT, NetSalesAD as NetSales,
                numberOfGuests as NoOfCovers, (GrossSalesBD - VATByBillAmount) as GGGross,
                SUBSTRING_INDEX(openingTime, 'T', -1) as OpeningTimeFormated, openingTime, tableNo, shiftNo, deliveryType, 
                section, externalOrderNo, customerPhoneNumber, customerFullName, customerPriceCategory,
                deliveryComment, waiterForTheOrder as orderWaiter, deliveryPrintTime, plannedDeliveryTime,
                deliveryDispatchTime, actualDeliveryTime, deliveryClosingTime, 
                SUBSTRING_INDEX(guestBillTime, 'T', -1) as GuestBillTimeFormated, guestBillTime,
                deliveryInvoicePrintTime, 
                SUBSTRING_INDEX(closingTime, 'T', -1) as ClosingTimeFormated, closingTime, 
                numberOfIitems, numberOfDiscountedItems, numberOfComboDiscounts, averageRevenuePerGuest,
                lastServPrintTime, 'iiko' as DataSource, AvgCookingTime, AvgCookingTime1, AvgCookingTime2,
                AvgCookingTime3, AvgCookingTime4, AverageCookingDelay, AverageServingTime, 
                AverageWaitingTimeToBeServed, AverageServingDelay,
                CASE
                    WHEN (deliverySource != '') and (deliverySource != 'CloudCallCenter')
                        Then deliverySource
                    else 
                        orderType
                END AS RefinedOrderSourceBM,
                CASE
                    WHEN deliverySource = 'CloudCallCenter'
                        THEN 'CC'
                    ELSE
                        'NO'
                END AS OriginatedCCYN
            FROM `rawHeader` WHERE orderDeleted = 'NOT_DELETED' and store != 'Test Lab' and transformFlag = 0
            LIMIT {self.configData['LIMIT_SQL']}
        )

        SELECT 
            rawHeaderFetchData.*, 
            
            iikoRefinedOrderSourceTransform.RefinedOrderSource as RefinedOrderSourceAM,
            CASE
                WHEN iikoRefinedOrderSourceTransform.RefinedOrderSource is NULL
                THEN rawHeaderFetchData.RefinedOrderSourceBM
                ELSE
                    iikoRefinedOrderSourceTransform.RefinedOrderSource
            END as RefinedOrderSource,
            
            iikoIsDeliveryMapping.deliveryNo as DeliveryYN,
            iikoVirtualStoreMapping.conceptionNo as brand, 
            iikoVirtualStoreMapping.storeNo,
            iikoVirtualStoreMapping.virtualStoreNo,
            iikoVirtualStoreMapping.virtualStoreName as virtualStoreName,
            iikoVirtualStoreMapping.financeStoreNo, iikoIsDeliveryMapping.deliveryNo,
            iikoVirtualStoreMapping.regionCode as region,
            CASE
                WHEN iikoVirtualStoreMapping.regionCode = CurrencyMappingSheet.region
                    THEN rawHeaderFetchData.GrossSalesBD * CurrencyMappingSheet.factor
            END AS GrossSalesBD_BC,
            CASE
                WHEN iikoVirtualStoreMapping.regionCode = CurrencyMappingSheet.region
                    THEN rawHeaderFetchData.grossSalesAD * CurrencyMappingSheet.factor
            END AS grossSalesAD_BC,
            CASE
                WHEN iikoVirtualStoreMapping.regionCode = CurrencyMappingSheet.region
                    THEN rawHeaderFetchData.discountAmount * CurrencyMappingSheet.factor
            END AS discountAmount_BC,
            CASE
                WHEN iikoVirtualStoreMapping.regionCode = CurrencyMappingSheet.region
                    THEN rawHeaderFetchData.surchargeAmount * CurrencyMappingSheet.factor
            END AS surchargeAmount_BC,
            CASE
                WHEN iikoVirtualStoreMapping.regionCode = CurrencyMappingSheet.region
                    THEN rawHeaderFetchData.effectiveDiscount * CurrencyMappingSheet.factor
            END AS effectiveDiscount_BC,
            CASE
                WHEN iikoVirtualStoreMapping.regionCode = CurrencyMappingSheet.region
                    THEN rawHeaderFetchData.VAT * CurrencyMappingSheet.factor
            END AS VAT_BC,
            CASE
                WHEN iikoVirtualStoreMapping.regionCode = CurrencyMappingSheet.region
                    THEN rawHeaderFetchData.NetSales * CurrencyMappingSheet.factor
            END AS NetSales_BC,
            CASE
                WHEN iikoVirtualStoreMapping.regionCode = CurrencyMappingSheet.region
                    THEN rawHeaderFetchData.GGGross * CurrencyMappingSheet.factor
            END AS GGGross_BC,
            CASE
                WHEN iikoVirtualStoreMapping.regionCode = CurrencyMappingSheet.region
                    THEN rawHeaderFetchData.averageRevenuePerGuest * CurrencyMappingSheet.factor
            END AS averageRevenuePerGuest_BC
            
        from rawHeaderFetchData 
        inner join `iikoVirtualStoreMapping` as iikoVirtualStoreMapping
        on (rawHeaderFetchData.store = iikoVirtualStoreMapping.storeName 
            and rawHeaderFetchData.conception = iikoVirtualStoreMapping.conceptionName)
        inner join `iikoIsDeliveryMapping` as iikoIsDeliveryMapping
        on rawHeaderFetchData.delivery = iikoIsDeliveryMapping.deliveryName
        LEFT join `iikoRefinedOrderSourceTransform` as iikoRefinedOrderSourceTransform
        on iikoRefinedOrderSourceTransform.SourceKey = rawHeaderFetchData.RefinedOrderSourceBM
        INNER JOIN `CurrencyMappingSheet` as CurrencyMappingSheet
        on iikoVirtualStoreMapping.regionCode = CurrencyMappingSheet.region;
        """

        rawHeaderTransformData = pd.read_sql(query, conn)
        rawHeaderTransformData['salesTransformed'] = False
        rawHeaderTransformData['paymentTransformed'] = False

        rawHeaderTransformData = rawHeaderTransformData[[
            'fk_rawHeader', 'uniqueOrderId', 'storeNo', 'brand', 'virtualStoreNo', 'virtualStoreName',
            'financeStoreNo', 'accountingDate', 'receiptNo', 'DeliveryYN', 'RefinedOrderSource', 'OriginatedCCYN',
            'GrossSalesBD', 'discountAmount', 'surchargeAmount', 'grossSalesAD', 'effectiveDiscount', 'VAT',
            'NetSales', 'NoOfCovers', 'GGGross', 'region',

            'OpeningTimeFormated', 'openingTime',

            'tableNo', 'shiftNo', 'deliveryType',
            'section', 'externalOrderNo', 'customerPhoneNumber', 'customerFullName', 'deliveryComment', 'orderWaiter',
            'deliveryPrintTime', 'plannedDeliveryTime', 'deliveryDispatchTime', 'actualDeliveryTime',
            'deliveryClosingTime',

            'GuestBillTimeFormated', 'guestBillTime',

            'deliveryInvoicePrintTime',

            'ClosingTimeFormated', 'closingTime',

            'customerPriceCategory',
            'numberOfIitems', 'numberOfDiscountedItems', 'numberOfComboDiscounts', 'averageRevenuePerGuest',
            'lastServPrintTime',

            'DataSource', 'AvgCookingTime', 'AvgCookingTime1', 'AvgCookingTime2',
            'AvgCookingTime3', 'AvgCookingTime4', 'AverageCookingDelay', 'AverageServingTime', 'AverageWaitingTimeToBeServed',
            'AverageServingDelay',

            'GrossSalesBD_BC', 'discountAmount_BC', 'surchargeAmount_BC', 'grossSalesAD_BC',
            'effectiveDiscount_BC', 'VAT_BC', 'NetSales_BC', 'GGGross_BC', 'averageRevenuePerGuest_BC',

            'salesTransformed', 'paymentTransformed'
        ]]

        rawHeaderTransformData_toList = list(tuple(row)
                                             for row in rawHeaderTransformData.values)

        insertQuery = """ INSERT INTO `transformHeader`
                        (
                        `fk_rawHeader`,
                        `uniqueOrderId`,
                        `store`,
                        `brand`,
                        `virtualStoreNo`,
                        `virtualStoreName`,
                        `financeStoreNo`,
                        `accountingDate`,
                        `receiptNo`,
                        `deliveryNo`,
                        `RefinedOrderSource`,
                        `OriginatedCCYN`,
                        `GrossSalesBD`,
                        `discountAmount`,
                        `surchargeAmount`,
                        `grossSalesAD`,
                        `effectiveDiscount`,
                        `VAT`,
                        `NetSales`,
                        `NoOfCovers`,
                        `GGGross`,
                        `region`,
                        `OpeningTimeFormated`,
                        `openingTime`,
                        `tableNo`,
                        `shiftNo`,
                        `deliveryType`,
                        `section`,
                        `externalOrderNo`,
                        `customerPhoneNumber`,
                        `customerFullName`,
                        `deliveryComment`,
                        `orderWaiter`,
                        `deliveryPrintTime`,
                        `plannedDeliveryTime`,
                        `deliveryDispatchTime`,
                        `actualDeliveryTime`,
                        `deliveryClosingTime`,
                        `GuestBillTimeFormated`,
                        `guestBillTime`,
                        `deliveryInvoicePrintTime`,
                        `ClosingTimeFormated`,
                        `closingTime`,
                        `customerPriceCategory`,
                        `numberOfIitems`,
                        `numberOfDiscountedItems`,
                        `numberOfComboDiscounts`,
                        `averageRevenuePerGuest`,
                        `lastServPrintTime`,
                        `DataSource`,
                        `AvgCookingTime`,
                        `AvgCookingTime1`,
                        `AvgCookingTime2`,
                        `AvgCookingTime3`,
                        `AvgCookingTime4`,
                        `AverageCookingDelay`,
                        `AverageServingTime`,
                        `AverageWaitingTimeToBeServed`,
                        `AverageServingDelay`,
                        `GrossSalesBD_BC`,
                        `discountAmount_BC`,
                        `surchargeAmount_BC`,
                        `grossSalesAD_BC`,
                        `effectiveDiscount_BC`,
                        `VAT_BC`,
                        `NetSales_BC`,
                        `GGGross_BC`,
                        `averageRevenuePerGuest_BC`,
                        `salesTransformed`,
                        `paymentTransformed`
                    )
                VALUES
                (%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        cursor.executemany(insertQuery, rawHeaderTransformData_toList)
        conn.commit()
        print("INSERTED INTO transform Header")

        updateQuery = f"""
            UPDATE `rawHeader` SET transformFlag = True
            WHERE uniqueOrderId IN {tuple(rawHeaderTransformData.uniqueOrderId)};
        """
        cursor.execute(updateQuery)
        conn.commit()

        print("Update INTO raw Header's transformFlag")

    def transformPayment(self):
        query = """
            with rawPayment as (SELECT 
                uniqueOrderId, paymentType, DATE_FORMAT(accountingDay,'%d/%m/%Y') as accountingDate,
                paymentTypeSplit, paymentCardNo as paymentCardNo, 
                paymentCurrency as paymentCurrency, nonCashPaymentType as nonCashPaymentType, 
                creditedTo as creditedTo_orginal, paymentExchangeRateAED as exchangeRateAED,
                authorisedBy as authorisedBy, authorisedByID as authorisedByID, 
                cashier as cashier, cashierID as cashierID, grossSalesAD as grossSalesAD, 
                VATByBillAmount as VAT, netSalesAD as netSales, numberOfVouchers as numberOfVouchers
            FROM `rawPayment`)

            SELECT 
                rawPayment.*,
                transformHeader.store,
                transformHeader.receiptNo,
                transformHeader.region,
                CASE
                    WHEN rawPayment.creditedTo_orginal = ''THEN 
                        CASE
                            WHEN (rawPayment.paymentType = 'Aggregator Payment') or (rawPayment.paymentType = 'Aggregator Payment old') THEN
                                transformHeader.RefinedOrderSource
                            ELSE
                                ''
                        END
                    ELSE
                        rawPayment.creditedTo_orginal
                END As creditedTo,
                
                CASE
                    WHEN transformHeader.region = CurrencyMappingSheet.region
                        THEN rawPayment.grossSalesAD * CurrencyMappingSheet.factor
                END AS grossSalesAD_BC,
                CASE
                    WHEN transformHeader.region = CurrencyMappingSheet.region
                        THEN rawPayment.VAT * CurrencyMappingSheet.factor
                END AS VAT_BC,
                CASE
                    WHEN transformHeader.region = CurrencyMappingSheet.region
                        THEN rawPayment.netSales * CurrencyMappingSheet.factor
                END AS netSales_BC

            FROM rawPayment 
            INNER JOIN `transformHeader` as transformHeader 
                on rawPayment.uniqueOrderId = transformHeader.uniqueOrderId
            INNER JOIN `CurrencyMappingSheet` as CurrencyMappingSheet
                on transformHeader.region = CurrencyMappingSheet.region
            WHERE transformHeader.paymentTransformed = 0;
        """

        transformPaymentFetchData = pd.read_sql(query, conn)

        transformPaymentFetchData = transformPaymentFetchData[
            [
                'uniqueOrderId', 'paymentType', 'accountingDate', 'paymentTypeSplit', 'paymentCardNo',
                'paymentCurrency', 'nonCashPaymentType', 'creditedTo', 'exchangeRateAED', 'authorisedBy',
                'authorisedByID', 'cashier', 'cashierID', 'grossSalesAD', 'VAT', 'netSales', 'numberOfVouchers',
                'store', 'receiptNo', 'region', 'grossSalesAD_BC', 'VAT_BC', 'netSales_BC'
            ]]

        transformPaymentFetchData_toList = list(tuple(row)
                                                for row in transformPaymentFetchData.values)

        insert_Query = """
            INSERT INTO `rawDataTestV2`.`transformPayment`
            (
                `uniqueOrderId`,
                `paymentType`,
                `accountingDate`,
                `paymentTypeSplit`,
                `paymentCardNo`,
                `paymentCurrency`,
                `nonCashPaymentType`,
                `creditedTo`,
                `exchangeRateAED`,
                `authorisedBy`,
                `authorisedByID`,
                `cashier`,
                `cashierID`,
                `grossSalesAD`,
                `VAT`,
                `netSales`,
                `numberOfVouchers`,
                `storeNo`,
                `receiptNo`,
                `region`,
                `grossSalesAD_BC`,
                `VAT_BC`,
                `netSales_BC`
            )
                VALUES
                (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        cursor.executemany(insert_Query, transformPaymentFetchData_toList)
        conn.commit()
        print("INSERTED INTO transform Payment")

        updateQuery = f"""
            UPDATE `transformHeader` SET paymentTransformed = True
            WHERE uniqueOrderId IN {tuple(transformPaymentFetchData.uniqueOrderId)};
        """
        cursor.execute(updateQuery)
        conn.commit()

        print("Update INTO transform Header's paymentTransformed")

    def transformSales(self):
        query = """
            with rawSalesFetchData as (
            SELECT 
                uniqueOrderId, CAST(DATE_FORMAT(accountingDay,'%d/%m/%Y') as CHAR) as accountingDate, receiptNo,
                itemCode, itemId, 
                TRIM(SUBSTRING_INDEX(item, '/', 1)) as itemName, 
                itemFullName, itemAccountingCategory, itemCategory, itemGroup, level1_itemGroup, level2_itemGroup, level3_itemGroup,
                courseNo, itemWaiter, itemServicePrinting, itemComment, soldWithItem, discountType, surchargeType,
                nameOfDiscountOrSurcharge,
                cookingFinishedAt, numberOfItems, averagePriceBeforeDiscount, averagePrice, grossSalesBD, 
                discountAmount, surchargeAmountAED, grossSalesAD, (grossSalesBD - grossSalesAD) as effectiveDiscount,
                VATByBillAmount as VAT, netSalesAD as netSales, cookingTimeAverage, cookingTimeAverage1, cookingTimeAverage2,
                cookingTimeAverage3, cookingTimeAverage4
            FROM `rawSales`
            WHERE rawSales.itemDeleted = 'NOT_DELETED'
        )

        SELECT 
            rawSales.*, 
            transformHeader.store,
            transformHeader.brand,
            transformHeader.region,
            CASE
                WHEN transformHeader.region = CurrencyMappingSheet.region
                    THEN rawSales.averagePriceBeforeDiscount * CurrencyMappingSheet.factor
            END AS averagePriceBeforeDiscount_BC,
            CASE
                WHEN transformHeader.region = CurrencyMappingSheet.region
                    THEN rawSales.averagePrice * CurrencyMappingSheet.factor
            END AS averagePrice_BC,
            CASE
                WHEN transformHeader.region = CurrencyMappingSheet.region
                    THEN rawSales.GrossSalesBD * CurrencyMappingSheet.factor
            END AS GrossSalesBD_BC,
            CASE
                WHEN transformHeader.region = CurrencyMappingSheet.region
                    THEN rawSales.discountAmount * CurrencyMappingSheet.factor
            END AS discountAmount_BC,
            CASE
                WHEN transformHeader.region = CurrencyMappingSheet.region
                    THEN rawSales.surchargeAmountAED * CurrencyMappingSheet.factor
            END AS surchargeAmountAED_BC,
            CASE
                WHEN transformHeader.region = CurrencyMappingSheet.region
                    THEN rawSales.grossSalesAD * CurrencyMappingSheet.factor
            END AS grossSalesAD_BC,
            CASE
                WHEN transformHeader.region = CurrencyMappingSheet.region
                    THEN rawSales.effectiveDiscount * CurrencyMappingSheet.factor
            END AS effectiveDiscount_BC,
            CASE
                WHEN transformHeader.region = CurrencyMappingSheet.region
                    THEN rawSales.VAT * CurrencyMappingSheet.factor
            END AS VAT_BC,
            CASE
                WHEN transformHeader.region = CurrencyMappingSheet.region
                    THEN rawSales.netSales * CurrencyMappingSheet.factor
            END AS netSales_BC

        FROM rawSalesFetchData as rawSales

        INNER JOIN `transformHeader` as transformHeader
        ON transformHeader.uniqueOrderId = rawSales.uniqueOrderId
        INNER JOIN `CurrencyMappingSheet` as CurrencyMappingSheet
        on transformHeader.region = CurrencyMappingSheet.region
        WHERE transformHeader.salesTransformed = 0;
        """

        transformSalesFetchData = pd.read_sql(query, conn)

        transformSalesFetchData['accountingDate'] = transformSalesFetchData.accountingDate.astype(
            "str")
        transformSalesFetchData_toList = list(tuple(row)
                                              for row in transformSalesFetchData.values)

        insert_Query = """
            INSERT INTO `transformSales`
            (`uniqueOrderId`,
            `accountingDate`,
            `receiptNo`,
            `itemCode`,
            `itemId`,
            `itemName`,
            `itemFullName`,
            `itemAccountingCategory`,
            `itemCategory`,
            `itemGroup`,
            `level1_itemGroup`,
            `level2_itemGroup`,
            `level3_itemGroup`,
            `courseNo`,
            `itemWaiter`,
            `itemServicePrinting`,
            `itemComment`,
            `soldWithItem`,
            `discountType`,
            `surchargeType`,
            `nameOfDiscountOrSurcharge`,
            `cookingFinishedAt`,
            `numberOfItems`,
            `averagePriceBeforeDiscount`,
            `averagePrice`,
            `grossSalesBD`,
            `discountAmount`,
            `surchargeAmountAED`,
            `grossSalesAD`,
            `effectiveDiscount`,
            `VAT`,
            `netSales`,
            `cookingTimeAverage`,
            `cookingTimeAverage1`,
            `cookingTimeAverage2`,
            `cookingTimeAverage3`,
            `cookingTimeAverage4`,
            `storeNo`,
            `brand`,
            `region`,
            `averagePriceBeforeDiscount_BC`,
            `averagePrice_BC`,
            `GrossSalesBD_BC`,
            `discountAmount_BC`,
            `surchargeAmountAED_BC`,
            `grossSalesAD_BC`,
            `effectiveDiscount_BC`,
            `VAT_BC`,
            `netSales_BC`
            )
            VALUES
            ( %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        cursor.executemany(insert_Query, transformSalesFetchData_toList)
        conn.commit()
        print("INSERTED INTO transform Sales")

        updateQuery = f"""
            UPDATE `transformHeader` SET salesTransformed = True
             WHERE uniqueOrderId IN {tuple(transformSalesFetchData.uniqueOrderId)};
        """
        cursor.execute(updateQuery)
        conn.commit()
        print("Update INTO transform Header's salesTransformed")


transformationRawData = TransformationRawData()
transformationRawData.transformHeader()
transformationRawData.transformPayment()
transformationRawData.transformSales()
