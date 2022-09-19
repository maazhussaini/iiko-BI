import pandas as pd
import numpy as np
import mysql.connector as sql
import json


def DatabaseConnection():
    conn = sql.connect(host="rawiikodata.cpi0e90jeqda.us-east-2.rds.amazonaws.com",
                            user="admin", password="MySql12$34#", database="rawDataTest")

    return conn


conn = DatabaseConnection()
cursor = conn.cursor()


class TransformationRawData:

    def transformHeader(self):

        query = """
            with rawHeaderFetchData as (
                SELECT
                    pk_rawHeader as fk_rawHeader, uniqueOrderId, store, conception, DATE_FORMAT(accountingDay,'%d/%m/%Y') as accountingDay,
                    receiptNo, orderDeleted, delivery, GrossSalesBD, grossSalesAD, discountAmount, surchargeAmount,
                    (GrossSalesBD - grossSalesAD) as effectiveDiscount, VATByBillAmount as VAT, NetSalesAD as NetSales,
                    numberOfGuests as NoOfCovers, (GrossSalesBD - VATByBillAmount) as GGGross,
                    SUBSTRING_INDEX(openingTime, 'T', -1) as OpeningTime, tableNo, shiftNo, deliveryType,
                    section, externalOrderNo, customerPhoneNumber, customerFullName, customerPriceCategory,
                    deliveryComment, waiterForTheOrder as orderWaiter, deliveryPrintTime, plannedDeliveryTime,
                    deliveryDispatchTime, actualDeliveryTime, deliveryClosingTime, guestBillTime, deliveryInvoicePrintTime,
                    closingTime, numberOfIitems, numberOfDiscountedItems, numberOfComboDiscounts, averageRevenuePerGuest,
                    lastServPrintTime,
                    CASE
                        WHEN (deliverySource != '') and (
                            deliverySource != 'CloudCallCenter')
                            Then deliverySource
                        else
                            orderType
                    END AS RefinedOrderSource,
                    CASE
                        WHEN deliverySource = 'CloudCallCenter'
                            THEN 'CC'
                        ELSE
                            'NO'
                    END AS OriginatedCCYN
                FROM `rawDataTest`.`rawHeader` WHERE orderDeleted = 'NOT_DELETED' and store != 'Test Lab'
            )

            SELECT 
                rawHeaderFetchData.fk_rawHeader, rawHeaderFetchData.uniqueOrderId, rawHeaderFetchData.store, 
                rawHeaderFetchData.conception, rawHeaderFetchData.accountingDay, rawHeaderFetchData.receiptNo, 
                rawHeaderFetchData.orderDeleted, rawHeaderFetchData.delivery, rawHeaderFetchData.GrossSalesBD, 
                rawHeaderFetchData.grossSalesAD, rawHeaderFetchData.discountAmount, rawHeaderFetchData.surchargeAmount,
                rawHeaderFetchData.effectiveDiscount, rawHeaderFetchData.VAT, rawHeaderFetchData.NetSales, 
                rawHeaderFetchData.NoOfCovers, rawHeaderFetchData.GGGross, rawHeaderFetchData.OpeningTime, rawHeaderFetchData.tableNo, 
                rawHeaderFetchData.shiftNo, rawHeaderFetchData.deliveryType, rawHeaderFetchData.section, 
                rawHeaderFetchData.externalOrderNo, rawHeaderFetchData.customerPhoneNumber, 
                rawHeaderFetchData.customerFullName, rawHeaderFetchData.customerPriceCategory, rawHeaderFetchData.deliveryComment, 
                rawHeaderFetchData.orderWaiter, rawHeaderFetchData.deliveryPrintTime, rawHeaderFetchData.plannedDeliveryTime,
                rawHeaderFetchData.deliveryDispatchTime, rawHeaderFetchData.actualDeliveryTime, 
                rawHeaderFetchData.deliveryClosingTime, rawHeaderFetchData.guestBillTime, rawHeaderFetchData.deliveryInvoicePrintTime,
                rawHeaderFetchData.closingTime, rawHeaderFetchData.numberOfIitems, 
                rawHeaderFetchData.numberOfDiscountedItems, rawHeaderFetchData.numberOfComboDiscounts, 
                rawHeaderFetchData.averageRevenuePerGuest, rawHeaderFetchData.lastServPrintTime, rawHeaderFetchData.OriginatedCCYN,
                CASE
                    WHEN rawHeaderFetchData.RefinedOrderSource = 'CALL CENTER'
                        THEN 'CALL'
                    ELSE
                        rawHeaderFetchData.RefinedOrderSource
                END as RefinedOrderSource,

                iikoStoreMapping.storeNo, iikoBrandMapping.conceptionNo,
                iikoVirtualStoreMapping.storeNo as virtualStoreNo, iikoVirtualStoreMapping.virtualStoreName,
                iikoVirtualStoreMapping.financeStoreNo, iikoIsDeliveryMapping.deliveryNo, iikoStoreMapping.regionCode

            from rawHeaderFetchData
                inner join `rawDataTest`.`iikoStoreMapping` as iikoStoreMapping
                    on rawHeaderFetchData.store = iikoStoreMapping.storeName
                inner join `rawDataTest`.`iikoBrandMapping` as iikoBrandMapping
                    on rawHeaderFetchData.conception = iikoBrandMapping.conceptionName
                inner join `rawDataTest`.`iikoVirtualStoreMapping` as iikoVirtualStoreMapping
                    on (
                            rawHeaderFetchData.store = iikoVirtualStoreMapping.storeName
                                and
                            rawHeaderFetchData.conception = iikoVirtualStoreMapping.conceptionName
                        )
                inner join `rawDataTest`.`iikoIsDeliveryMapping` as iikoIsDeliveryMapping
                    on rawHeaderFetchData.delivery = iikoIsDeliveryMapping.deliveryName
            ;
        """

        rawHeaderTransformData = pd.read_sql(query, conn)
        rawHeaderTransformData['salesTransformed'] = False
        rawHeaderTransformData['paymentTransformed'] = False

        rawHeaderTransformData = rawHeaderTransformData[[
            'fk_rawHeader', 'uniqueOrderId', 'store', 'conception', 'virtualStoreNo', 'virtualStoreName',
            'financeStoreNo', 'accountingDay', 'receiptNo', 'deliveryNo', 'RefinedOrderSource', 'OriginatedCCYN',
            'GrossSalesBD', 'discountAmount', 'surchargeAmount', 'grossSalesAD', 'effectiveDiscount', 'VAT',
            'NetSales', 'NoOfCovers', 'GGGross', 'regionCode', 'OpeningTime', 'tableNo', 'shiftNo', 'deliveryType',
            'section', 'externalOrderNo', 'customerPhoneNumber', 'customerFullName', 'deliveryComment', 'orderWaiter',
            'deliveryPrintTime', 'plannedDeliveryTime', 'deliveryDispatchTime', 'actualDeliveryTime',
            'deliveryClosingTime', 'guestBillTime', 'deliveryInvoicePrintTime', 'closingTime', 'customerPriceCategory',
            'numberOfIitems', 'numberOfDiscountedItems', 'numberOfComboDiscounts', 'averageRevenuePerGuest',
            'lastServPrintTime', 'salesTransformed', 'paymentTransformed'
        ]]

        print(rawHeaderTransformData.shape)

        rawHeaderTransformData_toList = list(tuple(row)
                                             for row in rawHeaderTransformData.values)

        insertQuery = """ INSERT INTO `transformedData`.`transformHeader`
                (
                    `fk_rawHeader`,
                    `uniqueOrderId`,
                    `store`,
                    `conception`,
                    `virtualStoreNo`,
                    `virtualStoreName`,
                    `financeStoreNo`,
                    `accountingDay`,
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
                    `regionCode`,
                    `OpeningTime`,
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
                    `guestBillTime`,
                    `deliveryInvoicePrintTime`,
                    `closingTime`,
                    `customerPriceCategory`,
                    `numberOfIitems`,
                    `numberOfDiscountedItems`,
                    `numberOfComboDiscounts`,
                    `averageRevenuePerGuest`,
                    `lastServPrintTime`,
                    `salesTransformed`,
                    `paymentTransformed`
                )
                VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(insertQuery, rawHeaderTransformData_toList)
        conn.commit()
        print("INSERTED INTO transform Header")

    def transformPayment(self):
        query = """
            with rawHeaderFetchData as (
                SELECT uniqueOrderId FROM `transformedData`.`transformHeader` WHERE paymentTransformed = 0
            )

            SELECT 
                rawPayment.uniqueOrderId as uniqueOrderId, rawPayment.paymentType as paymentType, DATE_FORMAT(rawPayment.accountingDay,'%d/%m/%Y') as accountingDay,
                rawPayment.paymentTypeSplit as paymentTypeSplit, rawPayment.paymentCardNo as paymentCardNo, 
                rawPayment.paymentCurrency as paymentCurrency, rawPayment.nonCashPaymentType as nonCashPaymentType, 
                rawPayment.creditedTo as creditedTo, rawPayment.paymentExchangeRateAED as exchangeRateAED,
                rawPayment.authorisedBy as authorisedBy, rawPayment.authorisedByID as authorisedByID, 
                rawPayment.cashier as cashier, rawPayment.cashierID as cashierID, rawPayment.grossSalesAD as grossSalesAD, 
                rawPayment.VATByBillAmount as VAT, rawPayment.netSalesAD as netSales, rawPayment.numberOfVouchers as numberOfVouchers
            FROM `rawDataTest`.`rawPayment` as rawPayment
            INNER JOIN rawHeaderFetchData
            on rawHeaderFetchData.uniqueOrderId = rawPayment.uniqueOrderId;

        """

        transformPaymentFetchData = pd.read_sql(query, conn)

        transformPaymentFetchData_toList = list(tuple(row)
                                                for row in transformPaymentFetchData.values)

        insert_Query = """
            INSERT INTO `transformedData`.`transformPayment`
                (`uniqueOrderId`,
                `paymentType`,
                `accountingDay`,
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
                `numberOfVouchers`)
                VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        cursor.executemany(insert_Query, transformPaymentFetchData_toList)
        conn.commit()
        print("INSERTED INTO transform Payment")

        updateQuery = f"""
            UPDATE `transformedData`.`transformHeader` SET paymentTransformed = True
            WHERE uniqueOrderId IN {tuple(transformPaymentFetchData.uniqueOrderId)};
        """
        cursor.execute(updateQuery)
        conn.commit()

        print("Update INTO transform Header's paymentTransformed")

    def transformSales(self):
        query = """
            with rawSalesFetchData as (
                SELECT 
                    uniqueOrderId, store, concept, DATE_FORMAT(accountingDay,'%d/%m/%Y') as accountingDay, receiptNo,
                    itemCode, itemId, item as itemName, itemFullName, itemAccountingCategory, itemCategory, itemGroup,
                    courseNo, itemWaiter, itemServicePrinting, itemComment, soldWithItem, discountType, surchargeType,
                    cookingFinishedAt, numberOfItems, averagePriceBeforeDiscount, averagePrice, grossSalesBD, 
                    discountAmount, surchargeAmountAED, grossSalesAD, (grossSalesBD - grossSalesAD) as effectiveDiscount,
                    VATByBillAmount as VAT, netSalesAD as netSales, cookingTimeAverage, cookingTimeAverage1, cookingTimeAverage2,
                    cookingTimeAverage3, cookingTimeAverage4
                FROM `rawDataTest`.`rawSales`
                WHERE rawSales.itemDeleted = 'NOT_DELETED'
            )

            SELECT rawSales.*, iikoStoreMapping.storeNo, iikoBrandMapping.conceptionNo
            FROM rawSalesFetchData as rawSales

            INNER JOIN `transformedData`.`transformHeader` as transHeader
            ON transHeader.uniqueOrderId = rawSales.uniqueOrderId

            inner join `rawDataTest`.`iikoStoreMapping` as iikoStoreMapping 
            on rawSales.store = iikoStoreMapping.storeName

            inner join `rawDataTest`.`iikoBrandMapping` as iikoBrandMapping
            on rawSales.concept = iikoBrandMapping.conceptionName

            WHERE transHeader.salesTransformed = 0
            ;
        """

        transformSalesFetchData = pd.read_sql(query, conn)

        transformSalesFetchData_toList = list(tuple(row)
                                              for row in transformSalesFetchData.values)

        insert_Query = """
            INSERT INTO `transformedData`.`transformSales`
            (
                `uniqueOrderId`,
                `store`,
                `concept`,
                `accountingDay`,
                `receiptNo`,
                `itemCode`,
                `itemId`,
                `itemName`,
                `itemFullName`,
                `itemAccountingCategory`,
                `itemCategory`,
                `itemGroup`,
                `courseNo`,
                `itemWaiter`,
                `itemServicePrinting`,
                `itemComment`,
                `soldWithItem`,
                `discountType`,
                `surchargeType`,
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
                `conceptionNo`
            )
            VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        cursor.executemany(insert_Query, transformSalesFetchData_toList)
        conn.commit()
        print("INSERTED INTO transform Sales")

        updateQuery = f"""
            UPDATE `transformedData`.`transformHeader` SET salesTransformed = True
             WHERE uniqueOrderId IN {tuple(transformSalesFetchData.uniqueOrderId)};
        """
        cursor.execute(updateQuery)
        conn.commit()
        print("Update INTO transform Header's salesTransformed")


transformationRawData = TransformationRawData()
transformationRawData.transformHeader()
transformationRawData.transformPayment()
transformationRawData.transformSales()
