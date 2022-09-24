with rawSalesFetchData as (
	SELECT 
		uniqueOrderId, DATE_FORMAT(accountingDay,'%d/%m/%Y') as accountingDate, receiptNo,
		itemCode, itemId, 
        TRIM(SUBSTRING_INDEX(item, '/', 1)) as itemName, 
        itemFullName, itemAccountingCategory, itemCategory, itemGroup,
        
        level1_itemGroup, level2_itemGroup, level3_itemGroup,
        
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