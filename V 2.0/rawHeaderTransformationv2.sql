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
    FROM `rawHeader` 
    WHERE orderDeleted = 'NOT_DELETED' and store != 'Test Lab'
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