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