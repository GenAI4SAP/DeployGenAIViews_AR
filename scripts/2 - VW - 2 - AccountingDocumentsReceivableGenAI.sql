create or replace view {{ project_id_tgt }}.{{ dataset_reporting_tgt }}.AccountingDocumentsReceivableGenAI as
SELECT
  AccountingDocuments.Client_MANDT,
  AccountingDocuments.ExchangeRateType_KURST,
  AccountingDocuments.CompanyCode_BUKRS,
  CompaniesMD.CompanyText_BUTXT,
  AccountingDocuments.CustomerNumber_KUNNR,
  AccountingDocuments.FiscalYear_GJAHR,
  AccountingDocuments.ClearingDate_AUGDT,
  AccountingDocuments.BusinessArea_GSBER,
  AccountingDocuments.DocumentType_BLART,
  AccountingDocuments.AmountInSecondLocalCurrency_DMBE2,
  AccountingDocuments.AmountInLocalCurrency_DMBTR,
  AccountingDocuments.AmountInDocumentCurrency_WRBTR,
  AccountingDocuments.TermsOfPaymentKey_ZTERM,
  CustomersMD.NAME1_NAME1 as CustomerName_NAME1,
  CompaniesMD.Country_LAND1 AS Company_Country,
  CompaniesMD.CityName_ORT01 AS Company_City,
  CustomersMD.CountryKey_LAND1,
  CustomersMD.City_ORT01,
  AccountingDocuments.AccountingDocumentNumber_BELNR,
  ReferenceDocumentNumber_XBLNR, --##CORTEX-CUSTOMER Insert field ReferenceDocumentNumber_XBLNR
  AccountingDocuments.NumberOfLineItemWithinAccountingDocument_BUZEI,
  AccountingDocuments.CurrencyKey_WAERS,
  AccountingDocuments.LocalCurrency_HWAER,
  AccountingDocuments.CurrencyKeyOfSecondLocalCurrency_HWAE2,
  CompaniesMD.FiscalyearVariant_PERIV,
      IF(`{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Period`(AccountingDocuments.Client_MANDT,
          CompaniesMD.FiscalyearVariant_PERIV,
          AccountingDocuments.PostingDateInTheDocument_BUDAT) = 'CASE1',
        `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Case1`(AccountingDocuments.Client_MANDT,
          CompaniesMD.FiscalyearVariant_PERIV,
          AccountingDocuments.PostingDateInTheDocument_BUDAT),
        IF(`{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Period`(AccountingDocuments.Client_MANDT,
            CompaniesMD.FiscalyearVariant_PERIV,
            AccountingDocuments.PostingDateInTheDocument_BUDAT) = 'CASE2',
          `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Case2`(AccountingDocuments.Client_MANDT,
            CompaniesMD.FiscalyearVariant_PERIV,
            AccountingDocuments.PostingDateInTheDocument_BUDAT),
          IF(`{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Period`(AccountingDocuments.Client_MANDT,
              CompaniesMD.FiscalyearVariant_PERIV,
              AccountingDocuments.PostingDateInTheDocument_BUDAT) = 'CASE3',
            `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Case3`(AccountingDocuments.Client_MANDT,
              CompaniesMD.FiscalyearVariant_PERIV,
              AccountingDocuments.PostingDateInTheDocument_BUDAT),
            'DATA ISSUE'))) AS Period,
      IF(`{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Period`(AccountingDocuments.Client_MANDT,
          CompaniesMD.FiscalyearVariant_PERIV,
          CURRENT_DATE()) = 'CASE1',
        `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Case1`(AccountingDocuments.Client_MANDT,
          CompaniesMD.FiscalyearVariant_PERIV,
          CURRENT_DATE()),
        IF(`{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Period`(AccountingDocuments.Client_MANDT,
            CompaniesMD.FiscalyearVariant_PERIV,
            CURRENT_DATE()) = 'CASE2',
          `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Case2`(AccountingDocuments.Client_MANDT,
            CompaniesMD.FiscalyearVariant_PERIV,
            CURRENT_DATE()),
          IF(`{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Period`(AccountingDocuments.Client_MANDT,
              CompaniesMD.FiscalyearVariant_PERIV,
              CURRENT_DATE()) = 'CASE3',
            `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Case3`(AccountingDocuments.Client_MANDT,
              CompaniesMD.FiscalyearVariant_PERIV,
              CURRENT_DATE()),
            'DATA ISSUE'))) AS Current_Period,
      AccountingDocuments.AccountType_KOART,
      AccountingDocuments.PostingDateInTheDocument_BUDAT,
      AccountingDocuments.DocumentDateInDocument_BLDAT,
      AccountingDocuments.InvoiceToWhichTheTransactionBelongs_REBZG,
      AccountingDocuments.BillingDocument_VBELN,
      AccountingDocuments.WrittenOffAmount_DMBTR,
      --AccountingDocuments.BadDebt_DMBTR,
      AccountingDocuments.netDueDateCalc AS NetDueDate,
      AccountingDocuments.sk2dtCalc AS CashDiscountDate1,
      AccountingDocuments.sk1dtCalc AS CashDiscountDate2,
      -- AccountingDocuments.OpenAndNotDue, --##CORTEX-CUSTOMER Change OpenAndNotDue to Attribute
        IF(PostingDateInTheDocument_BUDAT <= CURRENT_DATE() AND NetDueDateCalc >= CURRENT_DATE()
        AND ClearingDate_AUGDT IS NULL, True, False) AS IsOpenAndNotDue, --##CORTEX-CUSTOMER Insert IsOpenAndNotDue as Attribute  
      -- AccountingDocuments.OpenAndOverDue, --##CORTEX-CUSTOMER Change OpenAndOverDue to Attribute
        IF(PostingDateInTheDocument_BUDAT <= CURRENT_DATE() AND NetDueDateCalc < CURRENT_DATE()
        AND ClearingDate_AUGDT IS NULL, True, False) AS IsOpenAndOverDue, --##CORTEX-CUSTOMER Insert OpenAndOverDue as Attribute  
      -- AccountingDocuments.ClearedAfterDueDate, --##CORTEX-CUSTOMER Change ClearedAfterDueDate to Attribute
        IF(PostingDateInTheDocument_BUDAT < CURRENT_DATE() AND ClearingDate_AUGDT > NetDueDateCalc 
        AND ClearingDate_AUGDT IS NOT NULL, True, False) AS IsClearedAfterDueDate, --##CORTEX-CUSTOMER Insert ClearedAfterDueDate as Attribute
      -- AccountingDocuments.ClearedOnOrBeforeDueDate, --##CORTEX-CUSTOMER Change ClearedOnOrBeforeDueDate to Attribute
        IF(PostingDateInTheDocument_BUDAT < CURRENT_DATE() AND ClearingDate_AUGDT <= NetDueDateCalc 
        AND ClearingDate_AUGDT  IS NOT NULL, True, False) AS IsClearedOnOrBeforeDueDate, --##CORTEX-CUSTOMER Insert ClearedOnOrBeforeDueDate as Attribute
      -- AccountingDocuments.DoubtfulReceivables, --##CORTEX-CUSTOMER Change DoubtfulReceivables to Attribute
        IF(PostingDateInTheDocument_BUDAT < CURRENT_DATE() AND (DATE_DIFF(NetDueDateCalc, CURRENT_DATE(), DAY) > 90 )
        AND ClearingDate_AUGDT IS NULL, True, False) AS IsDoubtfulReceivables,  --##CORTEX-CUSTOMER Insert DoubtfulReceivables as Attribute
      -- AccountingDocuments.DaysInArrear, --##CORTEX-CUSTOMER Change to consider the compensation date
        IF( ClearingDate_AUGDT > NetDueDateCalc,DATE_DIFF(IF(ClearingDate_AUGDT IS NULL,CURRENT_DATE(),ClearingDate_AUGDT),NetDueDateCalc,DAY),0) AS DaysInArrear,
      -- AccountingDocuments.DaysOfPayment, --##CORTEX-CUSTOMER Consider the compensation date and adiantament
        DATE_DIFF(IF(ClearingDate_AUGDT IS NULL,CURRENT_DATE(),ClearingDate_AUGDT),NetDueDateCalc,DAY) AS DaysOfPayment,    
      -- AccountingDocuments.AccountsReceivable, --##CORTEX-CUSTOMER Change AccountsReceivable to Attribute
      IF( ClearingDate_AUGDT IS NULL AND PostingDateInTheDocument_BUDAT < CURRENT_DATE()
      , True, False) AS IsAccountsReceivable, --##CORTEX-CUSTOMER Insert AccountsReceivable as Attribute
      -- AccountingDocuments.Sales --##CORTEX-CUSTOMER Change Sales to Attribute
      IF( Indicator_SalesRelatedItem_XUMSW = 'X' AND DocumentType_BLART IN('RN','RV','DR', 'FR', 'RE', 'RF', 'RR','ZG')
      , True, False) AS IsSales, --##CORTEX-CUSTOMER Insert Sales as Attribute
    FROM
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.AccountingDocuments` AS AccountingDocuments
    LEFT JOIN
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CustomersMD` AS CustomersMD
      ON
        AccountingDocuments.Client_MANDT = CustomersMD.Client_MANDT
        AND AccountingDocuments.CustomerNumber_KUNNR = CustomersMD.CustomerNumber_KUNNR
    LEFT JOIN
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CompaniesMD` AS CompaniesMD
      ON
        AccountingDocuments.Client_MANDT = CompaniesMD.Client_MANDT
        AND AccountingDocuments.CompanyCode_BUKRS = CompaniesMD.CompanyCode_BUKRS
    WHERE AccountingDocuments.AccountType_KOART = "D"      
    AND AccountingDocuments.DocumentType_BLART <> 'GD'