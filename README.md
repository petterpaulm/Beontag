
# Multi-ERP Integration Master Pipeline

![Beontag Logo](./support/beontag_logo.png)

**Data Unleashed | Insights Amplified | Victory Engineered**  
*Version 1.1 | February 20, 2025 | Pedro Mendes | Beontag*

<img src="./support/RFID_giff.gif" alt="ETL Pipeline Animation" width="300">

---

## 🌌 The Data ETL Proposal

Step boldly into the future with the **Multi-ERP Integration Master Pipeline**, a comprehensive, enterprise-grade framework designed to unify and optimize data from **seven leading ERPs**—all within a **Snowflake** fortress on **AWS**. Powered by **Python**, advanced **Machine Learning**, and **Power BI**, this solution revolutionizes procurement, financial reporting, product analytics, and executive risk oversight.

### 🔥 Core Strengths
- **ERP Ecosystem**: **SAP** S/4HANA, **Oracle Hyperion**, **JD Edwards**, **Datasul**, **TOTVS**, **Senior Sistemas**, **Teamsystem**  
- **Strategic Imperatives**: Procurement Optimization, PnL Precision, Margin Enhancement, Cash Flow Foresight, Inventory Dominance, **Financial Risk Management**  
- **Technology Edge**: **Python**, **AWS S3** ☁️, **Snowflake** ❄️, **Power BI** 📊, **Prophet ML**

### 🚀 Identified Positive Impacts
- **Forecast Accuracy Gain**: ML-driven financial projections  
- **Inventory Cost Reduction**: Automated reorder point & safety stock calculations  
- **Analytics Acceleration**: Pre-aggregated BI metrics for real-time insights  
- **Enhanced Risk Visibility**: Built-in financial and operational risk indicators

---

## 📜 Blueprint

- [Multi-ERP Integration Master Pipeline](#multi-erp-integration-master-pipeline)
  - [🌌 The Data ETL Proposal](#-the-data-etl-proposal)
    - [🔥 Core Strengths](#-core-strengths)
    - [🚀 Identified Positive Impacts](#-identified-positive-impacts)
  - [📜 Blueprint](#-blueprint)
  - [ETL Command Center](#etl-command-center)
    - [🔍 Extraction: Source Activation](#-extraction-source-activation)
    - [1. SAP S/4HANA: Procurement Titan](#1-sap-s4hana-procurement-titan)
    - [2. Oracle Hyperion: Financial Beacon](#2-oracle-hyperion-financial-beacon)
    - [3. JD Edwards: Margin Architect](#3-jd-edwards-margin-architect)
    - [4. TOTVS: Procurement Data Extraction](#4-totvs-procurement-data-extraction)
    - [5. Datasul: Procurement Data Extraction](#5-datasul-procurement-data-extraction)
    - [6. Senior Sistemas (2.1.6): Procurement Data Extraction](#6-senior-sistemas-216-procurement-data-extraction)
    - [7. Teamsystem (2.1.7): Procurement Data Extraction](#7-teamsystem-217-procurement-data-extraction)
  - [Transformation: Data Reforging](#transformation-data-reforging)
    - [Procurement Transformation](#procurement-transformation)
    - [PnL Transformation](#pnl-transformation)
    - [Product Margin Transformation](#product-margin-transformation)
  - [Load Arsenal](#load-arsenal)
    - [AWS S3 Staging ☁️](#aws-s3-staging-️)
    - [Snowflake Loading ❄️](#snowflake-loading-️)
  - [📦 Data Fortification](#-data-fortification)
  - [Power BI War Room 📊](#power-bi-war-room-)
    - [🔗 Live Connection](#-live-connection)
    - [Inventory Command \& Additional KPIs](#inventory-command--additional-kpis)
    - [🧠 Advanced Pre-Calculations](#-advanced-pre-calculations)
  - [Deployment Protocol](#deployment-protocol)
    - [🛠️ System Setup](#️-system-setup)
    - [⚙️ Core Configuration](#️-core-configuration)
  - [Launch Sequence](#launch-sequence)
  - [Flow Visualization](#flow-visualization)
    - [🎥 Pipeline Animation](#-pipeline-animation)
    - [📓 Interactive Notebooks](#-interactive-notebooks)
  - [Pioneering the Future](#pioneering-the-future)
    - [Visionary Roadmap](#visionary-roadmap)

---

## ETL Command Center

### 🔍 Extraction: Source Activation
This pipeline orchestrates data acquisition from diverse ERPs. Each system has specialized tables, queries, and indexing strategies—below we expand on additional attributes and potential calculations that each ERP can contribute.

---

### 1. SAP S/4HANA: Procurement Titan

**Purpose**: Acquire comprehensive PO data—headers, items, and delivery. Also captures supplier master data and invoice tracking.

**Key Tables**:
- **EKKO**: PO Header  
  - Fields: `EBELN` (PO number), `BSART` (Order type), `AEDAT` (Change date), `LIFNR` (Vendor ID)
- **EKPO**: PO Items  
  - Fields: `MATNR` (Material), `MENGE` (Quantity), `NETPR` (Unit price), `LOEKZ` (Deletion indicator)
- **EKET**: Schedule Lines  
  - Fields: `EINDT` (Delivery date), `MENGE_ET` (Scheduled qty)
- **LFA1**: Vendor Master (Optional)  
  - Fields: `LIFNR` (Vendor ID), `NAME1` (Vendor Name), `SORTL` (Sort key)

**SQL Query**:
```sql
SELECT 
    EKKO.EBELN AS PurchaseOrder,
    EKKO.LIFNR AS Vendor,
    EKPO.MATNR AS Material,
    EKPO.MENGE AS Quantity,
    EKPO.NETPR AS UnitPrice,
    EKET.EINDT AS DeliveryDate
FROM S4HANA.EKKO
INNER JOIN S4HANA.EKPO
    ON EKKO.EBELN = EKPO.EBELN
LEFT JOIN S4HANA.EKET
    ON EKPO.EBELN = EKET.EBELN
    AND EKPO.EBELP = EKET.EBELP
WHERE 
    EKKO.BSART IN ('NB','ZNB')
    AND EKKO.AEDAT >= '20240101'
    AND EKPO.LOEKZ = ''
```

**Additional Factors**:
- **Vendor Performance**: Join with `LFA1` to track vendor rating, blacklisted vendors, or payment terms.
- **Gr/Ir (Goods Receipt/Invoice Receipt)**: Compare receipts vs. invoiced amounts for accrual management.
- **Potential Calculations**:
  - `POtoGRTime` = difference between PO creation and goods receipt date
  - `InvoicePaymentDelta` = difference between invoice date & actual payment date

---

### 2. Oracle Hyperion: Financial Beacon

**Purpose**: Capture financial balances and detailed accounts. Often used for consolidated financial statements, IFRS reporting, and scenario planning.

**Key Tables**:
- **BALANCES**: b.ACCOUNT, b.PERIOD, b.VALUE, b.ENTITY, b.SCENARIO
- **ACCOUNTS**: a.ACCOUNT_ID, a.DESCRIPTION, a.ACCOUNT_TYPE

**SQL Query**:
```sql
SELECT 
    b.ACCOUNT AS AccountCode,
    b.PERIOD AS Period,
    b.VALUE AS Amount,
    b.ENTITY AS Entity,
    b.SCENARIO AS Scenario,
    a.DESCRIPTION AS AccountDescription,
    a.ACCOUNT_TYPE AS AccountType
FROM HFM_DATA.BALANCES b
INNER JOIN HFM_DATA.ACCOUNTS a
    ON b.ACCOUNT = a.ACCOUNT_ID
WHERE 
    b.SCENARIO = 'ACTUAL'
    AND b.YEAR = '2024'
    AND (b.ACCOUNT LIKE 'REV%' OR b.ACCOUNT LIKE 'EXP%')
```

**Additional Factors**:
- **IFRS vs. GAAP**: Hyperion can store multiple local GAAP & IFRS adjustments.
- **FX Conversion**: Manage multi-currency conversions and revaluations in Hyperion.
- **Potential Calculations**:
  - `EBITDA` = (Revenue - Expenses + Other Income) if chart of accounts supports it
  - `CashFlowOps` = Summation of net changes in relevant accounts
  - `ScenarioComparisons` = Actual vs. Budget vs. Forecast

---

### 3. JD Edwards: Margin Architect

**Purpose**: Evaluate margins by analyzing sales price vs. unit cost, plus potential overhead or labor cost integration.

**Key Tables**:
- **F4105**: Cost references  
  - Fields: `IMLITM` (Item), `COUNCS` (Unit cost), `LANL` (labor add-on?), `MOH` (manufacturing overhead, if present)
- **F4211**: Sales orders
  - Fields: `SDUPRC` (Unit price), `SDQSOL` (Qty sold), `SDIVD` (Invoice date)

**SQL Query**:
```sql
SELECT
    F4105.IMLITM AS ItemNumber,
    (F4211.SDUPRC / 10000) AS UnitPrice,
    (F4105.COUNCS / 10000) AS UnitCost,
    F4211.SDQSOL AS QuantitySold,
    ((F4211.SDUPRC - F4105.COUNCS) / 10000) * F4211.SDQSOL AS Margin
FROM JDE_DTA.F4105
INNER JOIN JDE_DTA.F4211
    ON F4105.IMLITM = F4211.SDLITM
WHERE
    F4211.SDIVD >= '20240101'
    AND F4211.SDQSOL > 0
```

**Additional Factors**:
- **Overhead Rates**: If manufacturing overhead fields are enabled, add to cost calculations.
- **Rebates or Discounts**: Check `SDSPATTN` in F4211 for special pricing or promotional discounts.
- **Potential Calculations**:
  - `LaborCostPercentage` = (LaborCost / UnitCost) * 100
  - `OverheadAbsorption` = Overhead per item * QuantitySold

---

### 4. TOTVS: Procurement Data Extraction

**Purpose**: Collect POs and corresponding items, integrating with TOTVS RM or Protheus for extended data.

**Key Tables**:
- **TSI1030**: PO Header (NUM_PEDIDO, DATA_EMISSAO, STATUS_PEDIDO, FORNECEDOR_ID, COND_PAGTO)
- **TSI1040**: PO Items (COD_PRODUTO, QTD_PEDIDO, VAL_UNITARIO, QTD_ENTREGUE, DESCONTO)

**SQL Query**:
```sql
SELECT
    TSI1030.NUM_PEDIDO,
    TSI1030.DATA_EMISSAO,
    TSI1030.FORNECEDOR_ID,
    TSI1030.COND_PAGTO,
    TSI1040.COD_PRODUTO,
    TSI1040.QTD_PEDIDO,
    TSI1040.VAL_UNITARIO,
    TSI1040.QTD_ENTREGUE,
    TSI1040.DESCONTO,
    TSI1030.STATUS_PEDIDO
FROM TOTVS.TSI1030
INNER JOIN TOTVS.TSI1040
    ON TSI1030.NUM_PEDIDO = TSI1040.NUM_PEDIDO
WHERE
    TSI1030.STATUS_PEDIDO = 'APROVADO'
    AND TSI1030.DATA_EMISSAO >= '20240101'
```

**Additional Factors**:
- **Payment Terms**: `COND_PAGTO` to track net 30, net 60, etc.
- **Discount Management**: `DESCONTO` for item-level or header-level discount synergy.
- **Potential Calculations**:
  - `EffectiveUnitPrice` = VAL_UNITARIO - DESCONTO
  - `PayableDays` = parse `COND_PAGTO` to compute average days to pay

---

### 5. Datasul: Procurement Data Extraction

**Purpose**: Obtain PO headers & line item details, plus additional supplier or item group references.

**Key Tables**:
- **COMPRAS_PEDIDOS** (PO header info)
  - Fields: `COD_PEDIDO`, `DATA_EMISSAO`, `STATUS`, `COD_FORNEC`
- **ITENS_COMPRA** (PO item details)
  - Fields: `COD_ITEM`, `QTD_PEDIDA`, `VALOR_UNITARIO`, `QTD_ENTREGUE`, `DESCONTO_ITEM`

**SQL Query**:
```sql
SELECT
    cp.COD_PEDIDO,
    cp.DATA_EMISSAO,
    cp.COD_FORNEC,
    cp.STATUS,
    ic.COD_ITEM,
    ic.QTD_PEDIDA,
    ic.VALOR_UNITARIO,
    ic.QTD_ENTREGUE,
    ic.DESCONTO_ITEM
FROM DATASUL.COMPRAS_PEDIDOS cp
INNER JOIN DATASUL.ITENS_COMPRA ic
    ON cp.COD_PEDIDO = ic.COD_PEDIDO
WHERE
    cp.STATUS = 'APROVADO'
    AND cp.DATA_EMISSAO >= '2024-01-01'
    AND ic.SITUACAO = 'CONFIRMADO'
```

**Additional Factors**:
- **COD_FORNEC** cross-reference to `FORNECEDORES` table for vendor rating.
- **Potential Calculations**:
  - `AdjustedUnitPrice` = VALOR_UNITARIO * (1 - DESCONTO_ITEM/100)
  - `FillRate` = QTD_ENTREGUE / QTD_PEDIDA * 100

---

### 6. Senior Sistemas (2.1.6): Procurement Data Extraction

**Purpose**: Aggregate approved purchase orders for deeper analytics. Senior often integrates HR, payroll, and manufacturing modules, allowing extended analytics.

**Key Tables**:
- **PEDIDO_COMPRA**: PO header
- **ITENS_PEDIDO**: PO items
- **FORNECEDOR** (if available): Supplier details

**SQL Query**:
```sql
SELECT
    pc.NUMERO_PEDIDO,
    pc.DATA_EMISSAO,
    ip.CODIGO_PRODUTO,
    ip.QUANTIDADE,
    ip.VALOR_UNITARIO,
    ip.DATA_ENTREGA,
    pc.STATUS
FROM SENIOR.PEDIDO_COMPRA pc
INNER JOIN SENIOR.ITENS_PEDIDO ip
    ON pc.NUMERO_PEDIDO = ip.NUMERO_PEDIDO
WHERE
    pc.STATUS = 'APROVADO'
    AND pc.DATA_EMISSAO >= '2024-01-01'
```

**Additional Factors**:
- **Supplier Workforce**: If Senior HR is integrated, link labor data to item costs.
- **Potential Calculations**:
  - `OnTimeDeliveryRate` = count of orders delivered before `DATA_ENTREGA` / total orders
  - `WorkforceCostRatio` = portion of payroll allocated to procurement ops (if cross-module data is used)

---

### 7. Teamsystem (2.1.7): Procurement Data Extraction

**Purpose**: Retrieve order data and item-level specifics, often used in EU markets.

**Key Tables**:
- **ORDINI_ACQUISTO**: PO header (ID_ORDINE, DATA_ORDINE, STATO)
- **DETTAGLI_ORDINI**: PO items (CODICE_ARTICOLO, QUANTITA, PREZZO_UNITARIO, DATA_CONSEGNA)
- **FORNITORI** (optional): If you need vendor-level data

**SQL Query**:
```sql
SELECT
    oa.ID_ORDINE,
    oa.DATA_ORDINE,
    do.CODICE_ARTICOLO,
    do.QUANTITA,
    do.PREZZO_UNITARIO,
    do.DATA_CONSEGNA,
    oa.STATO
FROM TEAMSYS.ORDINI_ACQUISTO oa
INNER JOIN TEAMSYS.DETTAGLI_ORDINI do
    ON oa.ID_ORDINE = do.ID_ORDINE
WHERE
    oa.STATO = 'APPROVATO'
    AND oa.DATA_ORDINE >= '20240101'
```

**Additional Factors**:
- **EU VAT**: Teamsystem may contain VAT rates or invoice data for tax compliance.
- **Potential Calculations**:
  - `VATAmount` = PREZZO_UNITARIO * QUANTITA * VATRate
  - `DeliveryFulfillment%` = (DeliveredQty / QUANTITA) * 100

---

## Transformation: Data Reforging
In this stage, raw datasets are refined into analytical gold, incorporating business logic, advanced metrics, and ML enhancements.

### Procurement Transformation
**Input**: Raw procurement data from SAP, Datasul, TOTVS, Senior, Teamsystem

**Steps**:
1. **Standardization**: Convert columns to `PurchaseOrder`, `Item`, `Quantity`, `UnitPrice`, unify date formats.
2. **Aggregation**: Group by `(PurchaseOrder, Item)`; compute `sum(Quantity)`, `avg(UnitPrice)`.
3. **Enrichment**:
   - `TotalCost = Quantity * UnitPrice`
   - `SupplierLeadTime` = difference between approval date & scheduled delivery date
   - `DaysToDelivery` = difference between system date & actual delivery date
   - `Source` = ERP label (e.g., "SAP", "TOTVS")
   - `APCycleTime` = difference between invoice date & payment date (if available)
   - `CashConversionCycle` if purchase & payment data is available
   - `DiscountAdjustedPrice` for TOTVS/Datasul item-level discount
4. **Validation**:
   - Remove duplicates, ensure `Quantity > 0`.
   - Check outliers on `UnitPrice` (flag if above 3-sigma range)
5. **ML-Ready Fields**:
   - Feature engineering for supplier reliability, historical delays, or payment trends
   - Potential binary feature for discount usage, vendor category, etc.

**Output**: Standardized, ML-ready procurement dataset.

### PnL Transformation
**Input**: Oracle Hyperion balances & accounts.

**Steps**:
1. **Aggregation**: By `(AccountCode, Period, Entity)`, sum `Amount`.
2. **Categorization**: Assign to `Revenue`, `Expense`, or `COGS` based on account patterns.
3. **Enrichment**:
   - `AmountUSD = Amount * 0.19` (BRL→USD rate)
   - `Variance% = ( (Amount - lag(Amount)) / lag(Amount) ) * 100`
   - `RollingAvgAmount = rolling_mean(Amount, window=3)`
   - `OperatingMargin = (Revenue - Expense) / Revenue * 100` (if data available)
   - `NetIncome = (Revenue - (Expense + COGS))` (where relevant)
4. **Risk & Finance Metrics**:
   - **Liquidity Ratio**: `CurrentAssets / CurrentLiabilities` if short-term balance data is merged
   - **VaR (Value at Risk)**: approximate daily/monthly revenue volatility
   - **CreditExposure**: potential overdue or high-risk AR entries
   - **BudgetvsActual**: if Hyperion Budget data is loaded, calculate performance gap
5. **Validation**:
   - Null checks on `AccountType`
   - Integrity checks (Revenue >= 0, etc.)
6. **Forecast Add-On**:
   - Prophet or ARIMA for monthly forecasted `Amount`
   - `ConfidenceInterval` for scenario analysis

**Output**: Enhanced PnL dataset for financial intelligence, risk assessment, and multi-scenario planning.

### Product Margin Transformation
**Input**: JD Edwards margin data.

**Steps**:
1. **Aggregation**: Summarize by `ItemNumber`; `sum(Margin)`, `sum(QuantitySold)`, `mean(UnitPrice)`, `mean(UnitCost)`.
2. **Enrichment**:
   - `MarginPercentage = (Margin / (UnitPrice * QuantitySold)) * 100`
   - `Profitability = categorize(Margin, thresholds=[0, 5000, 20000])`
   - `CostToPriceRatio = UnitCost / UnitPrice`
   - `InventoryTurnover = QuantitySold / avg_stock_on_hand`
   - `ABCClassification` = categorize items by sales volume or Margin
3. **Validation**: `fillna(0)` for missing cost/sales fields.
4. **Advanced Calculations**:
   - `GrossMarginTrend` = rolling 3-month average of margin
   - `BreakEvenPoint` = fixedCosts / (UnitPrice - UnitCost)
   - `CostVariance` = actual cost vs. standard cost
   - `DiscountImpact` = measure how promotional pricing affects margin

**Output**: Margin-optimized dataset with deep profit metrics.

---

## Load Arsenal

### AWS S3 Staging ☁️
- **Process**: Convert data to **Parquet** (Snappy compression), upload to `s3://erp-data-bucket/processed/` with metadata (e.g., `ProcessedDate`, `Region`, `DataOwner`).
- **Output**: Partitioned & versioned Parquet (e.g., `procurement_20250221.parquet`).

### Snowflake Loading ❄️
- **Process**:
  1. Create tables (`ERP_PROCUREMENT`, `ERP_PNL`, `ERP_PRODUCT_MARGIN`) with adaptive column definitions.
  2. Stage Parquet with `PUT`, load with `COPY INTO`.
  3. Optionally configure Snowflake **Streams** for incremental data.
- **Output**: Secure, scalable tables for BI & advanced analytics.

---

## 📦 Data Fortification
Following transformation, data resides in:
- **AWS S3**: Flexible, durable data lake.
- **Snowflake**: High-performance warehouse with advanced query optimization.

Additionally, for **stakeholders preferring Excel** outputs, an **automated Excel export** can be configured post-load. Using Python libraries like **XlsxWriter** or **OpenPyXL**, the pipeline can:
- Generate pivot tables on the processed data
- Save dashboards into `.xlsx` format
- Email or store these Excel files in a shared drive or an **Azure/AWS** location for executive review

---

## Power BI War Room 📊

### 🔗 Live Connection
Real-time dashboards via **Snowflake Direct Query**.
```plaintext
Server: account.region.snowflakecomputing.com
Database: ERP_DATA
Schema: PUBLIC
```

### Inventory Command & Additional KPIs
- **SafetyStock**:
```DAX
SafetyStock = AVERAGE('ERP_PRODUCT_MARGIN'[QuantitySold]) * 1.5
```
- **LeadTimeDeviation**:
```DAX
LeadTimeDeviation = STDEVX.P('ERP_PROCUREMENT', 'ERP_PROCUREMENT'[DaysToDelivery])
```
- **ServiceLevel**:
```DAX
ServiceLevel = 1 - (SUM('ERP_PROCUREMENT'[LateDeliveries]) / SUM('ERP_PROCUREMENT'[TotalDeliveries]))
```

### 🧠 Advanced Pre-Calculations
- **ForecastedAmount**: Prophet or ARIMA for PnL forecasting
- **SupplierRiskScore**: Weighted score of delivery performance, lead time variance, quality metrics
- **ProductLifecycleStage**: Category mapping based on sales velocity & margin trends

---

## Deployment Protocol

### 🛠️ System Setup
```bash
# Clone repository
git clone https://github.com/yourusername/multi-erp-etl.git
cd multi-erp-etl

# Install essential dependencies
pip install -r requirements.txt
```

### ⚙️ Core Configuration
Update `config.yaml` with credentials and settings:
```yaml
erp_connections:
  sap:
    conn_str: "sap://user:password@host:port/S4HANA"
aws:
  s3_bucket: "erp-data-bucket"
  access_key: "your_aws_access_key"
snowflake:
  user: "snowflake_user"
  account: "account.region"
  warehouse: "COMPUTE_WH"
  role: "SYSADMIN"
```

---

## Launch Sequence
```bash
python main.py
```

**Outputs**:
- **S3**: Partitioned Parquet files in `s3://erp-data-bucket/processed/`
- **Snowflake**: Analytical tables ready for exploration
- **Logs**: `erp_integration.log` captures each stage
- **Excel Exports** (optional): Automated `.xlsx` with pivot tables, emailed or stored for offline consumption

---

## Flow Visualization

### 🎥 Pipeline Animation
Watch the 6-second loop demonstrating extraction, transformation, and loading into **Snowflake** & **Power BI**.

### 📓 Interactive Notebooks
Try out sample transformations or run ML experiments:
> **Open in Colab** – [Add your link here]

---

## Pioneering the Future
Accelerate data-driven culture and modernize your enterprise:
- **Star** this repo ⭐ to support continued innovation
- **Contribute**: Fork, branch, and open a PR with your enhancements

### Visionary Roadmap
- **ETL Pipeline V1**: Full integration for all 7 ERPs
- **Machine Learning Upscale**: Predictive insights in Power BI
- **Real-Time Data Streams (Q2 2025)**: Ingest data via Kafka/Azure Event Hubs
- **Cost Optimization & Sustainability Dashboards (Q3 2025)**

