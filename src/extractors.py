from sqlalchemy import create_engine
import pyodbc
import pandas as pd
from typing import Dict
from .utils import setup_logging

logger = setup_logging({'logging': {'level': 'INFO', 'file': 'erp_integration.log'}})

class ERPExtractor:
    """Base class for ERP data extraction."""
    
    def __init__(self, config: Dict):
        self.config = config

    def extract_sap(self) -> pd.DataFrame:
        """Extract procurement data from SAP S/4HANA."""
        try:
            engine = create_engine(self.config['erp_connections']['sap']['conn_str'])
            query = """
                SELECT EKKO.EBELN AS PurchaseOrder, EKPO.MATNR AS Item, EKPO.MENGE AS Quantity, 
                       EKPO.NETPR AS UnitPrice, EKET.EINDT AS DeliveryDate
                FROM S4HANA.EKKO
                INNER JOIN S4HANA.EKPO ON EKKO.EBELN = EKPO.EBELN
                LEFT JOIN S4HANA.EKET ON EKPO.EBELN = EKET.EBELN AND EKPO.EBELP = EKET.EBELP
                WHERE EKKO.BSART IN ('NB', 'ZNB') AND EKKO.AEDAT >= '20240101' AND EKPO.LOEKZ = ''
            """
            df = pd.read_sql(query, engine)
            logger.info("Extracted procurement data from SAP S/4HANA")
            return df
        except Exception as e:
            logger.error(f"SAP extraction failed: {str(e)}")
            raise

    def extract_hyperion(self) -> pd.DataFrame:
        """Extract PnL data from Oracle Hyperion."""
        try:
            engine = create_engine(self.config['erp_connections']['hyperion']['conn_str'])
            query = """
                SELECT b.ACCOUNT AS AccountCode, b.PERIOD AS Period, b.VALUE AS Amount, b.ENTITY AS Entity,
                       a.DESCRIPTION AS AccountDescription
                FROM HFM_DATA.BALANCES b
                INNER JOIN HFM_DATA.ACCOUNTS a ON b.ACCOUNT = a.ACCOUNT_ID
                WHERE b.SCENARIO = 'ACTUAL' AND b.YEAR = '2024' AND (b.ACCOUNT LIKE 'REV%' OR b.ACCOUNT LIKE 'EXP%')
            """
            df = pd.read_sql(query, engine)
            logger.info("Extracted PnL data from Hyperion")
            return df
        except Exception as e:
            logger.error(f"Hyperion extraction failed: {str(e)}")
            raise

    # Similar methods for JDE, Datasul, TOTVS, Senior, Teamsystem (omitted for brevity)