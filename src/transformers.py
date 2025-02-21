import pandas as pd
import numpy as np
from prophet import Prophet
from typing import Dict, List
from .utils import setup_logging

logger = setup_logging({'logging': {'level': 'INFO', 'file': 'erp_integration.log'}})

class ERPTransformer:
    """Handles data transformation and pre-calculations."""
    
    def __init__(self):
        pass

    def transform_procurement(self, dfs: List[pd.DataFrame]) -> pd.DataFrame:
        """Transform procurement data with pre-calculations."""
        procurement_df = pd.concat(dfs).drop_duplicates()
        procurement_df = procurement_df.groupby(['PurchaseOrder', 'Item']).agg({
            'Quantity': 'sum',
            'UnitPrice': 'mean'
        }).reset_index()
        procurement_df['TotalCost'] = procurement_df['Quantity'] * procurement_df['UnitPrice']
        procurement_df['DaysToDelivery'] = (pd.to_datetime(procurement_df['DeliveryDate']) - pd.Timestamp.now()).dt.days
        procurement_df['CostTrend'] = procurement_df['TotalCost'].rolling(window=30, min_periods=1).mean()
        logger.info("Transformed procurement data")
        return procurement_df

    def transform_pnl(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform PnL data with ML forecasting."""
        pnl_df = df.groupby(['AccountCode', 'Period', 'Entity']).agg({'Amount': 'sum'}).reset_index()
        pnl_df['Category'] = np.where(pnl_df['AccountCode'].str.startswith('REV'), 'Revenue', 'Expense')
        pnl_df['AmountUSD'] = pnl_df['Amount'] * 0.19
        # ML Forecast
        forecast_df = pnl_df[['Period', 'AmountUSD']].rename(columns={'Period': 'ds', 'AmountUSD': 'y'})
        model = Prophet(yearly_seasonality=True)
        model.fit(forecast_df)
        future = model.make_future_dataframe(periods=90)
        forecast = model.predict(future)
        pnl_df = pnl_df.merge(forecast[['ds', 'yhat']], left_on='Period', right_on='ds', how='left')
        pnl_df.rename(columns={'yhat': 'ForecastedAmount'}, inplace=True)
        logger.info("Transformed PnL data with forecast")
        return pnl_df

    def transform_product_margin(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform product margin data with advanced metrics."""
        margin_df = df.groupby('ItemNumber').agg({
            'Margin': 'sum',
            'UnitPrice': 'mean',
            'UnitCost': 'mean',
            'QuantitySold': 'sum'
        }).reset_index()
        margin_df['MarginPercentage'] = (margin_df['Margin'] / (margin_df['UnitPrice'] * margin_df['QuantitySold'])) * 100
        margin_df['Profitability'] = pd.cut(margin_df['Margin'], bins=[-float('inf'), 0, 1000, float('inf')], labels=['Loss', 'Low', 'High'])
        logger.info("Transformed product margin data")
        return margin_df