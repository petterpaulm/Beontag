from src.extractors import ERPExtractor
from src.transformers import ERPTransformer
from src.loaders import DataLoader
from src.utils import load_config, setup_logging, validate_data

def main():
    """Main entry point for the ETL pipeline."""
    config = load_config()
    logger = setup_logging(config)
    logger.info("Starting ETL pipeline execution")

    # Extraction
    extractor = ERPExtractor(config)
    sap_df = extractor.extract_sap()
    hyperion_df = extractor.extract_hyperion()
    # Add other ERP extractions here

    # Validation
    rules = {'Quantity': {'type': 'range', 'min': 0, 'max': 1e6}, 'UnitPrice': {'type': 'not_null'}}
    issues = validate_data(sap_df, rules)
    if issues:
        logger.error(f"Validation issues: {issues}")
        return

    # Transformation
    transformer = ERPTransformer()
    procurement_df = transformer.transform_procurement([sap_df])  # Add other procurement DFs
    pnl_df = transformer.transform_pnl(hyperion_df)
    # Add product margin transformation

    # Loading
    loader = DataLoader(config)
    loader.upload_to_s3(procurement_df, 'procurement')
    loader.load_to_snowflake(procurement_df, 'ERP_PROCUREMENT')
    loader.upload_to_s3(pnl_df, 'pnl')
    loader.load_to_snowflake(pnl_df, 'ERP_PNL')

    logger.info("ETL pipeline completed successfully")

if __name__ == "__main__":
    main()