import great_expectations as gx
from Extraction.dfExtraction import readCsvDf

def exploratory_quality_check():
    """
    Quality Report for Input Data using Great Expectations.
    Summarizes findings per quality dimension.
    """
    
    readCsvRes = readCsvDf('data', 'retail_data.csv')
    if not readCsvRes['ok']:
        print(readCsvRes['msg'])
        return
    
    df = readCsvRes['data']
    
    
    df = df.rename(columns={
        "id": "transaction_id"
    })

    
    ge_df = gx.from_pandas(df)

    
    quality_report = {
        "Completeness": {},
        "Uniqueness": {},
        "Validity": {},
        "Consistency": {},
        "Duplicates": {}
    }

    for col in ge_df.columns:
        quality_report["Completeness"][col] = 1 - (ge_df[col].isnull().sum() / len(ge_df))
        quality_report["Uniqueness"][col] = ge_df[col].nunique()
        quality_report["Validity"][col] = str(ge_df[col].dtype)
        quality_report["Consistency"][col] = (ge_df[col].dropna().map(type).nunique() == 1)
        quality_report["Duplicates"][col] = df.duplicated(subset=[col]).sum()

    
    print("=== QUALITY REPORT FOR INPUT DATA ===")
    for dimension, results in quality_report.items():
        print(f"\n--- {dimension} ---")
        for col, value in results.items():
            print(f"{col}: {value}")

    print("\n=== End of Report ===")

if __name__ == "__main__":
    exploratory_quality_check()
