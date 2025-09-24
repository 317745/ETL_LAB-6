import great_expectations as gx
from Extraction.dfExtraction import readCsvDf

def exploratory_quality_check():
    
    readCsvRes = readCsvDf('data', 'retail_data.csv')
    if not readCsvRes['ok']:
        print(readCsvRes['msg'])
        return
    
    df = readCsvRes['data']
    

    df = df.rename(columns={
        "id": "customer_id",
        "customer_id": "transaction_id"
    })

    
    ge_df = gx.from_pandas(df)

    
    report = {}
    for col in ge_df.columns:
        report[col] = {
            "data_type": str(ge_df[col].dtype),
            "missing_values": ge_df[col].isnull().sum(),
            "unique_values": ge_df[col].nunique(),
            "duplicates": df.duplicated(subset=[col]).sum(),
            "sample_problematic": df[df[col].isnull()].head(3).to_dict(orient="records")
        }
    
    
    for col, info in report.items():
        print(f"\n=== Columna: {col} ===")
        for k, v in info.items():
            print(f"{k}: {v}")

if __name__ == "__main__":
    exploratory_quality_check()
