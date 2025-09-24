import great_expectations as gx
from cleaning import cleaning_data  

def exploratory_quality_check_output():
    
    # datos luego de limpieza
    df_clean = cleaning_data()
    if df_clean is None:
        print("No se pudo obtener el dataframe limpio.")
        return
    

    ge_df = gx.from_pandas(df_clean)
    
    
    # Completeness / Nulls
    for col in ge_df.columns:
        ge_df.expect_column_values_to_not_be_null(col)
    
    # Uniqueness para IDs
    if 'transaction_id' in ge_df.columns:
        ge_df.expect_column_values_to_be_unique('transaction_id')
    if 'customer_id' in ge_df.columns:
        ge_df.expect_column_values_to_be_unique('customer_id')
    
    # Validity para amount
    if 'amount' in ge_df.columns:
        ge_df.expect_column_values_to_be_between('amount', min_value=0)
    
    # Validity para purchase_date
    if 'purchase_date' in ge_df.columns:
        ge_df.expect_column_values_to_be_of_type('purchase_date', 'datetime64[ns]')
    

    results = ge_df.validate(result_format="SUMMARY")
    
    print("\n=== REPORT DE CALIDAD DE DATOS LIMPIOS (GE) ===")
    print(f"Exito: {results['success']}")
    for res in results['results']:
        expectation_type = res['expectation_config']['expectation_type']
        column = res['expectation_config']['kwargs'].get('column', '')
        success = res['success']
        print(f"Expectation: {expectation_type} | Columna: {column} | Cumplida: {success}")
    
   

if __name__ == "__main__":
    exploratory_quality_check_output()
