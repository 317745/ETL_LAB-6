import great_expectations as gx
from Extraction.dfExtraction import readCsvDf
import pandas as pd

def cleaning_data():
    
    readCsvRes = readCsvDf('data', 'retail_data.csv')
    if not readCsvRes['ok']:
        print(readCsvRes['msg'])
        return
    
    df_original = readCsvRes['data']
    
    df = df_original.copy()
    
    df = df.rename(columns={
        "id": "customer_id",
        "customer_id": "transaction_id"
    })

    columns = df.columns



    # ---Validity---

    # trasnformar purchase_date a formato datetime

    df['purchase_date'] = pd.to_datetime(df['purchase_date'], infer_datetime_format=True)
    print(f"purchase_date tipo luego de limpieza: {df['purchase_date'].dtype}")
    
    # eliminar filas con purchase_date nulo
    before = len(df)
    df = df.dropna(subset=['purchase_date'])
    after = len(df)
    print(f"Se eliminaron {before - after} filas con valores nulos en 'purchase_date'")
    
    #amount
    
    #strings
    
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    
    #eliminar NaNs generados
    before = len(df)
    df = df.dropna(subset=['amount'])
    after = len(df)
    print(f"Se eliminaron {before - after} filas con valores no numéricos en 'amount'")
        
    #numericos positivos
    df = df[df['amount'] > 0]
    after = len(df)
    print(f"Se eliminaron {before - after} filas con valores no positivos en 'amount'")
        
    print(f"amount tipo luego de limpieza: {df['amount'].dtype}")
    
    
    # Completeness
    calculo_faltantes = df[columns].isnull().sum()
    porcentaje_faltantes = (calculo_faltantes / len(df)) * 100
    porcentaje_faltantes_df = (df.isnull().sum().sum() / (df.shape[0] * df.shape[1])) * 100
    
    for col in columns:
        
        if porcentaje_faltantes[col] > 5:
            print(f"Columnas con valores faltantes que requieren imputacion:\n{porcentaje_faltantes[porcentaje_faltantes > 0]}")
        
        if col == 'purchase_date':
            print("No es posible imputar la columna 'purchase_date' debido a su naturaleza de fecha. "
                  "Se recomienda tener esto en cuenta al realizar analisis posteriores.")
            
        elif col == 'customer_id':
            print("No es posible imputar la columna 'customer_id' debido a su naturaleza de id. "
                  "Se recomienda tener esto en cuenta al realizar analisis posteriores.")
        
        elif col == 'transaction_id':
            print("No es posible imputar la columna 'transaction_id' debido a su naturaleza de id. "
                  "Se recomienda tener esto en cuenta al realizar analisis posteriores.")
            
        elif col == 'product_category':
            product_mode = df['product_category'].mode()[0]
            df['product_category'].fillna(product_mode, inplace=True)
            print(f"Se ha imputado en las filas con faltantes de la columna 'product_category' con la moda: {product_mode}")
            
        elif col == 'amount':
            amount_mean = df['amount'].mean()
            df['amount'].fillna(amount_mean, inplace=True)
            print(f"Se ha imputado en las filas con faltantes de la columna 'amount' con la media: {amount_mean}")
            
        elif porcentaje_faltantes[col] <= 5 and porcentaje_faltantes_df < 10:
            print("Las filas con valores faltantes no representan más del 5% en la columna y menos del 10% en todo el dataset. "
                  "Procedemos a eliminar las filas con faltantes.")
            before = len(df)
            df = df.dropna(subset=[col])
            after = len(df)
            print(f"Se eliminaron {before - after} filas con valores faltantes en '{col}'")

    #--- Uniqueness ---
    
    # Eliminar duplicados exactos por fecha, product_category y amount
    df_clean = df.drop_duplicates(subset=['purchase_date', 'product_category', 'amount'], keep='first')

    
    #ordenar transaction_id segun fecha 
    df_clean = df_clean.sort_values(by='purchase_date').reset_index(drop=True)

    # Generar transaction_id secuencial
    df_clean['transaction_id'] = df_clean.index + 1

    print(f"Transaction_id únicos DESPUÉS: {df_clean['transaction_id'].nunique()}")
    print(f"Duplicados DESPUÉS: {df_clean.duplicated(subset=['transaction_id']).sum()}")
    print("Se ha regenerado 'transaction_id' ")
    
    
    return df_clean

        
if __name__ == "__main__":
    df_clean = cleaning_data()  
    if df_clean is not None:
        df_clean.to_csv('data/retail_data_clean.csv', index=False)
        print("Dataset limpio guardado correctamente en 'data/retail_data_clean.csv'")
    






