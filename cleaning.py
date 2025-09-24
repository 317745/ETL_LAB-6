from Extraction.dfExtraction import readCsvDf
import pandas as pd
import numpy as np

def cleaning_data():
    
    readCsvRes = readCsvDf('data', 'retail_data.csv')
    if not readCsvRes['ok']:
        print(readCsvRes['msg'])
        return
    
    df_original = readCsvRes['data']
    
    df = df_original.copy()
    
    df = df.drop_duplicates()
    df.columns = ['transaction_id', 'purchase_date', 'product_category', 'amount', 'customer_id']

    columns = df.columns



    # ---Validity---

    # trasnformar purchase_date a formato datetime

    df['purchase_date'] = pd.to_datetime(df['purchase_date'], infer_datetime_format=True)
    print(f"purchase_date tipo luego de limpieza: {df['purchase_date'].dtype}")
    
    # asignar grupo a duplicados por cliente + fecha
    df['group_id'] = (
    df.groupby(['customer_id', 'purchase_date']).ngroup())

    mask = df.duplicated(subset=['purchase_date', 'customer_id'], keep=False)

    df['group_id'] = np.where(
        mask, 
        df.groupby(['customer_id', 'purchase_date']).ngroup(), 
        np.nan
    )

    # reasignar transaction_id con base en group_id
    df.loc[df['group_id'].notna(), 'transaction_id'] = df.loc[df['group_id'].notna(), 'group_id']

    # eliminar columna auxiliar
    df = df.drop('group_id', axis=1)
    print(len(df))
    
    #-----------------------------------------
    
    # eliminar filas con purchase_date nulo_----------- arreglar 
    before = len(df)
    df = df.dropna(subset=['purchase_date'])
    after = len(df)
    
    
   
    #imputaciones especificas-------------------------
    #strings
    
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    
    # product_category -> moda
    product_mode = df['product_category'].mode()[0]
    df['product_category'].fillna(product_mode, inplace=True)
    #print(f"Se ha imputado en las filas con faltantes de la columna 'product_category' con la moda: {product_mode}")

    
    # imputar Nan en amount con la media por categoria
    media_categoria = df.groupby('product_category')['amount'].transform('mean')
    missing_before = df['amount'].isna().sum()
    df['amount'] = df['amount'].fillna(media_categoria)
    missing_after = df['amount'].isna().sum()
    #print(f"Se imputaron {missing_before - missing_after} valores en 'amount' con la media por categoria.")

    
    
    # Completeness-----------------------------
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
            
      
            
        elif porcentaje_faltantes[col] <= 5 and porcentaje_faltantes_df < 10:
            print("Las filas con valores faltantes no representan más del 5% en la columna y menos del 10% en todo el dataset. "
                  "Procedemos a eliminar las filas con faltantes.")
            before = len(df)
            df = df.dropna(subset=[col])
            after = len(df)
            #print(f"Se eliminaron {before - after} filas con valores faltantes en '{col}'")
    
    
    
   #filtrar amount
   
   #numericos positivos
    df = df[df['amount'] > 0]
    after = len(df)
    #print(f"Se eliminaron {before - after} filas con valores no positivos en 'amount'")
        
    #print(f"amount tipo luego de limpieza: {df['amount'].dtype}")
  
    #--- Uniqueness ---
    
    df['transaction_id'] = pd.to_numeric(df['transaction_id'], errors='coerce')
    
    duplicados_id = df.duplicated(subset=['transaction_id'], keep=False)
    
    if duplicados_id.any():
        max_transaction_id = df['transaction_id'].max() + 1
        
        duplicados = df[df.duplicated(subset=['transaction_id'], keep='first')].index
        df.loc[duplicados, 'transaction_id'] = range(max_transaction_id, max_transaction_id + len(duplicados))


    
    return df

        
if __name__ == "__main__":
    df_clean = cleaning_data()  
    if df_clean is not None:
        df_clean.to_csv('data/retail_data_clean.csv', index=False)
        print("Dataset limpio guardado correctamente en 'data/retail_data_clean.csv'")
    






