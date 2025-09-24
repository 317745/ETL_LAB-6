#%% Imports
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv(r"C:\Users\Admin\Downloads\ETL_LAB-6-main\data\retail_data_clean.csv")
df_original = pd.read_csv(r"C:\Users\Admin\Downloads\ETL_LAB-6-main\data\retail_data.csv")

print(df.head())
print(df.columns)

#%% KPI 1:  Presicion de ingresos luego de limpieza
df_original['amount'] = pd.to_numeric(df_original['amount'], errors='coerce')

ingresos_antes = df_original['amount'].sum()
ingresos_despues = df['amount'].sum()
precision= (ingresos_despues/ ingresos_antes) * 100


kpi_df = pd.DataFrame({
    'Estado': ['Antes de limpiar', 'Después de limpiar'],
    'Ingresos': [ingresos_antes, ingresos_despues]
})

# Gráfico de barras
plt.figure(figsize=(8,5))
bars = plt.bar(kpi_df['Estado'], kpi_df['Ingresos'], color=['#FF5733', '#4CAF50'])
plt.ylabel('Ingresos Totales')
plt.title(f'Revenue Accuracy Improvement ({precision:.2f}%)')

for bar in bars:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2, yval + 500, f"${yval:,.0f}", ha='center', va='bottom')

plt.show()

#%% KPI 2:  Crecimiento de ventas
df['purchase_date'] = pd.to_datetime(df['purchase_date'])


monthly_sales = df.groupby(pd.Grouper(key='purchase_date', freq='M'))['amount'].sum().reset_index()

# Crear gráfico
fig, ax = plt.subplots(figsize=(8,4))
ax.scatter(monthly_sales['purchase_date'], monthly_sales['amount'])
ax.plot(monthly_sales['purchase_date'], monthly_sales['amount'])
ax.set_ylabel('Total Sales ($)')
ax.set_xlabel('Month')
ax.set_title('Monthly Sales in 2025')
plt.xticks(rotation=-20)
plt.show()

#%% KPI 3: Categorias con mas ventas

sales_by_category = df.groupby('product_category')['amount'].sum().sort_values(ascending=False)

# gráfico
sales_by_category.plot(kind='bar', figsize=(8,4), title='Ventas totales por categoría')
plt.ylabel('Monto total ($)')
plt.xlabel('Categoría')
plt.show()

# %% KPI 4: 

# Porcentaje de registros con ids unicos
unique_transaction_rate = df['transaction_id'].nunique() / len(df) * 100

plt.figure(figsize=(6,6))
plt.pie([unique_transaction_rate, 100 - unique_transaction_rate], labels=['Únicos', 'Duplicados'], autopct='%1.1f%%', colors=["#6AB2AB", '#FF5733'])
plt.title('Porcentaje de IDs de Transacción Únicos')
plt.show()


