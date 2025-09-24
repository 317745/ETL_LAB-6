from Extraction.dfExtraction import readCsvDf
from input import exploratory_quality_check as input_quality_report
from quality import exploratory_quality_check as detailed_quality_check
from cleaning import cleaning_data  
from quality_out import exploratory_quality_check_output

def main():
    readCsvRes = readCsvDf('data', 'retail_data.csv')
    if readCsvRes['ok'] == True:
        print(readCsvRes['data'])
    else:
        return readCsvRes
    
    print("input quality report")
    input_quality_report()
    
    print("detailed quality report")
    detailed_quality_check()
    
    print("\n data cleaning")
    cleaning_data()
    
    print("\n data cleaning")
    df_clean = cleaning_data()
    if df_clean is not None: 
        df_clean.to_csv('data/retail_data_clean.csv', index=False)
        print("Dataset limpio guardado correctamente en 'data/retail_data_clean.csv'")
    
    print("\n output quality report")
    exploratory_quality_check_output()

if __name__ == "__main__":
    main()