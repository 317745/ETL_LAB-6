from Extraction.dfExtraction import readCsvDf
def main():

    readCsvRes = readCsvDf('data', 'retail_data.csv')
    if readCsvRes['ok'] == True:
        print(readCsvRes['data'])
    else:
        return readCsvRes


if __name__ == "__main__":
    main()