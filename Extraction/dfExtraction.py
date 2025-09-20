import pandas as pd
import glob

def readCsvDf(dir, fileName=None):
    try: 
        if dir.endswith('.csv'):
            filesValidation = glob.glob(dir, recursive=False)
            df = pd.read_csv(filesValidation[0])

            return {
                'ok': True,
                'data': df, 
                'msg': f'The DataFrame extraction was successful'
            }

        elif fileName is None:
            filesValidation = glob.glob(f'{dir}/*', recursive=False)
            df = pd.read_csv(filesValidation[0])

            return {
                'ok': True,
                'data': df, 
                'msg': f'The file name was not provided, so the chosen dataset is {filesValidation[0]}'
            }

        fileValidation = glob.glob(f'{dir}/{fileName}', recursive=False)
        df = pd.read_csv(fileValidation[0])
        return {
            'ok': True,
            'data': df, 
            'msg': 'The DataFrame extraction was successful'
        }
    
    except Exception as e:
        return {
            'ok': False, 
            'data': None,
            'msg': f'There is no CSV file named {fileName}'
        }