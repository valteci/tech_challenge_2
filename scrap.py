from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import s3
from io import StringIO
import time
from datetime import datetime
import os


BUCKET_NAME = 'valteci-b3-raw'

def _to_dataframe(tabela)-> pd.DataFrame:
    html = tabela.get_attribute('outerHTML')
    df_list = pd.read_html(
        StringIO(html),
        decimal=',',
    )

    df = df_list[0].iloc[:-2]

    df.loc[:, 'Part. (%)'] /= 1000
    df.loc[:, 'Qtde. Teórica'] = df['Qtde. Teórica'].str.replace('.', '', regex=False).astype('int64')

    if df_list:
        return df
    else:
        return pd.DataFrame()

def _scraping() -> str:
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    url = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br"

    driver.get(url)

    try:
        df_final = pd.DataFrame({
        'Código': pd.Series(dtype='str'),              # Tipo string
        'Ação': pd.Series(dtype='str'),                # Tipo string
        'Tipo': pd.Series(dtype='category'),           # Tipo categoria (para eficiência)
        'Qtde. Teórica': pd.Series(dtype='int64'),     # Tipo inteiro
        'Part. (%)': pd.Series(dtype='float64')        # Tipo decimal
        })
        for i in range(1, 6):
            if i > 1:
                try:
                    pagina_botao = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.XPATH, f"//ul[contains(@class, 'ngx-pagination')]/li/a[span[text()='{i}']]"))
                    )

                    pagina_botao.click()
                    time.sleep(2)
                except Exception as e:
                    print(f"Erro ao clicar na página {i}: {e}")
                    continue

            tabela = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, 'table'))
            )

            print(f"\nPágina {i} foi precessada com sucesso!\n")
            df = _to_dataframe(tabela)
            df_final = pd.concat([df_final, df], ignore_index=True)
        
        df_final['Qtde. Teórica'] = df_final['Qtde. Teórica'].astype(int)
        date = datetime.strftime(datetime.now(), '%d-%m-%Y')
        df_final['Data'] = date
        df_final['Data'] = df_final['Data'].astype('str')
        filename = f'b3-{date}.parquet'
        df_final.to_parquet(filename, engine='fastparquet', index=False)
        
        return filename

    except Exception as e:
        print(e)
        print("Erro ao carregar a tabela.")
        driver.quit()
        exit(1)

def _send_to_s3(filename: str, bucket_name: str) -> None:
    s3.upload(filename, bucket_name)

def _remove_file(file_path: str) -> None:
    if os.path.exists(file_path):
            os.remove(file_path)


def start():
    filename = _scraping()
    _send_to_s3(filename, BUCKET_NAME)
    _remove_file(filename)