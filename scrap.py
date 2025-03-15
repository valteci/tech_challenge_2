from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import s3
from io import StringIO
import time
from datetime import datetime
import os


BUCKET_NAME = 'valteci-b3-raw'
URL = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br"

def _data_de_hoje() -> str:
    """Retorna a data de hoje no formato dd-mm-YYYY"""
    return datetime.strftime(datetime.now(), '%d-%m-%Y')


def _to_dataframe_codigo(tabela)-> pd.DataFrame:
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


def _to_dataframe_setor(tabela)-> pd.DataFrame:
    html = tabela.get_attribute('outerHTML')

    df_list = pd.read_html(
        StringIO(html), 
        header=[0, 1], 
        decimal=','
    )

    df = df_list[0].iloc[:-2]

    df.columns = [col[0] if (pd.isna(col[1]) or col[1]=='') else col[1] for col in df.columns]
    df_final = df[['Código', 'Setor', 'Part. (%)', 'Part. (%)Acum.']].copy()
    df_final = df_final.rename(columns={
        'Part. (%)': 'Setor - Part. (%)',
        'Part. (%)Acum.': 'Setor - Part. (%)Acum.'
    })

    df_final.loc[:, 'Setor - Part. (%)'] /= 1000
    df_final.loc[:, 'Setor - Part. (%)Acum.'] /= 1000

    return df_final


def _scraping_por_codigo() -> pd.DataFrame:
    print('\n\n===========Iniciando scraping por código===========\n\n')
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    driver.get(URL)

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
                except Exception as err:
                    print(f"Erro ao clicar na página {i}: {err}")
                    continue

            tabela = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, 'table'))
            )
            
            print(f"\nPágina {i} foi precessada com sucesso!\n")
            df = _to_dataframe_codigo(tabela)
            df_final = pd.concat([df_final, df], ignore_index=True)
        
        df_final['Qtde. Teórica'] = df_final['Qtde. Teórica'].astype(int)
        df_final['Data'] = _data_de_hoje()
        df_final['Data'] = df_final['Data'].astype('str')
        
        return df_final

    except Exception as e:
        print(e)
        print("Erro ao carregar a tabela por código.")
        driver.quit()
        exit(1)


def _scraping_por_setor() -> pd.DataFrame:
    print('\n\n===========Iniciando scraping por setor===========\n\n')
    try:
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.get(URL)

        select_element = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, 'segment'))
        )

        select = Select(select_element)
        select.select_by_value('2')
        time.sleep(2)


        try:
            df_final = pd.DataFrame({
            'Código': pd.Series(dtype='str'),   
            'Setor': pd.Series(dtype='str'),     
            'Setor - Part. (%)': pd.Series(dtype='float64'),
            'Setor - Part. (%)Acum.': pd.Series(dtype='float64') 
            })

            for i in range(1, 6):
                if i > 1:
                    try:
                        pagina_botao = WebDriverWait(driver, 10).until(
                            EC.element_to_be_clickable((By.XPATH, f"//ul[contains(@class, 'ngx-pagination')]/li/a[span[text()='{i}']]"))
                        )

                        pagina_botao.click()
                        time.sleep(2)
                    except Exception as error:
                        print(f"Erro ao clicar na página {i}: {error}")
                        continue

                tabela = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, 'table'))
                )

                print(f"\nPágina {i} foi precessada com sucesso!\n")
                df = _to_dataframe_setor(tabela)
                df_final = pd.concat([df_final, df], ignore_index=True)
            
            return df_final

        except Exception as e:
            print("erro: ", e)


    except Exception as err:
        print(err)
        print("Erro ao carregar a tabela por setor")
        driver.quit()
        exit(1)


def _send_to_s3(filename: str, bucket_name: str) -> None:
    s3.upload(filename, bucket_name)


def _remove_file(file_path: str) -> None:
    if os.path.exists(file_path):
            os.remove(file_path)


def _gerar_parquet(df: pd.DataFrame, filename: str) -> None:
    try:
        df.to_parquet(
            filename,
            engine='fastparquet',
            index=False
        )

    except Exception as e:
        print('Erro ao gerar parquet:', e)
 

def start():
    df_codigo = _scraping_por_codigo()
    df_setor = _scraping_por_setor()
    df_final = pd.merge(df_codigo, df_setor, on='Código', how='inner')
    filename = f'{_data_de_hoje()}.parquet'
    _gerar_parquet(df_final, filename)
    _send_to_s3(filename, BUCKET_NAME)
    _remove_file(filename)

