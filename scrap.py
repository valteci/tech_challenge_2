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
    """
    Converte uma tabela HTML contendo dados por código em um DataFrame formatado.

    Esta função extrai o conteúdo HTML da tabela fornecida, converte os dados
    para um DataFrame do Pandas e realiza ajustes para garantir a formatação
    correta dos dados.

    Parâmetros:
    -----------
    tabela : WebElement
        Elemento HTML (obtido via Selenium) que representa a tabela de dados
        que será convertida.

    Retorno:
    --------
    pd.DataFrame
        Um DataFrame contendo as seguintes colunas:
        - 'Código': Código do ativo.
        - 'Ação': Nome da ação.
        - 'Tipo': Tipo da ação.
        - 'Qtde. Teórica': Quantidade teórica da ação (convertida para `int64`).
        - 'Part. (%)': Percentual de participação do ativo no índice (ajustado para escala decimal).

        Caso nenhuma tabela seja encontrada, a função retorna um DataFrame vazio.

    Fluxo do Processo:
    ------------------
    1. Obtém o HTML da tabela usando `.get_attribute('outerHTML')`.
    2. Converte o HTML em uma lista de DataFrames utilizando `pd.read_html`.
    3. Remove as duas últimas linhas da tabela, que possivelmente contêm totais ou valores indesejados.
    4. Divide os valores da coluna `'Part. (%)'` por 1000 para corrigir a escala.
    5. Remove os pontos (`.`) da coluna `'Qtde. Teórica'` e converte-a para o tipo inteiro (`int64`).
    6. Retorna o DataFrame processado ou um DataFrame vazio caso a lista `df_list` esteja vazia.

    Observação:
    ------------
    - A manipulação da string na coluna `'Qtde. Teórica'` visa remover separadores de milhar (`.`)
      e garantir que os valores sejam corretamente convertidos para inteiros.

    Exemplo de uso:
    ---------------
    df_codigo = _to_dataframe_codigo(tabela)
    print(df_codigo.head())
    """
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
    """
    Converte uma tabela HTML contendo dados de setores em um DataFrame formatado.

    Esta função extrai o conteúdo HTML da tabela fornecida, converte os dados
    para um DataFrame do Pandas e realiza ajustes nas colunas para padronizar
    e preparar os dados para análise.

    Parâmetros:
    -----------
    tabela : WebElement
        Elemento HTML (obtido via Selenium) que representa a tabela de dados
        que será convertida.

    Retorno:
    --------
    pd.DataFrame
        Um DataFrame contendo as seguintes colunas:
        - 'Código': Código do ativo.
        - 'Setor': Nome do setor.
        - 'Setor - Part. (%)': Percentual de participação do ativo no setor (ajustado para escala decimal).
        - 'Setor - Part. (%)Acum.': Percentual acumulado de participação no setor (ajustado para escala decimal).

    Fluxo do Processo:
    ------------------
    1. Obtém o HTML da tabela usando `.get_attribute('outerHTML')`.
    2. Converte o HTML em uma lista de DataFrames utilizando `pd.read_html`.
    3. Remove as duas últimas linhas da tabela, que possivelmente contêm totais ou valores indesejados.
    4. Reestrutura as colunas para garantir que nomes vazios ou `NaN` sejam corrigidos.
    5. Renomeia as colunas para:
        - 'Part. (%)' → 'Setor - Part. (%)'
        - 'Part. (%)Acum.' → 'Setor - Part. (%)Acum.'
    6. Ajusta os valores percentuais dividindo-os por 1000 para corrigir a escala.

    Exemplo de uso:
    ---------------
    df_setor = _to_dataframe_setor(tabela)
    print(df_setor.head())
    """
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
    """
    Realiza o scraping de dados por na tabela por código e retorna um DataFrame com as informações
    coletadas.

    Esta função utiliza o Selenium para acessar uma página web, navegar por páginas de uma tabela,
    e extrair dados relacionados aos códigos de ações e suas respectivas informações. Os dados são 
    organizados em um DataFrame do Pandas.

    Parâmetros:
    -----------
    Nenhum.

    Retorno:
    --------
    pd.DataFrame
        Um DataFrame contendo as seguintes colunas:
        - 'Código' : Código da ação (string).
        - 'Ação' : Nome da ação (string).
        - 'Tipo' : Tipo da ação (categoria, para eficiência).
        - 'Qtde. Teórica' : Quantidade teórica de ações (int64).
        - 'Part. (%)' : Percentual de participação da ação na composição do índice (float64).
        - 'Data' : Data da coleta dos dados (string).

    Fluxo do Processo:
    ------------------
    1. Configura o Selenium com o ChromeDriver no modo headless.
    2. Acessa a URL especificada.
    3. Inicializa um DataFrame vazio com as colunas especificadas.
    4. Itera pelas páginas (1 a 5), clicando em cada botão de paginação quando necessário.
    5. Em cada página, coleta a tabela e converte os dados para um DataFrame usando a função `_to_dataframe_codigo`.
    6. Os dados são concatenados em um DataFrame final.
    7. A coluna 'Qtde. Teórica' é convertida para o tipo inteiro e a data da coleta é adicionada na coluna 'Data'.

    Tratamento de Erros:
    --------------------
    - Se houver erro ao clicar na paginação ou carregar a tabela, uma mensagem é exibida,
      e a execução continua na próxima página.
    - Caso ocorra uma falha grave ao carregar a página ou a tabela, o driver é encerrado 
      e o programa é finalizado com `exit(1)`.

    Exemplo de uso:
    ---------------
    df_codigo = _scraping_por_codigo()
    print(df_codigo.head())
    """
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
    """
    Realiza o scraping de dados na tabela por setor e retorna um DataFrame com as informações
    coletadas.

    Esta função utiliza o Selenium para acessar uma página web, navegar por páginas de uma tabela
    e extrair dados relacionados aos setores e suas participações. Os dados são organizados em um
    DataFrame do Pandas.

    Parâmetros:
    -----------
    Nenhum.

    Retorno:
    --------
    pd.DataFrame
        Um DataFrame contendo as seguintes colunas:
        - 'Código': Código do ativo.
        - 'Setor': Nome do setor.
        - 'Setor - Part. (%)': Percentual de participação do ativo no setor.
        - 'Setor - Part. (%)Acum.': Percentual acumulado de participação do setor.

    Fluxo do Processo:
    ------------------
    1. Configura o Selenium com o ChromeDriver no modo headless.
    2. Acessa a URL especificada.
    3. Seleciona uma opção específica no elemento `<select>` com ID `'segment'`.
    4. Itera pelas páginas (1 a 5), clicando em cada botão de paginação quando necessário.
    5. Em cada página, coleta a tabela e converte os dados para um DataFrame usando a função `_to_dataframe_setor`.
    6. Os dados são concatenados em um DataFrame final, que é retornado.

    Tratamento de Erros:
    --------------------
    - Se houver erro ao clicar na paginação ou carregar a tabela, uma mensagem será exibida,
      e a função continua a execução na próxima página.
    - Caso ocorra uma falha grave ao carregar a página ou a tabela, o driver é encerrado
      e o programa é finalizado com `exit(1)`.

    Exemplo de uso:
    ---------------
    df_setor = _scraping_por_setor()
    print(df_setor.head())
    """
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
    """
    Faz o upload de um arquivo local para um bucket no Amazon S3.

    Esta função utiliza o cliente S3 para enviar um arquivo especificado para 
    o bucket indicado.

    Parâmetros:
    -----------
    filename : str
        O nome (ou caminho) do arquivo a ser enviado para o S3.
    bucket_name : str
        O nome do bucket no qual o arquivo será armazenado.
    """
    s3.upload(filename, bucket_name)


def _remove_file(file_path: str) -> None:
    """
    Remove um arquivo do sistema de arquivos, se ele existir.

    Esta função verifica se o arquivo especificado existe e, caso positivo,
    remove-o.

    Parâmetros:
    -----------
    file_path : str
        O caminho completo (ou relativo) do arquivo a ser removido.

    Observação:
    ------------
    - Caso o arquivo não exista, nenhuma ação é realizada e nenhum erro é gerado.

    Exemplo de uso:
    ---------------
    _remove_file('dados/dataset.parquet')
    """
    if os.path.exists(file_path):
            os.remove(file_path)


def _gerar_parquet(df: pd.DataFrame, filename: str) -> None:
    """
    Gera um arquivo Parquet a partir de um DataFrame.

    Esta função recebe um DataFrame e salva seu conteúdo em um arquivo
    no formato Parquet utilizando o motor `fastparquet`.

    Parâmetros:
    -----------
    df : pd.DataFrame
        O DataFrame que será salvo no arquivo Parquet.
    filename : str
        O nome do arquivo de saída, incluindo a extensão `.parquet`.

    Tratamento de Erros:
    --------------------
    Caso ocorra algum erro durante a geração do arquivo, uma mensagem
    de erro será exibida no console, indicando a causa do problema.
    """
    try:
        df.to_parquet(
            filename,
            engine='fastparquet',
            index=False
        )

    except Exception as e:
        print('Erro ao gerar parquet:', e)


def start():
    """
    Executa o processo completo de scraping, tratamento e envio de dados para o S3.

    Essa função realiza as seguintes etapas:
    1. Coleta dados por código usando a função `_scraping_por_codigo`.
    2. Coleta dados por setor usando a função `_scraping_por_setor`.
    3. Faz a junção (merge) dos dois DataFrames com base na coluna 'Código'.
    4. Gera um arquivo `.parquet` com os dados processados, nomeado com a data atual.
    5. Envia o arquivo gerado para um bucket S3 especificado.
    6. Remove o arquivo gerado localmente após o envio bem-sucedido.
    """
    df_codigo = _scraping_por_codigo()
    df_setor = _scraping_por_setor()
    df_final = pd.merge(df_codigo, df_setor, on='Código', how='inner')
    filename = f'{_data_de_hoje()}.parquet'
    _gerar_parquet(df_final, filename)
    _send_to_s3(filename, BUCKET_NAME)
    _remove_file(filename)

