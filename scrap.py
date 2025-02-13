from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time

options = Options()
options.add_argument('--headless')
options.add_argument('--disable-gpu')
options.add_argument('--no-sandbox')

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)

url = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br"

driver.get(url)

try:
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

        print(f"\n=== HTML da tabela - Página {i} ===\n")
        print(tabela.get_attribute('outerHTML'))

except:
    print("Erro ao carregar a tabela.")
    driver.quit()
    exit(1)


