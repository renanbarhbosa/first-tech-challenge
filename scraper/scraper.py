from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
import pandas as pd
import time
import os

chrome_driver_path = r'C:\intellij-projects\postech\first-tech-challenge\chrome_driver\chromedriver-win64\chromedriver.exe'
output_dir = r'C:\intellij-projects\postech\first-tech-challenge\wine_data\grape_juice'

os.makedirs(output_dir, exist_ok=True)
service = Service(executable_path=chrome_driver_path)
driver = webdriver.Chrome(service=service)
base_url = "http://vitibrasil.cnpuv.embrapa.br/index.php"
anos = range(2024, 2008, -1)

for ano in anos:
    key_for_table_wine = "01"
    key_for_foaming_wine = "02"
    key_for_fresh_grapes = "03"
    key_for_grape_juice = "04"
    try:
        driver.get(f"{base_url}?ano={ano}&opcao=opt_06&subopcao=subopt_{key_for_grape_juice}")
        time.sleep(2)

        tabela = driver.find_element(By.XPATH, '/html/body/table[4]/tbody/tr/td[2]/div/div/table[1]')

        html_tabela = tabela.get_attribute('outerHTML')

        soup = BeautifulSoup(html_tabela, 'html.parser')
        dados = []

        for linha in soup.find_all('tr'):
            celulas = linha.find_all(['th', 'td'])
            dados.append([celula.get_text(strip=True) for celula in celulas])

        df = pd.DataFrame(dados)
        csv_path = os.path.join(output_dir, f'dados_{ano}.csv')
        df.to_csv(csv_path, index=False, header=False, encoding='utf-8')

        print(f"Dados de {ano} salvos em {csv_path}")

    except Exception as e:
        print(f"Erro em {ano}: {str(e)}")

driver.quit()
