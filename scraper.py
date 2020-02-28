
# -*- coding: utf-8 -*-
import os
from datetime import datetime
import pandas as pd
import shutil
import requests
import scraperwiki
from requests_html import HTMLSession


def download_file(url, file_path):
    response = requests.get(url, stream=True)
    
    if response.status_code != 200:
        print('Arquivo não encontrado', url, response.status_code)
        return False

    with open(file_path, "wb") as handle:
        print('Downloading', url)
        for data in response.iter_content():
            handle.write(data)    
    handle.close()
    return True

    
def create_download_folder():
    # Create directory
    dirName = os.path.join('downloads')
 
    try:
        # Create target Directory
        os.mkdir(dirName)
        print("Directory", dirName, "Created ")
    except Exception:
        print("Directory", dirName, "already exists")


def main():
    create_download_folder()

    for link in get_list_files_cvm_site():
        if not link.endswith('.csv'):
            continue
        
        file_name = link.split('/')[-1]
        file_path = os.path.join('downloads', file_name)
        print('Fazendo download do arquivo', link)
        download_arquivo(link, file_path)

    return True


def download_arquivo(url, file_path):
    # morph.io requires this db filename, but scraperwiki doesn't nicely
    # expose a way to alter this. So we'll fiddle our environment ourselves
    # before our pipeline modules load.
    os.environ['SCRAPERWIKI_DATABASE_NAME'] = 'sqlite:///data.sqlite'

    file_name = url.split('/')[-1]
    file_path = os.path.join('downloads', file_name)

    # faz o download do arquivo na pasta       
    if download_file(url, file_path):
        processa_arquivo(file_path)

    return True


def get_list_files_cvm_site():
    session = HTMLSession()
    
    url = 'http://dados.cvm.gov.br/dados/FI/DOC/PERFIL_MENSAL/DADOS/'
    r = session.get(url)    
    
    if r.status_code != 200:
        print('Erro ao acessar site da CVM')
        return False

    links = []
    for link in r.html.absolute_links:
        if not link.endswith('.csv'):
            continue
        links.append(link)

    return links


def processa_arquivo(file_path):
    try:
        df = pd.read_csv(file_path, sep=';', encoding='latin1')
    except Exception as e:
        print('Erro ao ler arquivo', file_path, e)
        return False

    # transforma o campo saldo em número
    print(df.columns)

    df['DT_REF'] = datetime.today().strftime('%Y-%m-%d')

    # transforma o campo CNPJ_CIA e CNPJ_AUDITOR
    df['CNPJ_FUNDO'] = df['CNPJ_FUNDO'].str.replace('.','')
    df['CNPJ_FUNDO'] = df['CNPJ_FUNDO'].str.replace('/','')
    df['CNPJ_FUNDO'] = df['CNPJ_FUNDO'].str.replace('-','')
    df['CNPJ_FUNDO'] = df['CNPJ_FUNDO'].str.zfill(14)

    df = df.astype(str)

    # remove os caracteres em brancos do nome das colunas
    df.rename(columns=lambda x: x.strip(), inplace=True)

    for row in df.to_dict('records'):
        scraperwiki.sqlite.save(unique_keys=['CNPJ_FUNDO', 'DT_COMPTC'], data=row)

    print('{} Registros importados com sucesso'.format(len(df)))

    return True


if __name__ == '__main__':
    main()

    # rename file
    print('Renomeando arquivo sqlite')
    if os.path.exists('scraperwiki.sqlite'):
        shutil.copy('scraperwiki.sqlite', 'data.sqlite')
