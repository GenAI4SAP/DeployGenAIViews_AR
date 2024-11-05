import pandas as pd
import requests
import os
import json

with open('config/config.json', 'r') as f:
    config_data = json.load(f)

project_id_tgt = config_data['project_id_tgt']
dataset_reporting_tgt = config_data['dataset_reporting_tgt']
training_url = config_data['training_url']
admin_api_key = config_data['admin_api_key']
archive = 'files/training_data.xlsx'

def change_address(sql):
    old_address = "project_id_tgt.dataset_reporting_tgt.AccountingDocumentsReceivable"
    new_address = f"{project_id_tgt}.{dataset_reporting_tgt}.AccountingDocumentsReceivable"
    return sql.replace(old_address, new_address)

def proccess(archive):
    df = pd.read_excel(archive)

    if 'sql' in df.columns and 'question' in df.columns:
        for index, row in df.iterrows():
            question = row['question']
            sql = change_address(row['sql'])
            agent_id = row['agent_id'] 

            data = {
                "question": question,
                "sql": sql,
                "agent_id": agent_id
            }

            headers = {"ADMIN-API-KEY": admin_api_key, "Content-Type": "application/json"}
            response = requests.post(training_url, headers=headers, json=data)

            if response.status_code == 200:
                print(f"Linha {index} enviada com sucesso - Training ID: {json.loads(response.content)['id']}")
            else:
                print(f"Erro ao enviar linha {index}: {response.status_code}")

proccess(archive)