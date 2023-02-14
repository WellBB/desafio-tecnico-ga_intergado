# Instala a API do Banco Central e o módulo para ler arquivos xls
import subprocess
def install_package(package):
    subprocess.run(f"pip install {package}", shell=True)
install_package("python-bcb")
install_package("xlrd")

# Importa as bibliotecas necessárias
from airflow import DAG
import airflow.utils.dates
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
from bcb import sgs
import os

# Define o dicionário que será usado como argumentos padrão para cada tarefa na DAG
default_args = {
    'owner': 'Wellington_Broering',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': False
}

# Define o diretório padrão das dags
dags_path = "/opt/airflow/dags/"

# Define o diretório padrão dos dataframes principais
data_path = "/opt/airflow/dags/data/"

# Define o diretório padrão dos dataframes temporários
temp_path = "/opt/airflow/dags/temp/"

# Define a DAG
dag = DAG(
    'dag_importacao_dados_avaliacao_tecnica',
    default_args=default_args,
    description='DAG para importação, tratamento, atualização e carga de dados do desafio técnico proposto pela GA + Intergado para a vaga de Engenheiro/Analista de Dados',
    schedule_interval=None,
)

# Define a tarefa de carga de dados
def _carga_dados(**kwargs):

    '''
    Carrega os dados do arquivo 'cepea-consulta-20230116155544.xls', busca os dados do IPCA e pré-trata dados do dataframe
    '''

    # Carrega o dataframe para a memória e converte a coluna 'Data' adequadamente
    df_cepea_consulta = pd.read_excel(data_path + 'cepea-consulta-20230116155544.xls', header=3,
                                      parse_dates=['Data'], date_parser=lambda x: pd.to_datetime(x, format='%m/%Y'))
    
    # Preenche as datas faltantes com o mês anterior +1, no caso de existir, ou com o mês seguinte -1
    for f in df_cepea_consulta.index:
        if pd.isna(df_cepea_consulta['Data'][f]) == True:
            try:
                df_cepea_consulta.at[f, 'Data'] = df_cepea_consulta.at[f-1, 'Data'] + pd.DateOffset(months=1)
            except:
                df_cepea_consulta.at[f, 'Data'] = df_cepea_consulta.at[f+1, 'Data'] + pd.DateOffset(months=-1)

    # Completa os campos de valores que não estejam preenchidos com o valor do mês anterior
    df_cepea_consulta['Valor'].fillna(method='ffill', inplace=True)

    # Converte a coluna 'Valor' para float
    df_cepea_consulta['Valor'].replace(',', '.', regex = True, inplace = True)
    df_cepea_consulta['Valor'] = df_cepea_consulta['Valor'].astype(float)

    # Busca a série do IPCA com os dados necessários
    ipca_mes = sgs.get({'ipca': 433}, start = df_cepea_consulta['Data'].iloc[0], end = df_cepea_consulta['Data'].iloc[-1])

    # Reseta o índice e altera o nome das colunas para facilitar o merge
    ipca_mes.reset_index(inplace=True)
    ipca_mes.rename(columns = {'Date': 'Data', 'ipca': 'IPCA'}, inplace = True)

    # Salva o nome dos arquivos temporários
    temp_df_cepea_consulta = temp_path + 'df_cepea_consulta.parquet'
    temp_ipca_mes = temp_path + 'ipca_mes.parquet'

    # Salva os dataframes em parquet para push e pull
    df_cepea_consulta.to_parquet(temp_df_cepea_consulta)
    ipca_mes.to_parquet(temp_ipca_mes)

    # Retorna o diretório dos dataframes para XCom
    kwargs['ti'].xcom_push(key='temp_df_cepea_consulta', value=temp_df_cepea_consulta)
    kwargs['ti'].xcom_push(key='temp_ipca_mes', value=temp_ipca_mes)

tarefa_carga_dados = PythonOperator(
    task_id='carga_dados',
    python_callable=_carga_dados,
    dag=dag,
)

# Define a tarefa de tratamento de dados
def _tratamento_dados(**kwargs):

    '''
    Faz o tratamento de dados dos dataframes para possibilitar o merge
    '''

    # Carrega o diretório dos dataframes anteriores
    temp_df_cepea_consulta = kwargs['ti'].xcom_pull(key='temp_df_cepea_consulta', task_ids='carga_dados')
    temp_ipca_mes = kwargs['ti'].xcom_pull(key='temp_ipca_mes', task_ids='carga_dados')

    # Carrega os dataframes anteriores
    df_cepea_consulta = pd.read_parquet(temp_df_cepea_consulta)
    ipca_mes = pd.read_parquet(temp_ipca_mes)

    # Adiciona a coluna IPCA dando merge pela coluna 'Data'
    df_cepea_consulta = pd.merge(df_cepea_consulta, ipca_mes, how = 'left', on = 'Data')

    # Calcula o campo IPCA acumulado com a soma acumulada do campo IPCA
    df_cepea_consulta['IPCA_acumulado'] = df_cepea_consulta['IPCA'].cumsum()

    # Calcula o campo Real com o valor corrigido pelo IPCA de 12/2022
    ipca_12_2022 = df_cepea_consulta.loc[df_cepea_consulta['Data'] == '2022-12-01', 'IPCA_acumulado'].iloc[0]
    df_cepea_consulta['Real'] = df_cepea_consulta['Valor'] + (df_cepea_consulta['Valor'] * (ipca_12_2022 - df_cepea_consulta['IPCA_acumulado']) / 100)
    df_cepea_consulta['Real'] = df_cepea_consulta['Real'].round(2)

    # Salva o nome dos arquivos temporários
    temp_df_cepea_consulta = temp_path + 'df_cepea_consulta.parquet'

    # Salva os dataframes em parquet para push e pull
    df_cepea_consulta.to_parquet(temp_df_cepea_consulta)

    # Retorna o diretório do dataframe para XCom
    kwargs['ti'].xcom_push(key='temp_df_cepea_consulta', value=temp_df_cepea_consulta)

tarefa_tratamento_dados = PythonOperator(
    task_id='tratamento_dados',
    python_callable=_tratamento_dados,
    dag=dag,
)

# Define a tarefa de tratamento de dados
def _upsert_dados(**kwargs):

    '''
    Faz o upsert dos dados no arquivo 'boi_gord_base.csv' com base no dataframe tratado 'df_cepea_consulta_final'
    '''

    # Carrega o diretório dos dataframes anteriores
    temp_df_cepea_consulta = kwargs['ti'].xcom_pull(key='temp_df_cepea_consulta', task_ids='tratamento_dados')

    # Carrega o dataframe anteriore
    df_cepea_consulta_final = pd.read_parquet(temp_df_cepea_consulta)

    # Carregamento do dataframe principal para a memória
    df_boi_gordo = pd.read_csv(data_path + 'boi_gordo_base.csv')

    # Converte as colunas necessárias
    df_boi_gordo['dt_cmdty'] = pd.to_datetime(df_boi_gordo['dt_cmdty'], format='%Y-%m-%d')
    df_boi_gordo['cmdty_vl_rs_um'] = df_boi_gordo['cmdty_vl_rs_um'].astype(float)
    df_boi_gordo['cmdty_var_mes_perc'] = df_boi_gordo['cmdty_var_mes_perc'].astype(float)
    df_boi_gordo['dt_etl'] = pd.to_datetime(df_boi_gordo['dt_etl'], format='%Y-%m-%d')

    # Modifica o 'df_cepea_consulta' para o merge criando um 'df_cepea_consulta_merge'
    df_cepea_consulta_merge = df_cepea_consulta_final.rename(columns = {'Data': 'dt_cmdty'})

    # Faz o merge pré-carregando o novo df_boi_gordo
    df_boi_gordo = pd.merge(df_boi_gordo, df_cepea_consulta_merge[['dt_cmdty','Real']], how = 'left', on = 'dt_cmdty')

    # Calculada a coluna 'cmdty_var_mes_perc', atualiza a coluna 'cmdty_vl_rs_um' e exclui a coluna temporária 'Real'
    df_boi_gordo['cmdty_var_mes_perc'] = (df_boi_gordo['Real'] - df_boi_gordo['cmdty_vl_rs_um']) / df_boi_gordo['cmdty_vl_rs_um']
    df_boi_gordo['cmdty_vl_rs_um'] = df_boi_gordo['Real']
    del df_boi_gordo['Real']

    # Salva o nome dos arquivos temporários
    temp_df_boi_gordo = temp_path + 'df_boi_gordo.parquet'

    # Salva os dataframes em parquet para push e pull
    df_boi_gordo.to_parquet(temp_df_boi_gordo)

    # Retorna o diretório do dataframe para XCom
    kwargs['ti'].xcom_push(key='temp_df_boi_gordo', value=temp_df_boi_gordo)

tarefa_upsert_dados = PythonOperator(
    task_id='upsert_dados',
    python_callable=_upsert_dados,
    dag=dag,
)

# Define a tarefa de salvamento do arquivo
def _salva_arquivo(**kwargs):

    '''
    Salva o dataframe 'df_boi_gordo' no formato parquet
    '''

    # Carrega o diretório dos dataframes anteriores
    temp_df_boi_gordo = kwargs['ti'].xcom_pull(key='temp_df_boi_gordo', task_ids='upsert_dados')

    # Carrega os dataframes anteriores
    df_boi_gordo = pd.read_parquet(temp_df_boi_gordo)

    # Prepara para salvar o dataframe
    data_atual = datetime.now()
    nome_arquivo_atualizado = 'boi_gordo_base_atualizado_' + data_atual.strftime('%d_%m_%Y') + '.parquet'

    # Altera a coluna 'dt_etl' para a data atual
    df_boi_gordo['dt_etl'] = pd.to_datetime(data_atual.strftime('%Y-%m-%d'), format='%Y-%m-%d')

    # Salva o dataframe em parquet com as colunas necessárias
    df_boi_gordo.to_parquet(dags_path + nome_arquivo_atualizado)

tarefa_salva_arquivo = PythonOperator(
    task_id='salva_arquivo',
    python_callable=_salva_arquivo,
    dag=dag,
)

def _exclui_temporarios(**kwargs):

    '''
    Exclui arquivos .parquet temporários criados para passar os dataframes entre as tarefas individualizadas
    '''
    
    # Carrega o diretório dos dataframes anteriores
    temp_ipca_mes = kwargs['ti'].xcom_pull(key='temp_ipca_mes', task_ids='carga_dados')
    temp_df_cepea_consulta = kwargs['ti'].xcom_pull(key='temp_df_cepea_consulta', task_ids='tratamento_dados')
    temp_df_boi_gordo = kwargs['ti'].xcom_pull(key='temp_df_boi_gordo', task_ids='upsert_dados')

    # Verifica se os arquivos existem e exclui
    if os.path.exists(temp_df_cepea_consulta):
        os.remove(temp_df_cepea_consulta)
    if os.path.exists(temp_ipca_mes):
        os.remove(temp_ipca_mes)
    if os.path.exists(temp_df_boi_gordo):
        os.remove(temp_df_boi_gordo)

tarefa_exclui_temporarios = PythonOperator(
    task_id='exclui_temporarios',
    python_callable=_exclui_temporarios,
    dag=dag,
)

validacao = BashOperator(
    task_id='validacao_etl',
    bash_command='echo "Processo de ETL executado com sucesso!"',
    dag=dag,
)

# Especifica as dependências entre as tarefas
tarefa_carga_dados >> tarefa_tratamento_dados >> tarefa_upsert_dados >> tarefa_salva_arquivo >> tarefa_exclui_temporarios >> validacao