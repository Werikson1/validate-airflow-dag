# ===========================================================================================
# Objeto............: dag_acao_fluxo_load_daily
# Version...........: 2.0
# Data Criacao......: 19/09/2022
# Data Atualizacao..: 19/09/2022
# Projeto...........: VS Consumidor
# Descricao.........: Dag para execução VS Consumidor em Trusted/refined 
# Departamento......: Arquitetura e Engenharia de Dados
# Autor.............: werikson.rodrigues@grupoboticario.com.br 
# Git: https://github.com/grupoboticario/data-sql-sensitive/blob/main/dags/
# ===========================================================================================

# Alteração
# Adicionando Task da prc_load_tb_acao_fluxo_voucher_rfd  
# Data: 27/10/2022

import airflow
from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.operators.python import PythonOperator,BranchPythonOperator
from libs.airflow import log_and_slack, log
from airflow.models import Variable

# Coleta variaveis do Airflow
DAG_NAME = "dag_acao_fluxo_load_daily"
env_var = Variable.get(DAG_NAME, deserialize_json=True)

# Variaveis de Projeto
SCHEDULE_INTERVAL = env_var["SCHEDULE_INTERVAL"]
VAR_PRJ_RAW = env_var["VAR_PRJ_RAW"]
VAR_PRJ_SENSITIVE_TRUSTED = env_var["VAR_PRJ_SENSITIVE_TRUSTED"]
VAR_PRJ_SENSITIVE_RAW = env_var["VAR_PRJ_SENSITIVE_RAW"]
VAR_PRJ_TRUSTED = env_var["VAR_PRJ_TRUSTED"]
VAR_PRJ_REFINED = env_var["VAR_PRJ_REFINED"]
VAR_PRJ_SENSITIVE_REFINED = env_var["VAR_PRJ_SENSITIVE_REFINED"]

default_args = {
    "owner": "Gerencia: Front, Coord: Front 1",
    "on_failure_callback": log,
    "on_success_callback": log,
    "on_retry_callback": log,
    "sla_miss_callback": log,
    'start_date': airflow.utils.dates.days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    DAG_NAME,
    default_args=default_args,
    description='Executa procedures de carga da camada sensitive-trusted/refined de Ação de Fluxo',
    schedule_interval=SCHEDULE_INTERVAL,
    tags=["Chapter/VS: Consumidor", "Projeto: Loja Digital", "Trusted", "Refined"]
)


prc_load_tb_acao_fluxo_alerta = BigQueryExecuteQueryOperator(
    task_id='prc_load_tb_acao_fluxo_alerta',
    sql=f"CALL `{VAR_PRJ_SENSITIVE_TRUSTED}.sp.prc_load_tb_acao_fluxo_alerta`(NULL, NULL, '{VAR_PRJ_TRUSTED}', NULL,'{VAR_PRJ_SENSITIVE_RAW}', '{VAR_PRJ_SENSITIVE_TRUSTED}', NULL);",
    use_legacy_sql=False,
    priority="BATCH",
    dag=dag,
    depends_on_past=False
)

prc_load_tb_acao_fluxo_campanha = BigQueryExecuteQueryOperator(
    task_id='prc_load_tb_acao_fluxo_campanha',
    sql=f"CALL `{VAR_PRJ_SENSITIVE_TRUSTED}.sp.prc_load_tb_acao_fluxo_campanha`(NULL, NULL, '{VAR_PRJ_TRUSTED}', NULL,'{VAR_PRJ_SENSITIVE_RAW}', '{VAR_PRJ_SENSITIVE_TRUSTED}', NULL);",
    use_legacy_sql=False,
    priority="BATCH",
    dag=dag,
    depends_on_past=False
)

prc_load_tb_acao_fluxo_quiz = BigQueryExecuteQueryOperator(
    task_id='prc_load_tb_acao_fluxo_quiz',
    sql=f"CALL `{VAR_PRJ_SENSITIVE_TRUSTED}.sp.prc_load_tb_acao_fluxo_quiz`(NULL, NULL, '{VAR_PRJ_TRUSTED}', NULL,'{VAR_PRJ_SENSITIVE_RAW}', '{VAR_PRJ_SENSITIVE_TRUSTED}', NULL);",
    use_legacy_sql=False,
    priority="BATCH",
    dag=dag,
    depends_on_past=False
)

prc_load_tb_acao_fluxo_voucher = BigQueryExecuteQueryOperator(
    task_id='prc_load_tb_acao_fluxo_voucher',
    sql=f"CALL `{VAR_PRJ_SENSITIVE_TRUSTED}.sp.prc_load_tb_acao_fluxo_voucher`(NULL, NULL, '{VAR_PRJ_TRUSTED}', NULL,'{VAR_PRJ_SENSITIVE_RAW}', '{VAR_PRJ_SENSITIVE_TRUSTED}', NULL);",
    use_legacy_sql=False,
    priority="BATCH",
    dag=dag,
    depends_on_past=False
)

prc_load_tb_acao_fluxo_loja = BigQueryExecuteQueryOperator(
    task_id='prc_load_tb_acao_fluxo_loja',
    sql=f"CALL `{VAR_PRJ_SENSITIVE_TRUSTED}.sp.prc_load_tb_acao_fluxo_loja`(NULL, NULL, '{VAR_PRJ_TRUSTED}', NULL,'{VAR_PRJ_SENSITIVE_RAW}', '{VAR_PRJ_SENSITIVE_TRUSTED}', NULL);",
    use_legacy_sql=False,
    priority="BATCH",
    dag=dag,
    depends_on_past=False
)

prc_load_tb_acao_fluxo_revendedora = BigQueryExecuteQueryOperator(
    task_id='prc_load_tb_acao_fluxo_revendedora',
    sql=f"CALL `{VAR_PRJ_SENSITIVE_TRUSTED}.sp.prc_load_tb_acao_fluxo_revendedora`(NULL, NULL, '{VAR_PRJ_TRUSTED}', NULL,'{VAR_PRJ_SENSITIVE_RAW}', '{VAR_PRJ_SENSITIVE_TRUSTED}', NULL);",
    use_legacy_sql=False,
    priority="BATCH",
    dag=dag,
    depends_on_past=False
)

prc_load_tb_acao_fluxo_codigo_de_promocao = BigQueryExecuteQueryOperator(
    task_id='prc_load_tb_acao_fluxo_codigo_de_promocao',
    sql=f"CALL `{VAR_PRJ_SENSITIVE_TRUSTED}.sp.prc_load_tb_acao_fluxo_codigo_de_promocao`(NULL, NULL, '{VAR_PRJ_TRUSTED}', NULL,'{VAR_PRJ_SENSITIVE_RAW}', '{VAR_PRJ_SENSITIVE_TRUSTED}', NULL);",
    use_legacy_sql=False,
    priority="BATCH",
    dag=dag,
    depends_on_past=False
)

prc_load_tb_acao_fluxo_voucher_rfd = BigQueryExecuteQueryOperator(
    task_id='prc_load_tb_acao_fluxo_voucher_rfd',
    sql=f"CALL `{VAR_PRJ_SENSITIVE_REFINED}.sp.prc_load_tb_acao_fluxo_voucher_rfd`(NULL, NULL, '{VAR_PRJ_TRUSTED}', NULL, NULL, '{VAR_PRJ_SENSITIVE_TRUSTED}', '{VAR_PRJ_SENSITIVE_REFINED}');",
    use_legacy_sql=False,
    priority="BATCH",
    dag=dag,
    depends_on_past=False
)

prc_load_tb_acao_fluxo_receita = BigQueryExecuteQueryOperator(
    task_id='prc_load_tb_acao_fluxo_receita',
    sql=f"CALL `{VAR_PRJ_SENSITIVE_REFINED}.sp.prc_load_tb_acao_fluxo_receita`(NULL, NULL, '{VAR_PRJ_TRUSTED}', NULL, NULL, '{VAR_PRJ_SENSITIVE_TRUSTED}', '{VAR_PRJ_SENSITIVE_REFINED}');",
    use_legacy_sql=False,
    priority="BATCH",
    dag=dag,
    depends_on_past=False
)

prc_load_tb_acao_fluxo_alerta
prc_load_tb_acao_fluxo_campanha
prc_load_tb_acao_fluxo_quiz
prc_load_tb_acao_fluxo_voucher
prc_load_tb_acao_fluxo_loja
prc_load_tb_acao_fluxo_revendedora
prc_load_tb_acao_fluxo_codigo_de_promocao
[prc_load_tb_acao_fluxo_campanha, prc_load_tb_acao_fluxo_voucher] >> prc_load_tb_acao_fluxo_voucher_rfd
[prc_load_tb_acao_fluxo_campanha, prc_load_tb_acao_fluxo_voucher] >> prc_load_tb_acao_fluxo_receita

