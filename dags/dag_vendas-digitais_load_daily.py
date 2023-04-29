# ===========================================================================================
# Objeto........: dag_vendas-digitais_load_daily.py
# Data Criacao..: 12/09/2022
# Projeto.......: LOJA DIGITAL
# Descricao.....: DAG de execução de carga diária de vendas digitais
# Departamento..: Engenharia de Dados e Arquitetura
# Autor.........: werikson Rodrigues
# Repositório GitHub: https://github.com/grupoboticario/data-platform-airflow
# ===========================================================================================

# ===========================================================================================
# ATUALIZAÇÕES
# Autor  : Werikson Rodrigues 
# Data   : 17/10/2022
# Detalhe: Adicionando Task para acionar função do Bot de reports Tabelau > Slack
# ===========================================================================================

# ===========================================================================================
# BIBLIOTECAS
# ===========================================================================================

import os, json
from airflow import DAG
import google.oauth2.id_token
from google.cloud import bigquery
from airflow.models import Variable
import google.auth.transport.requests
from datetime import datetime, timedelta
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator 
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

from datetime import timedelta, datetime, date
from typing import Optional, List, Dict, Union, Any, Sequence
from airflow.operators.python_operator import ShortCircuitOperator

# ===========================================================================================
# VARIÁVEIS
# ===========================================================================================

# Coleta variaveis do Airflow
env_var = Variable.get("dag_vendas-digitais_load_daily", deserialize_json=True)

# Variaveis de Projeto
SCHEDULE_INTERVAL           = env_var["schedule_interval"]
RETRIES                     = env_var["retries"]
RETRY_DELAY                 = env_var["retry_delay"]
DAG_TIMEOUT                 = env_var["dag_timeout"]

VAR_PRJ_RAW                 = env_var["var_prj_raw"]
VAR_PRJ_RAW_CUSTOM          = env_var["var_prj_raw_custom"]
VAR_PRJ_TRUSTED             = env_var["var_prj_trusted"]
VAR_PRJ_REFINED             = env_var["var_prj_refined"]
VAR_PRJ_SENSITIVE_RAW       = env_var["var_prj_sensitive_raw"]
VAR_PRJ_SENSITIVE_TRUSTED   = env_var["var_prj_sensitive_trusted"]
VAR_PRJ_SENSITIVE_REFINED   = env_var["var_prj_sensitive_refined"]

REPORT_TABLEAU_SLACK_ARGS   = env_var["report_tableau_slack_args"]
HTTP_CONN_ID_FUNCTION       = env_var["http_conn_id_function"]
ENDPOINT_FUNCTION           = env_var["endpoint_function"]
GCF_URL                     = env_var["gcf_url"]


default_args = {
    "owner": "Gerencia: BI & Analytics, Coord: Front",
    'start_date': datetime(2022, 9, 12),
    'depends_on_past': False,
    'retries': RETRIES,
    'retry_delay': timedelta(minutes=RETRY_DELAY),
    'dagrun_timeout': timedelta(minutes=DAG_TIMEOUT)
}

dag = DAG(
    'dag_vendas-digitais_load_daily',
    default_args=default_args,
    description='DAG de execução de carga diária de vendas digitais',
    catchup=False,
    schedule_interval=SCHEDULE_INTERVAL,
    tags=["Chapter/VS: Omni e Lojas", "Projeto: Loja Digital", "Refined"]
)

with dag:

    """
        Checks Data Quality
    """
    dataquality_refined_zone_tb_dash_bxpd = BigQueryExecuteQueryOperator(
        task_id="dataquality_refined_zone_tb_dash_bxpd",
        sql="""
            CALL `data-quality-gb.sp.prc_dataquality_log_carga` ('{var_prj}', '{var_dataset}', '{var_table}', '{var_tolerance_days_log_carga}', '{var_config_log_carga}', 'na');
        """.format(
            var_prj= VAR_PRJ_REFINED,     # projeto
            var_dataset= "mensuracao",              # dataset que se encontra a tabela
            var_table= "tb_dash_bxpd",             # nome da tabela
            var_tolerance_days_log_carga= "1",      # tolerância em dias para dataquality_log_carga, 0=today, 1= -1 dia. 2=-2dias...
            var_config_log_carga= "0"               # 0= padrão, consulta auxiliar.tb_log_carga, 1=verifica diretamente no status da tabela.
        ),
        use_legacy_sql=False,
        priority="BATCH",
        dag=dag,
        depends_on_past=False
    )

    dataquality_refined_zone_tb_dash_loja_digital = BigQueryExecuteQueryOperator(
        task_id="dataquality_refined_zone_tb_dash_loja_digital",
        sql="""
            CALL `data-quality-gb.sp.prc_dataquality_log_carga` ('{var_prj}', '{var_dataset}', '{var_table}', '{var_tolerance_days_log_carga}', '{var_config_log_carga}', 'na');
        """.format(
            var_prj= VAR_PRJ_REFINED,     # projeto
            var_dataset= "loja_digital",              # dataset que se encontra a tabela
            var_table= "tb_dash_loja_digital",             # nome da tabela
            var_tolerance_days_log_carga= "1",      # tolerância em dias para dataquality_log_carga, 0=today, 1= -1 dia. 2=-2dias...
            var_config_log_carga= "0"               # 0= padrão, consulta auxiliar.tb_log_carga, 1=verifica diretamente no status da tabela.
        ),
        use_legacy_sql=False,
        priority="BATCH",
        dag=dag,
        depends_on_past=False
    )

    dataquality_trusted_zone_tb_real_dia_cupom_pdv = BigQueryExecuteQueryOperator(
        task_id="dataquality_trusted_zone_tb_real_dia_cupom_pdv",
        sql="""
            CALL `data-quality-gb.sp.prc_dataquality_log_carga` ('{var_prj}', '{var_dataset}', '{var_table}', '{var_tolerance_days_log_carga}', '{var_config_log_carga}', 'na');
        """.format(
            var_prj= VAR_PRJ_SENSITIVE_TRUSTED,     # projeto
            var_dataset= "sellout",              # dataset que se encontra a tabela
            var_table= "tb_real_dia_cupom_pdv",             # nome da tabela
            var_tolerance_days_log_carga= "1",      # tolerância em dias para dataquality_log_carga, 0=today, 1= -1 dia. 2=-2dias...
            var_config_log_carga= "0"               # 0= padrão, consulta auxiliar.tb_log_carga, 1=verifica diretamente no status da tabela.
        ),
        use_legacy_sql=False,
        priority="BATCH",
        dag=dag,
        depends_on_past=False
    )

    dataquality_trusted_zone_tb_okrs_produtos_digitais = BigQueryExecuteQueryOperator(
        task_id="dataquality_trusted_zone_tb_okrs_produtos_digitais",
        sql="""
            CALL `data-quality-gb.sp.prc_dataquality_log_carga` ('{var_prj}', '{var_dataset}', '{var_table}', '{var_tolerance_days_log_carga}', '{var_config_log_carga}', 'na');
        """.format(
            var_prj= VAR_PRJ_SENSITIVE_TRUSTED,     # projeto
            var_dataset= "loja_digital",              # dataset que se encontra a tabela
            var_table= "tb_okrs_produtos_digitais",             # nome da tabela
            var_tolerance_days_log_carga= "na",      # tolerância em dias para dataquality_log_carga, 0=today, 1= -1 dia. 2=-2dias...
            var_config_log_carga= "na"               # 0= padrão, consulta auxiliar.tb_log_carga, 1=verifica diretamente no status da tabela.
        ),
        use_legacy_sql=False,
        priority="BATCH",
        dag=dag,
        depends_on_past=False
    )

    """
        CARGA CAMADA REFINED
    """
    prc_load_tb_dash_vendas_digitais_slack = BigQueryExecuteQueryOperator(
        task_id='prc_load_tb_dash_vendas_digitais_slack',
        sql=f"CALL `{VAR_PRJ_REFINED}.sp.prc_load_tb_dash_vendas_digitais_slack`(NULL, NULL, '{VAR_PRJ_TRUSTED}', '{VAR_PRJ_REFINED}', NULL, '{VAR_PRJ_SENSITIVE_TRUSTED}', NULL); ",
        use_legacy_sql=False,
        priority="BATCH",
        dag=dag,
        depends_on_past=False,
        trigger_rule="all_success"
    )


    """
        ENVIO DE REPORT PARA O SLACK
    """
    request = google.auth.transport.requests.Request()
    function_id_token = google.oauth2.id_token.fetch_id_token(request,GCF_URL)

    def valid_slack_report_bq_monitor_dash_vendas_digitais():
        bq_client = bigquery.Client()
        result_query = None

        # Captura a data atual
        date = datetime.today().strftime('%Y-%m-%d')   

        # Executa a query abaixo e retorna o resultado
        query = """
            SELECT MAX(created_at), message_id
            FROM `{}.auxiliar.monitor_logs_slack`
            WHERE report_id = '{}' AND  SUBSTRING(created_at,1,10) = '{}'
            GROUP BY message_id       
        """.format(env_var["report_tableau_slack_args"][0]["project_log_table"], REPORT_TABLEAU_SLACK_ARGS[0]["report"], date)   

        for row in bq_client.query(query):
            # Row values can be accessed by field name or index.
            result_query = '{}'.format(row[1]) 
        
        if result_query is not None:       
            return "call_report_slack_delete_monitor_dash_vendas_digitais"
        else:   
            return "call_http_tableau_send_report_slack_monitor_dash_vendas_digitais"

    # Checa se o report ja foi enviado na data corrente e dependendo do resultado chama a task: call_report_slack_delete ou call_http_tableau_send_report_slack
    check_slack_report_monitor_receita_bot_loja = BranchPythonOperator(
            task_id="check_slack_report_monitor_receita_bot_loja",
            python_callable=valid_slack_report_bq_monitor_dash_vendas_digitais,        
            dag=dag,
        )

    # Chama Cloud Function que envia o report para o canal do slack
    call_http_tableau_send_report_slack_monitor_dash_vendas_digitais = SimpleHttpOperator(
        task_id= "call_http_tableau_send_report_slack_monitor_dash_vendas_digitais",
        method='GET',
        http_conn_id=HTTP_CONN_ID_FUNCTION,
        endpoint=ENDPOINT_FUNCTION,
        extra_options={"timeout": 300},
        headers={"Authorization": "bearer " + function_id_token, "method_type":"GET"},
        data=REPORT_TABLEAU_SLACK_ARGS[0],
        do_xcom_push=False,
        dag=dag,
        trigger_rule='all_done',
        retries=0
    )

    call_report_slack_delete_monitor_dash_vendas_digitais = SimpleHttpOperator(
        task_id= "call_report_slack_delete_monitor_dash_vendas_digitais",
        method='GET',
        http_conn_id=HTTP_CONN_ID_FUNCTION,
        endpoint=ENDPOINT_FUNCTION,
        extra_options={"timeout": 300},
        headers={"Authorization": "bearer " + function_id_token, "method_type":"DELETE"},
        data=REPORT_TABLEAU_SLACK_ARGS[0],
        do_xcom_push=False,
        dag=dag,
        retries=0
    )

"""
    FUNÇÃO DE SCHEDULE
"""
# EXECUCAO TODA SEGUNDA AS 10H
def frequencia_todasSegundas(execution_date, **kwargs):
    hora_execucao = [13]
    if date.today().weekday() in (0) and datetime.now().hour in hora_execucao:
        return 1
    else :
        return 0



    [dataquality_refined_zone_tb_dash_bxpd,
    dataquality_refined_zone_tb_dash_loja_digital,
    dataquality_trusted_zone_tb_real_dia_cupom_pdv,
    dataquality_trusted_zone_tb_okrs_produtos_digitais] >> prc_load_tb_dash_vendas_digitais_slack

frequencia_todasSegundas = ShortCircuitOperator(
  task_id='frequencia_todasSegundas',
  python_callable=frequencia_todasSegundas,
  provide_context=True,
  dag=dag
)

prc_load_tb_dash_vendas_digitais_slack >> check_slack_report_monitor_receita_bot_loja >> [call_report_slack_delete_monitor_dash_vendas_digitais >> call_http_tableau_send_report_slack_monitor_dash_vendas_digitais] >> call_report_slack_delete_monitor_dash_vendas_digitais >> call_http_tableau_send_report_slack_monitor_dash_vendas_digitais
    