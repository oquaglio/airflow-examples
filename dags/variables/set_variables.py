from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.python import PythonOperator
import json
import os

DAG_ID = os.path.basename(__file__).replace(".py", "")
STACK_NAME = "stack_common_niw_snowflake"
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
DBT_PROFILES_PATH = f"{AIRFLOW_HOME}/dags/{STACK_NAME}/dbt/profiles.yml"
DBT_PROFILES_DIR = f"{AIRFLOW_HOME}/dags/{STACK_NAME}/dbt"
ENVIRONMENT_NAME = "DEV"
SNOWFLAKE_DB_NAME = "NIW_DEV"
STACKS_DATA_MONITORING_CONFIG = '["stack_raw_waio_ent_crew_rostering","stack_qa_waio_all_sample_manager","stack_qa_waio_ent_meds", "stack_qa_waio_newmaneast_minestar","stack_raw_waio_ent_pi_system", "stack_raw_waio_ent_pi_system_v2", "stack_raw_waio_ent_mtm_v2","stack_raw_waio_ent_rail_leader", "stack_raw_waio_ent_pi_rail_v2",  "stack_qa_waio_all_scada_log", "stack_qa_waio_ent_sh_inflight", "stack_qa_waio_jimblebar_minestar", "stack_qa_waio_whaleback_modular","stack_qa_waio_ent_rail_time_usage","stack_qa_waio_southflank_modular","stack_qa_waio_yandi_minestar","stack_raw_waio_ent_pi_system_v3","stack_qa_waio_all_magnet_inventory","stack_qa_waio_ent_infoblox","stack_qa_waio_mac_minestar", "stack_qa_waio_caterpillar_health","stack_qa_waio_ent_safety_dashboard","stack_qa_waio_ent_pi_phd","stack_qa_waio_rail_fatigue_assessment","stack_qa_waio_ent_crew_rostering", "stack_qa_waio_ent_oba", "stack_qa_waio_ent_mtm", "stack_qa_waio_ent_pi_rail", "stack_qa_waio_all_ampla", "stack_qa_waio_all_material_tracker", "stack_qa_waio_ent_railhistorian", "stack_qa_waio_er_surfacemanager", "stack_qa_waio_ent_dsa", "stack_qa_waio_jmb_surfacemanager", "stack_qa_waio_mwb_surfacemanager", "stack_qa_waio_yn_surfacemanager", "stack_qa_waio_mac_surfacemanager", "stack_qa_waio_sf_surfacemanager", "stack_qa_waio_ent_pss", "stack_qa_waio_ent_schdb", "stack_qa_waio_ent_pi_port","stack_qa_waio_all_ramsystcmv","stack_qa_waio_ent_pi_streaming","stack_qa_waio_ent_oneplanscp","stack_qa_waio_all_primavera_shutdown","stack_conformed_waio_fms","stack_qa_waio_ent_smartmaintenance_apps","stack_qa_waio_all_blasor_planhistorian","stack_qa_waio_ent_pmac","stack_qa_waio_ent_smartplant","stack_qa_waio_ent_land_use_system","stack_qa_waio_rss_repds","stack_qa_waio_ent_env_aqms","stack_qa_waio_ent_p2","stack_qa_waio_ent_ddms","stack_qa_waio_ent_sftp_aqms","stack_qa_waio_ent_iiot_aqms","stack_qa_waio_ent_pi_aqms","stack_qa_waio_ent_dynamox"]'

default_args = {
    "owner": "niwdu",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "trigger_rule": "all_done",
}


def fetch_and_set_variables():
    from airflow.models import Variable

    Variable.set("ENVIRONMENT_NAME", ENVIRONMENT_NAME)
    Variable.set("SNOWFLAKE_DB_NAME", SNOWFLAKE_DB_NAME)
    Variable.set("DBT_PROFILES_PATH", DBT_PROFILES_PATH)
    Variable.set("DBT_PROFILES_DIR", DBT_PROFILES_DIR)
    Variable.set("STACKS_DATA_MONITORING_CONFIG", STACKS_DATA_MONITORING_CONFIG)


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Set the Airflow Variables.",
    schedule="@once",
    start_date=days_ago(0),
    tags=["admin", "variables"],
) as dag:

    dag.doc_md = f"""
    ### {DAG_ID} DAG Documentation
    This DAG runs on demand to copy Snowflake configuration from AWS Secrets Manager into Airflow variables for use in DAGs
    """

    set_variables = PythonOperator(task_id="set_variables", python_callable=fetch_and_set_variables)
