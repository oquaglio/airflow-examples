from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.bash import BashOperator

# from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowException
from airflow.utils.task_group import TaskGroup
from airflow.models.param import Param
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.scheduling_utils import choose_tasks_for_current_window
from utils.data_processing_utils import process_data_freshness_logs

DAG_ID = os.path.basename(__file__).replace(".py", "")

# Example list of strings
list_of_stacks = [
    "stack_raw_waio_ent_crew_rostering",
    "stack_qa_waio_all_sample_manager",
    "stack_qa_waio_ent_meds",
    "stack_qa_waio_newmaneast_minestar",
    "stack_raw_waio_ent_pi_system",
    "stack_raw_waio_ent_pi_system_v2",
    "stack_raw_waio_ent_mtm_v2",
    "stack_raw_waio_ent_rail_leader",
    "stack_raw_waio_ent_pi_rail_v2",
    "stack_qa_waio_all_scada_log",
    "stack_qa_waio_ent_sh_inflight",
    "stack_qa_waio_jimblebar_minestar",
    "stack_qa_waio_whaleback_modular",
    "stack_qa_waio_ent_rail_time_usage",
    "stack_qa_waio_southflank_modular",
    "stack_qa_waio_yandi_minestar",
    "stack_raw_waio_ent_pi_system_v3",
    "stack_qa_waio_all_magnet_inventory",
    "stack_qa_waio_ent_infoblox",
    "stack_qa_waio_mac_minestar",
    "stack_qa_waio_caterpillar_health",
    "stack_qa_waio_ent_safety_dashboard",
    "stack_qa_waio_ent_pi_phd",
    "stack_qa_waio_rail_fatigue_assessment",
    "stack_qa_waio_ent_crew_rostering",
    "stack_qa_waio_ent_oba",
    "stack_qa_waio_ent_mtm",
    "stack_qa_waio_ent_pi_rail",
    "stack_qa_waio_all_ampla",
    "stack_qa_waio_all_material_tracker",
    "stack_qa_waio_ent_railhistorian",
    "stack_qa_waio_er_surfacemanager",
    "stack_qa_waio_ent_dsa",
    "stack_qa_waio_jmb_surfacemanager",
    "stack_qa_waio_mwb_surfacemanager",
    "stack_qa_waio_yn_surfacemanager",
    "stack_qa_waio_mac_surfacemanager",
    "stack_qa_waio_sf_surfacemanager",
    "stack_qa_waio_ent_pss",
    "stack_qa_waio_ent_schdb",
    "stack_qa_waio_ent_pi_port",
    "stack_qa_waio_all_ramsystcmv",
    "stack_qa_waio_ent_pi_streaming",
    "stack_qa_waio_ent_oneplanscp",
    "stack_qa_waio_all_primavera_shutdown",
    "stack_conformed_waio_fms",
    "stack_qa_waio_ent_smartmaintenance_apps",
    "stack_qa_waio_all_blasor_planhistorian",
    "stack_qa_waio_ent_pmac",
    "stack_qa_waio_ent_smartplant",
    "stack_qa_waio_ent_land_use_system",
    "stack_qa_waio_rss_repds",
    "stack_qa_waio_ent_env_aqms",
    "stack_qa_waio_ent_p2",
    "stack_qa_waio_ent_ddms",
    "stack_qa_waio_ent_sftp_aqms",
    "stack_qa_waio_ent_iiot_aqms",
    "stack_qa_waio_ent_pi_aqms",
    "stack_qa_waio_ent_dynamox",
    "stack_qa_waio_ent_pi_rtm",
    "stack_qa_waio_mac_mhm",
    "stack_qa_waio_er_ore_tracking",
    "stack_qa_waio_epg94_ore_tracking",
    "stack_qa_waio_cpg94_ore_tracking",
    "stack_qa_waio_wb_ore_tracking",
    "stack_qa_waio_ynd_ore_tracking",
]


# Function to print the string (simulate task execution)
def first_task(task_string, **kwargs):
    import time

    print(f"Executing first task for: {task_string}")
    delay_in_secs = kwargs.get("delay_in_secs")
    time.sleep(delay_in_secs)
    print(f"Done.")


def second_task(task_string, **kwargs):
    import time

    print(f"Executing second task for: {task_string}")
    delay_in_secs = kwargs.get("delay_in_secs")
    time.sleep(delay_in_secs)
    print(f"Done.")


def delay_execution(**kwargs):
    """Custom delay function."""
    import time

    delay_in_secs = kwargs.get("delay_in_secs")

    time.sleep(delay_in_secs)  # Sleep for 60 seconds


def fail(**kwargs):
    raise AirflowException("Simulated failure occurred!")


# DAG definition
with DAG(
    DAG_ID,
    default_args={
        "owner": "otto",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    start_date=datetime(2021, 1, 1),
    schedule="*/5 * * * *",  # Every 5 minutes
    max_active_runs=3,  # It's safe to allow parallel runs since the tasks are grouped into windows
    tags=["example", "test"],
    catchup=False,
    params={
        "minutes_per_window": Param(
            5,
            type="integer",
            minimum=1,
            maximum=60,
            title="Minutes per window:",
            description="Minutes per window, e.g. 5 will split stacks into 12 groups (60/5).",
        ),
    },
    render_template_as_native_obj=True,
) as dag:

    start = EmptyOperator(task_id="start")

    # choose the tasks for the current window
    select_tasks_operator = BranchPythonOperator(
        task_id="choose_tasks_to_run",
        python_callable=choose_tasks_for_current_window,
        do_xcom_push=False,
        retries=0,
        op_kwargs={
            "task_group_id": "tg_stacks",
            "task_to_trigger_prefix": "trigger_",
            "list_of_tasks": list_of_stacks,
            "minutes_per_window": "{{ params.minutes_per_window }}",
        },
    )

    with TaskGroup("tg_stacks") as tg_stacks:

        # Create tasks for each stack
        for stack in list_of_stacks:
            with TaskGroup(f"tg_{stack.replace(' ', '_')}"):

                # Note:
                # skipped counts as done
                # upstream are direct upstream task(s) only
                # all_success is the default trigger rule
                # all_done will count skips

                trigger_task = EmptyOperator(task_id=f"trigger_{stack.replace(' ', '_')}")

                bash_script = "echo 'Hello World!'"

                task1 = BashOperator(
                    task_id=f"task_{stack.replace(' ', '_')}_first_task",
                    bash_command=bash_script,
                    do_xcom_push=False,
                    retries=2,
                    retry_delay=timedelta(seconds=10),
                    trigger_rule="none_skipped",
                )

                # task1 = PythonOperator(
                #     task_id=f"task_{stack.replace(' ', '_')}_first_task",
                #     python_callable=first_task,
                #     op_kwargs={"delay_in_secs": 0},
                #     do_xcom_push=False,
                #     op_args=[stack],
                #     retries=2,
                #     retry_delay=timedelta(seconds=10),
                # )

                # skipped of all reties on task 1 fail
                task2 = PythonOperator(
                    task_id=f"task_{stack.replace(' ', '_')}_second_task",
                    python_callable=second_task,
                    op_kwargs={"delay_in_secs": 0},
                    do_xcom_push=False,
                    op_args=[stack],
                    retries=2,
                    retry_delay=timedelta(seconds=10),
                    trigger_rule="none_skipped",
                )

                task3 = PythonOperator(
                    task_id=f"task_{stack.replace(' ', '_')}_third_task",
                    python_callable=fail,
                    op_kwargs={"delay_in_secs": 0},
                    do_xcom_push=False,
                    op_args=[stack],
                    retries=2,
                    retry_delay=timedelta(seconds=10),
                    trigger_rule="none_skipped",
                )

                task4 = PythonOperator(
                    task_id=f"task_{stack.replace(' ', '_')}_fourth_task",
                    python_callable=second_task,
                    op_kwargs={"delay_in_secs": 0},
                    do_xcom_push=False,
                    op_args=[stack],
                    retries=2,
                    retry_delay=timedelta(seconds=10),
                    trigger_rule="none_skipped",
                )

                process_data_freshness_task = PythonOperator(
                    task_id=f"process_freshness_and_alert_{stack}",
                    python_callable=process_data_freshness_logs,
                    templates_dict={
                        "stack_name": f"{stack}",
                        "task_group_name": f"{stack}_group.extracting_data_freshness_status",
                    },
                    trigger_rule="none_skipped",
                )

                (trigger_task >> task1 >> task2 >> task3 >> task4 >> process_data_freshness_task)

    # run when all tasks in task groups complete (success, failed, or skipped)
    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    start >> select_tasks_operator >> tg_stacks >> end
