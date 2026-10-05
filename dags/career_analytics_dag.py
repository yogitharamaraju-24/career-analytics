from datetime import datetime, timedelta
 
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
 
default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}
 
@dag(
    dag_id="career_analytics",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,  # worker only has 1 vCPU
    default_args=default_args,
)
def career_analytics():
 
    @task
    def extract(data_interval_start=None, data_interval_end=None):
        # only professionals updated in this run's window
        # read in batches and write to a file, worker has 2GB so can't hold everything
        raw_path = f"/data/raw/professionals_{data_interval_start:%Y%m%d}.json"
        return raw_path
 
    @task
    def transform(raw_path):
        # flatten and fix types
        staged_path = raw_path.replace("/raw/", "/staged/")
        return staged_path
 
    # no retries for bad data
    @task(retries=0)
    def quality_checks(staged_path):
        failed_share = 0.0  #share of rows failing the checks
        if failed_share > 0.05:
            raise AirflowFailException("too many rows failed quality checks")
        return staged_path
 
    @task
    def load(staged_path):
        # delete and insert per professional so a rerun gives the same result
        pass
 
    @task
    def refresh_metrics():
        # rebuild metric tables
        pass
 
    raw = extract()
    staged = transform(raw)
    checked = quality_checks(staged)
    load(checked) >> refresh_metrics()
 
 
career_analytics()