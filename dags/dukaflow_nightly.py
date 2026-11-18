"""Nightly batch layer (phase 4.4): lake raw -> Spark -> Snowflake PUT/COPY/MERGE -> dbt build.

TODO:
- @dag(schedule="@daily", catchup=False) with a branch that skips nights with no new raw files
- tasks: build_curated (Spark), load_and_merge (Snowflake), dbt_build
- run Airflow from its official docker-compose, mounting dags/, dbt/, spark/
"""
