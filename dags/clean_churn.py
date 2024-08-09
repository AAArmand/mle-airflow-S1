import pendulum
from airflow.decorators import dag, task

# test
@dag(
    dag_id = 'clean_churn',
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ETL"]
)
def clean_churn_dataset():
    import pandas as pd
    import numpy as np
    from airflow.providers.postgres.hooks.postgres import PostgresHook

	#  ваш код здесь #
    @task()
    def create_table():
        
        import sqlalchemy
        from sqlalchemy import inspect, MetaData, Table, Column, String, Integer, Float, DateTime, UniqueConstraint
    
	    # ваш код здесь #
        metadata = MetaData()
        hook = PostgresHook('destination_db')
        db_conn = hook.get_sqlalchemy_engine()
         
        users_table = Table(
            'clean_users_churn',
		    metadata,
		    Column('id', Integer, primary_key=True, autoincrement=True),
		    Column('customer_id', String),
		    Column('begin_date', DateTime),
            Column('end_date', DateTime),
            Column('type', String),
            Column('paperless_billing', String),
            Column('payment_method', String),
            Column('monthly_charges', Float),
            Column('total_charges', Float),
            Column('internet_service', String),
            Column('online_security', String),
            Column('online_backup', String),
            Column('device_protection', String),
            Column('tech_support', String),
            Column('streaming_tv', String),
            Column('streaming_movies', String),
            Column('gender', String),
            Column('senior_citizen', Integer),
            Column('partner', String),
            Column('dependents', String),
            Column('multiple_lines', String),
            Column('target', Integer),
		    UniqueConstraint('customer_id', name='unique_clean_employee_constraint')
        )
        if not inspect(db_conn).has_table(users_table.name):
            metadata.create_all(db_conn)
    
    @task()
    def extract(**kwargs):

        hook = PostgresHook('destination_db')
        conn = hook.get_conn()
        sql = f"""
        select
            c.customer_id, c.begin_date, c.end_date, c.type, c.paperless_billing, c.payment_method, c.monthly_charges, c.total_charges,
            c.internet_service, c.online_security, c.online_backup, c.device_protection, c.tech_support, c.streaming_tv, c.streaming_movies,
            c.gender, c.senior_citizen, c.partner, c.dependents,
            c.multiple_lines, c.target
        from users_churn as c
        """
        data = pd.read_sql(sql, conn)
        conn.close()
        return data

    @task()
    def transform(data: pd.DataFrame):
        feature_cols = data.columns.drop('customer_id').tolist()
        is_duplicated_features = data.duplicated(subset=feature_cols, keep=False)
        data = data[~is_duplicated_features].reset_index(drop=True)

        cols_with_nans = data.isnull().sum()
        cols_with_nans = cols_with_nans[cols_with_nans > 0].index.drop('end_date')
        for col in cols_with_nans:
            if data[col].dtype in [float, int]:
                fill_value = data[col].mean()
            elif data[col].dtype == 'object':
                fill_value = data[col].mode()[0]
            data[col] = data[col].fillna(fill_value)
        
        num_cols = data.select_dtypes(['float']).columns
        threshold = 1.5
        potential_outliers = pd.DataFrame()
        for col in num_cols:
            Q1 = data[col].quantile(.25)
            Q3 = data[col].quantile(.75) 
            IQR = Q3 - Q1
            margin = threshold * IQR 
            lower = Q1 - margin 
            upper = Q3 + margin 
            potential_outliers[col] = ~data[col].between(lower, upper)

        outliers = potential_outliers.any(axis=1)
        print(data[outliers])

        return data

    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook('destination_db')
        data['end_date'] = data['end_date'].astype('object').replace(np.nan, None)
        hook.insert_rows(
            table="clean_users_churn",
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['customer_id'],
            rows=data.values.tolist()
    )

    # ваш код здесь #
    create_table()
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)
    
clean_churn_dataset()