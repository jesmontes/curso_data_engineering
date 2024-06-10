{{
    codegen.generate_source(
        schema_name = 'sql_server_dbo',
        database_name = 'ALUMNO27_DEV_SILVER_DB',
        table_names = ['orders','order_items','users','events','addresses','promos','products'],
        generate_columns = True,
        include_descriptions=True,
        include_data_types=True,
        name='desarrollo',
        include_database=True,
        include_schema=True
        )
}}