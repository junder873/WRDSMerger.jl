function ff_data(
    conn,
    date_start::Date=Date(1926, 7, 1),
    date_end::Date=today();
    cols::Array{String}=[
        "date",
        "mktrf",
        "smb",
        "hml",
        "rf",
        "umd"
    ]
)
    col_str = join(cols, ", ")
    query = """
        SELECT $col_str FROM $(default_tables["ff_factors"])
        WHERE date BETWEEN '$date_start' AND '$date_end'
    """
    return run_sql_query(conn, query)
end