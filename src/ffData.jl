"""
    ff_data(conn, date_start=Date(1926,7,1), date_end=today(); cols=["date","mktrf","smb","hml","rf","umd"])

Download Fama-French factor data from the `ff.factors_daily` table (configurable
via `default_tables["ff_factors"]`). Returns a DataFrame with the requested columns
over the given date range.

Available columns include `mktrf`, `smb`, `hml`, `rf`, and `umd` (momentum).
"""
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