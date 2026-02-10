
"""
    download_crsp_links(conn, main_table="crsp.stocknames", stockfile="crsp.dsf")

Runs the following SQL code (tables are changeable by setting
the `main_table` and `stockfile` keyword arguments):
```sql
select a.*, b.mkt_cap from crsp.stocknames a
        left join (
            select s.permno, s.namedt, s.nameenddt, avg(d.mkt_cap) as mkt_cap from crsp.stocknames s
                inner join (select permno, date, abs(prc) * shrout as mkt_cap from crsp.dsf) as d
                on s.permno = d.permno and s.namedt <= d.date and s.nameenddt >= d.date
            group by s.permno, s.namedt, s.nameenddt
            ) b
            on a.permno = b.permno and a.namedt = b.namedt and a.nameenddt = b.nameenddt
```
and returns a DataFrame.
"""
function download_crsp_links(conn, main_table="crsp.stocknames", stockfile="crsp.dsf")
    q = """
        select a.*, b.mkt_cap from $main_table a
        left join (
            select s.permno, s.namedt, s.nameenddt, avg(d.mkt_cap) as mkt_cap from $main_table s
                inner join (select permno, date, abs(prc) * shrout as mkt_cap from $stockfile) as d
                on s.permno = d.permno and s.namedt <= d.date and s.nameenddt >= d.date
            group by s.permno, s.namedt, s.nameenddt
            ) b
            on a.permno = b.permno and a.namedt = b.namedt and a.nameenddt = b.nameenddt
    """
    df = raw_sql(conn, q)
    df[!, :mkt_cap] = coalesce.(df[:, :mkt_cap], 0.0)
    df
end

"""
    download_crsp_links_v2(conn, main_table="crsp.stocknames_v2", stockfile="crsp.dsf_v2")

CRSP V2 equivalent of [`download_crsp_links`](@ref). Uses `dlycap` (daily market
cap) instead of calculating `abs(prc) * shrout`, and `dlycaldt` instead of `date`.

Runs the following SQL code (tables are changeable by setting
the `main_table` and `stockfile` arguments):
```sql
select a.*, b.mkt_cap from crsp.stocknames_v2 a
        left join (
            select s.permno, s.namedt, s.nameenddt, avg(d.dlycap) as mkt_cap from crsp.stocknames_v2 s
                inner join (select permno, dlycaldt, dlycap from crsp.dsf_v2) as d
                on s.permno = d.permno and s.namedt <= d.dlycaldt and s.nameenddt >= d.dlycaldt
            group by s.permno, s.namedt, s.nameenddt
            ) b
            on a.permno = b.permno and a.namedt = b.namedt and a.nameenddt = b.nameenddt
```
and returns a DataFrame.
"""
function download_crsp_links_v2(conn, main_table="crsp.stocknames_v2", stockfile="crsp.dsf_v2")

    q = """
        select a.*, b.mkt_cap from $main_table a
        left join (
            select s.permno, s.namedt, s.nameenddt, avg(d.dlycap) as mkt_cap from $main_table s
                inner join (select permno, dlycaldt, dlycap from $stockfile) as d
                on s.permno = d.permno and s.namedt <= d.dlycaldt and s.nameenddt >= d.dlycaldt
            group by s.permno, s.namedt, s.nameenddt
            ) b
            on a.permno = b.permno and a.namedt = b.namedt and a.nameenddt = b.nameenddt
    """
    df = raw_sql(conn, q)
    df[!, :mkt_cap] = coalesce.(df[:, :mkt_cap], 0.0)
    df
end

"""
    download_comp_crsp_links(conn, main_table="crsp_a_ccm.ccmxpf_linkhist")

Runs the following SQL code (table is changeable by setting the `main_table` keyword argument):
```sql
SELECT * FROM crsp_a_ccm.ccmxpf_linkhist
```
and returns the resulting DataFrame
"""
function download_comp_crsp_links(conn, main_table="crsp_a_ccm.ccmxpf_linkhist")
    q = "SELECT * FROM $main_table"
    raw_sql(conn, q)
end

"""
    download_comp_cik_links(conn, main_table="comp.company")

Runs the following SQL code (table is changeable by setting the `main_table` keyword argument):
```sql
SELECT * FROM comp.company
```
and returns the resulting DataFrame
"""
function download_comp_cik_links(conn, main_table="comp.company")
    q = "SELECT * FROM $main_table"
    raw_sql(conn, q)
end

"""
    download_ibes_links(conn, main_table="wrdsapps.ibcrsphist")

Runs the following SQL code (table is changeable by setting the `main_table` keyword argument):
```sql
SELECT * FROM wrdsapps.ibcrsphist
```
and returns the resulting DataFrame
"""
function download_ibes_links(conn, main_table="wrdsapps.ibcrsphist")
    q = "SELECT * FROM $main_table"
    raw_sql(conn, q)
end

"""
    download_option_crsp_links(conn, main_table="optionm_all.secnmd")

Runs the following SQL code (table is changeable by setting the `main_table` keyword argument):
```sql
SELECT * FROM optionm_all.secnmd
```
and returns the resulting DataFrame
"""
function download_option_crsp_links(conn, main_table="optionm_all.secnmd")
    q = "SELECT * FROM $main_table"
    raw_sql(conn, q)
end

"""
    download_ravenpack_links(conn, main_table="ravenpack.rp_entity_mapping", cusip_list="crsp.stocknames")

Runs the following SQL code (tables are changeable by setting
the `main_table` and `cusip_list` keyword arguments):
```sql
SELECT rp_entity_id, data_value as ncusip, range_start, range_end FROM ravenpack.rp_entity_mapping as a
            inner join (select distinct ncusip from crsp.stocknames) as b
            on left(a.data_value, 8) = b.ncusip
```
and returns a DataFrame.
"""
function download_ravenpack_links(conn, main_table="ravenpack_common.rp_entity_mapping", cusip_list="crsp.stocknames")
    q = """
        SELECT rp_entity_id, data_value as ncusip, range_start, range_end FROM $main_table as a
            inner join (select distinct ncusip from $cusip_list) as b
            on left(a.data_value, 8) = b.ncusip
    """
    raw_sql(conn, q)
end

"""
    download_all_links(conn, funs=[...], save_dfs=true)

Convenience function that calls each function in `funs` (passing `conn`) and then
calls [`create_all_links`](@ref). If `save_dfs=true` (default), returns a vector
of the DataFrames produced by each function. The default `funs` are:
`generate_crsp_links`, `generate_comp_crsp_links`, `generate_comp_cik_links`,
`generate_ibes_links`, `generate_option_crsp_links`, `generate_ravenpack_links`.
"""
function download_all_links(
    conn,
    funs=[
        generate_crsp_links,
        generate_comp_crsp_links,
        generate_comp_cik_links,
        generate_ibes_links,
        generate_option_crsp_links,
        generate_ravenpack_links
    ],
    save_dfs=true
)
    out_dfs = DataFrame[]
    for f in funs
        df = f(conn)
        if save_dfs
            push!(out_dfs, df)
        end
    end
    create_all_links()
    if save_dfs
        out_dfs
    else
        println("Downloaded $(length(funs)) files and created relevant methods")
    end
end

