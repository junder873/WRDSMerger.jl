
"""
    download_crsp_links(db; main_table="crsp.stocknames", stockfile="crsp.dsf")

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
function download_crsp_links(db; main_table="crsp.stocknames", stockfile="crsp.dsf")
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
    df = raw_sql(db, q)
    df[!, :mkt_cap] = coalesce.(df[:, :mkt_cap], 0.0)
    df
end

"""
    download_comp_crsp_links(db; main_table="crsp_a_ccm.ccmxpf_linkhist")

Runs the following SQL code (table is changeable by setting the `main_table` keyword argument):
```sql
SELECT * FROM crsp_a_ccm.ccmxpf_linkhist
```
and returns the resulting DataFrame
"""
function download_comp_crsp_links(db; main_table="crsp_a_ccm.ccmxpf_linkhist")
    q = "SELECT * FROM $main_table"
    raw_sql(db, q)
end

"""
    download_comp_cik_links(db; main_table="comp.company")

Runs the following SQL code (table is changeable by setting the `main_table` keyword argument):
```sql
SELECT * FROM comp.company
```
and returns the resulting DataFrame
"""
function download_comp_cik_links(db; main_table="comp.company")
    q = "SELECT * FROM $main_table"
    raw_sql(db, q)
end

"""
    download_ibes_links(db; main_table="wrdsapps.ibcrsphist")

Runs the following SQL code (table is changeable by setting the `main_table` keyword argument):
```sql
SELECT * FROM wrdsapps.ibcrsphist
```
and returns the resulting DataFrame
"""
function download_ibes_links(db; main_table="wrdsapps.ibcrsphist")
    q = "SELECT * FROM $main_table"
    raw_sql(db, q)
end

"""
    download_option_crsp_links(db; main_table="optionm_all.secnmd")

Runs the following SQL code (table is changeable by setting the `main_table` keyword argument):
```sql
SELECT * FROM optionm_all.secnmd
```
and returns the resulting DataFrame
"""
function download_option_crsp_links(db; main_table="optionm_all.secnmd")
    q = "SELECT * FROM $main_table"
    raw_sql(db, q)
end

"""
    download_ravenpack_links(db; main_table="ravenpack.rp_entity_mapping", cusip_list="crsp.stocknames")

Runs the following SQL code (tables are changeable by setting
the `main_table` and `cusip_list` keyword arguments):
```sql
SELECT rp_entity_id, data_value as ncusip, range_start, range_end FROM ravenpack.rp_entity_mapping as a
            inner join (select distinct ncusip from crsp.stocknames) as b
            on left(a.data_value, 8) = b.ncusip
```
and returns a DataFrame.
"""
function download_ravenpack_links(db; main_table="ravenpack.rp_entity_mapping", cusip_list="crsp.stocknames")
    q = """
        SELECT rp_entity_id, data_value as ncusip, range_start, range_end FROM $main_table as a
            inner join (select distinct ncusip from $cusip_list) as b
            on left(a.data_value, 8) = b.ncusip
    """
    raw_sql(db, q)
end

function download_all_links(
    db;
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
        df = f(db)
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

