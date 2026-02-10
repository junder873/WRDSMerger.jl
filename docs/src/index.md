# WRDSMerger.jl Docs


# Installation
From the Julia REPL:
```julia
julia> ]add WRDSMerger
```

```julia
julia> using Pkg; Pkg.add(WRDSMerger)
```

From source:
```julia
julia> ]add https://github.com/junder873/WRDSMerger.jl
```

```julia
julia> using Pkg; Pkg.add(url="https://github.com/junder873/WRDSMerger.jl")
```

# Establish DB Connection
This package requires a subscription to WRDS and can only access datasets that are included in your subscription. Any database connection that supports `DBInterface.execute` will work. There are several ways to connect:

### LibPQ

[LibPQ.jl](https://github.com/invenia/LibPQ.jl) connects directly to the WRDS Postgres server. It has no query length limit, which is important for functions like `crsp_data` that generate very long queries:

```julia
using LibPQ
conn = LibPQ.Connection(
    """
        host = wrds-pgdata.wharton.upenn.edu
        port = 9737
        user='username'
        password='password'
        sslmode = 'require' dbname = wrds
    """
)
```

Note, running the above too many times may cause WRDS to temporarily block your connections for having too many. Run the connection at the start of your script and only rerun that part when necessary.

### ODBC

Alternatively, you can connect to WRDS through an ODBC driver using [ODBC.jl](https://github.com/JuliaDatabases/ODBC.jl). ODBC is considerably faster at converting large result sets to DataFrames but requires additional driver setup. I recommend following the setup steps listed under WRDS support for connecting with Stata (since that also uses ODBC). You can find that information [here](https://wrds-www.wharton.upenn.edu/pages/support/programming-wrds/programming-stata/stata-from-your-computer/).

The third method is if you have the data locally, such as in a [DuckDB](https://github.com/duckdb/duckdb) database or as Parquet/CSV files. DuckDB is the recommended approach for local data (and is what this package uses for testing). DuckDB can read Parquet, CSV, and other file formats directly:

```julia
using DuckDB
conn = DBInterface.connect(DuckDB.DB, "my_wrds_data.duckdb")
```

If your DuckDB database uses different schema/table names than the WRDS defaults, update the table mappings:

```julia
WRDSMerger.default_tables["comp_funda"] = "comp.funda"
WRDSMerger.default_tables["crsp_stocknames"] = "crsp.stocknames"
# ... etc.
```

See [Using Local Files with DuckDB](@ref) for more details on working with local files.

## Connection Method Comparison

| Method | Setup | Speed | Query Length | Best For |
|--------|-------|-------|--------------|----------|
| **LibPQ** | `Pkg.add("LibPQ")` only | Slower for large results | No limit | General WRDS access |
| **ODBC** | Requires driver installation | Fast for large DataFrames | May have limits | Bulk data downloads |
| **DuckDB** | `Pkg.add("DuckDB")` only | Very fast (local I/O) | No limit | Local data / testing |

LibPQ requires no setup beyond installation. ODBC is considerably faster at converting large result sets to DataFrames (e.g., downloading the full CRSP daily stockfile takes ~4 minutes with ODBC vs ~24 minutes with LibPQ on a gigabit connection), but requires an ODBC driver to be installed separately. ODBC also stores your password in the driver settings, making it easier to share a project without exposing credentials. DuckDB is only for local data (Parquet, CSV, or DuckDB database files) and cannot connect to the WRDS Postgres server.

