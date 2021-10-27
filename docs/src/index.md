# WRDSMerger.jl Docs

# Installation
From the Julia REPL:
```julia
julia> ]add hWRDSMerger
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
This package requires a subscription to WRDS and can only access datasets that are included in your subscription. There are several ways to connect to the database. The simplest and most reliable is to use [LibPQ.jl](https://github.com/invenia/LibPQ.jl), to initiate a connection run:

```julia
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

Note, running the above too many times may cause WRDS to temporarily block your connections for having too many. Run the connection at the start of your script and only rerun that part when necessary. I have found that LibPQ is the easiest way to connect to WRDS since there are no restrictions on length of query and the data has a consistent format.

Alternatively, you can connect to WRDS through an ODBC driver using [ODBC.jl](https://github.com/JuliaDatabases/ODBC.jl). I recommend following the setup steps listed under WRDS support for connecting with Stata (since that also uses ODBC). You can find that information [here](https://wrds-www.wharton.upenn.edu/pages/support/programming-wrds/programming-stata/stata-from-your-computer/).

The third method is if you download the data to your own database, such as a SQLite database using [SQLite.jl](https://github.com/JuliaDatabases/SQLite.jl) (This is the method this package uses for testing). SQLite requires slightly different names for tables, so you will need to change the table defaults:

```julia
conn = SQLite.DB("db.sqlite")
WRDSMerger.default_tables.comp_funda = "compa_funda"
WRDSMerger.default_tables.comp_fundq = "compa_fundq"
...
```

# Explore WRDS
```@docs
list_libraries
list_tables
describe_table
get_table
raw_sql
```


# Compustat
```@docs
comp_data
```

# CRSP
```@docs
crsp_stocknames
crsp_market
crsp_data
crsp_delist
crsp_adjust
```

# Utilities
```@docs
range_join
```
