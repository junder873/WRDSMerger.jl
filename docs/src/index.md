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
This package requires a subscription to WRDS and can only access datasets that are included in your subscription. There are several ways to connect to the database. The simplest and most reliable is to use [LibPQ.jl](https://github.com/invenia/LibPQ.jl) (requires at least v1.16 of LibPQ.jl), to initiate a connection run:

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
WRDSMerger.default_tables["comp_funda"] = "compa_funda"
WRDSMerger.default_tables["comp_fundq"] = "compa_fundq"
...
```

## ODBC vs LibPQ

The two largest packages I am aware of for connecting to a Postgres database in Julia are [ODBC.jl](https://github.com/JuliaDatabases/ODBC.jl) and [LibPQ.jl](https://github.com/invenia/LibPQ.jl). Both of these have various advantages.

Starting with LibPQ, adding LibPQ to your project is the full installation process. To use ODBC, an extra driver, with extra setup, needs to occur before use. In addition, as far as I can tell, LibPQ does not have a limit on length of query. Some functions in this package (such as `crsp_data`) create exceptionally long queries to reduce the total amount of data downloaded, which LibPQ handles easily.

For ODBC, it is considerably faster at converting data to a DataFrame. For example, downloading the full CRSP Stockfile (`crsp.dsf`, which includes returns for every stock for each day and is about 100 million rows), takes about 4 minutes to download and make into a DataFrame with ODBC on a gigabit connection. LibPQ takes about 24 minutes. Most of this difference appears to be type instability while converting the LibPQ result to a DataFrame, since the initial LibPQ result only takes a minute and `@time` reports 80% garbage collection time. ODBC also stores your password separately (in the driver settings) making it a little easier to share a project without compromising your password.

