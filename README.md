[![Stable](https://img.shields.io/badge/docs-stable-blue.svg)](https://docs.juliahub.com/WRDSMerger/)
[![Build status](https://github.com/junder873/WRDSMerger.jl/workflows/CI/badge.svg)](https://github.com/junder873/WRDSMerger.jl/actions)

# WRDSMerger.jl

This package is designed to perform common tasks using the Wharton Research Database Services (WRDS). In particular, there is a focus on data from CRSP (stock market data), Compustat (firm balance sheet and income data) and the links between these datasets. It also implements an abnormal return calculation.

## Installation

This package is registered, so in the Julia REPL:
```julia
]add WRDSMerger
```
or
```julia
julia> using Pkg; Pkg.add("WRDSMerger")
```

## General Usage

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
WRDSMerger.default_tables["comp_funda"] = "compa_funda"
WRDSMerger.default_tables["comp_fundq"] = "compa_fundq"
...
```

### ODBC vs LibPQ

The two largest packages I am aware of for connecting to a Postgres database in Lulia are [ODBC.jl](https://github.com/JuliaDatabases/ODBC.jl) and [LibPQ.jl](https://github.com/invenia/LibPQ.jl). Both of these have various advantages.

Starting with LibPQ, adding LibPQ to your project is the full installation process. To use ODBC, an extra driver, with extra setup, needs to occur before use. In addition, as far as I can tell, LibPQ does not have a limit on length of query. Some functions in this package (such as `crsp_data`) create exceptionally long queries to reduce the total amount of data downloaded, which LibPQ handles easily.

For ODBC, it is considerably faster at converting data to a DataFrame. For example, downloading the full CRSP Stockfile (`crsp.dsf`, which includes returns for every stock for each day and is about 100 million rows), takes about 4 minutes to download and make into a DataFrame with ODBC on a gigabit connection. LibPQ takes about 24 minutes. Most of this difference appears to be type instability while converting the LibPQ result to a DataFrame, since the initial LibPQ result only takes a minute and `@time` reports 80% garbage collection time. ODBC also stores your password separately (in the driver settings) making it a little easier to share a project without compromising your password.

## Links Between WRDS Identifiers

A common task is linking two tables in WRDS. The most common identifiers are Permno (used in CRSP datasets), Cusip (used in a variety of datasets, and its historical version, NCusip), GVKey (Compustat datasets), and IBES Tickers (used in IBES). This package provides the function `link_identifiers` to link between these different identifiers (CIK and normal Tickers as well). It also provides a number of types to make it clear what identifier is being used. Most of these links (the only exception I know of is GVKey and CIK) are only valid for a specific range of dates. Therefore, you would pass the function some a vector of the initial type, vector of dates, and the types that you want to link to.

For example, assume you have a DataFrame with tickers (that are based on IBES tickers), and a series of dates:

```julia
df = DataFrame(
    ticker=["ORCL", "ETN", "ETN"],
    date=[Date(2020), Date(2020), Date(2010)]
)
```
You then pass these as vectors to `link_identifiers`:

```julia
link_identifiers(conn, IbesTicker.(df.ticker), df.date, NCusip, Cusip)

# 3×4 DataFrame
#  Row │ IbesTicker  date        NCusip    Cusip    
#      │ String      Date        String    String   
# ─────┼────────────────────────────────────────────
#    1 │ ORCL        2020-01-01  68389X10  68389X10
#    2 │ ETN         2020-01-01  G2918310  G2918310
#    3 │ ETN         2010-01-01  27805810  G2918310
```

With this output, it is relatively easy to merge with your original DataFrame. For example:

```julia
leftjoin(
    df,
    link_identifiers(conn, IbesTicker.(df.ticker), df.date, NCusip, Cusip),
    on=["ticker" => "IbesTicker", "date"],
    validate=(false, true)
)
```

These types are also easily extandable. The current built-in types are:
- `Permno`
- `Cusip`
- `NCusip`
- `IbesTicker`
- `Ticker`
- `GVKey`
- `CIK`

If there is another identifier that is not provided, all that is required is to specify a new type, subtyping the abstract type `FirmIdentifier`, a `convert` function that converts the new type to an `Integer` or `String`, and a  `LinkTable` that connects the new identifier to one of the existing identifiers. With those three, the merge function should work automatically.

### Type Standardization

Using these types can also help to standardize your dataset. For example, Cusips can vary by database and be 8 or 9 characters. For example, RavenPack uses 9 digit Cusips while most of CRSP uses 8 digits. There are many ways to standardize, but using the types you can run:
```julia
df1[!, :cusip] = WRDSMerger.value.(Cusip.(df1[:, :cusip]))
df2[!, :cusip] = WRDSMerger.value.(Cusip.(df2[:, :cusip]))
```

This will check that the Cusip is valid (at least according to the checksum, not that it exists in a database) and converts it to an 8 digit Cusip. If you want 9 digits, then `WRDSMerger.value` accepts an optinal length argument, so run `WRDSMerger.value.(Cusip.(df1[:, :cusip]), 9)`.

## Calculating Abnormal Returns and Other Return Statistics

This functionality is now part of the package [AbnormalReturns.jl](https://github.com/junder873/AbnormalReturns.jl)

## Disclaimer

This package is still early in development and many things could change. `WRDSMerger.jl` also has no association with WRDS or Wharton.
