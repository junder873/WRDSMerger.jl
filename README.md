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

## Examples

### Links between WRDS Identifiers

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

#### Type Standardization

Using these types can also help to standardize your dataset. For example, Cusips can vary by database and be 8 or 9 characters. For example, RavenPack uses 9 digit Cusips while most of CRSP uses 8 digits. There are many ways to standardize, but using the types you can run:
```julia
df1[!, :cusip] = WRDSMerger.value.(Cusip.(df1[:, :cusip]))
df2[!, :cusip] = WRDSMerger.value.(Cusip.(df2[:, :cusip]))
```

This will check that the Cusip is valid (at least according to the checksum, not that it exists in a database) and converts it to an 8 digit Cusip. If you want 9 digits, then `WRDSMerger.value` accepts an optinal length argument, so run `WRDSMerger.value.(Cusip.(df1[:, :cusip]), 9)`.

### Calculating Abnormal Returns and Other Return Statistics

Another common task is calculating abnormal returns around a firm event, such as an earnings announcement or when a firm enters the S&P 500. This package provides a variety of functions to calculate those.

#### Caching Firm and Market Data

To make the calculations as fast as possible, this package relies on cached data to quickly access the return data. This package provides functions to save the data in data types similar to BusinessDays.jl's cached data, which makes accessing a range of data incredibly quick. Using GroupDataFrame and filtering, it took 10+ minutes to run a large number (100,000) of regressions. Using this cached data, the same regressions took less than 3 seconds.

It is recommended to load market return data first so that the return dates for the firm data can be checked as valid dates. To load the market data into the cache, run:
```julia
MarketData(df_market_data)
```
Or, if you want to load the data from WRDS:
```julia
MarketData(ff_data(conn, Date(2000), today()))
```
By default, these functions add a column to the market data as the intercept column (a column of ones).

Second, load the firm data. This stores the firm data in a dictionary where the identifier (typically Permno, so an integer) is the key and the related data is stored for quick access to a range of dates. In terms of total size, I find that the dictionary is typically smaller than daily return data since the dictionary only stores the identifier once. To load the firm data, run:
```julia
FirmData(
    df_firm_data;
    valuecols="ret"
)
```
Depending on the amount of firm data, this might take some time. For example, tested on a Ryzen 3600, loading 36 million rows took about a minute. This is by far the slowest part of this, limiting the data will make the operations faster.

#### Accessing Cached Data

This package provides three functions for accessing the cached data. While these are available, most of the functions discussed later automatically use these, however, if you want to build your own functions these provide the basis for accessing the data quickly. Data for firms is stored in vectors, data for the market is stored in a matrix.

- `get_firm_data(id, date_start, date_end, col)` fetches a specific firms data (based in `id`) between two dates for one of the columns.
- `get_market_data(date_start, date_end, cols_market...)` fetches data for the market. It fetches all columns (if no values for cols_market is passed) or a selection of columns based on cols_market.
- `get_market_data(id, date_start, date_end, cols_market...)` also fetches data for the market, except will also make sure that the number of rows in the fetched matrix is the same as the dates that exist for the firm id provided. Since firms can be missing data relative to the market, this can make sure you are comparing equivalent data.
- `get_firm_market_data(id, date_start, date_end; cols_market::Union{Nothing, Vector{String}, String}=nothing, col_firm::String="ret")` is a combination of the previous functions. It returns a tuple, with the first element being a vector of the requested firm data and the second being a matrix of firm data, similar to the previous function, the number of rows in the market matrix is the same as the length of the firm vector.

#### Calculation Functions

##### Regression Estimate

It is often necessary to estimate a regression model for a specific firm based on market data. The method is similar to the `get_firm_market_data` function previously described:
```julia
cache_reg(
    id::Int,
    est_min::Date,
    est_max::Date;
    cols_market::Union{Nothing, Vector{String}}=nothing,
    col_firm::String="ret",
    minobs::Real=.8,
    calendar="USNYSE"
)
```
This fetches the data and runs the regression. If `cols_market` is `nothing`, then all columns are used. Be careful if leaving this as `nothing` and using Fama-French data since that data includes the risk free rate of return, which is often constant over short periods and would be colinear with an intercept.

If `minobs` is less than 1, then the function assumes that this is a ratio of the number of available data for the firm over the period relative to the market data required for the regression. If there is not enough data, this function returns `missing`.

This function returns a `BasicReg` model, which is a subtype of the `RegressionModel` from StatsBase.jl. Most of the later functions will work if you use a different package to calculate the regression, as long as that package provides the necessary items under the StatsBase.jl API.

The `BasicReg` model is inentionally minimalistic, making it easy to save and is useful when running a large number of regressions.

##### Variance and Standard Deviation

There are two common methods of calculating variance over a period for a firm. The first is to subtract the market return from the firm return and take the variance, given a period.
```julia
var[std](id, date_start, date_end; cols_market="vwretd", col_firm="ret")
```

The second method is to calculate the variance after estimating a regression. This uses the error in the regression, `var[std](rr)` where `rr` is a `RegressionModel`.

##### Buy and Hold Returns

Buy and hold returns are also known as geometric returns. The API for these functions is similar to `get_firm_data` and `get_market_data`:

```julia
bh_return([id], date_start, date_end, col)
```

If and `id` provided, then this is used for the market return.

##### Abnormal Returns

There are two types of common abnormal returns, buy and hold (bhar) and cumulative (car). These all work by subtracting some expected return from the firm's actual return. The expected return is either the market average or estimated based on a firm-specific regression model.

To calculate abnormal returns relative to a market index (typically `"vwretd"`, `"ewretd"`, or `"mkt"`), run:
```julia
bhar[car](id, date_start, date_end; cols_market="vwretd", col_firm="ret")
```

To calculate abnormal returns relative to a regression, run:
```julia
bhar[car](id, date_start, date_end, rr)
```

##### Example

Assume you have a DataFrame of firm-events:
```julia
df = DataFrame(
    permno = [61516, 76185, 87445, 14763, 15291, 51369, 82515],
    event_date = [Date(2020, 6, 22), Date(2020, 6, 22), Date(2020, 6, 22), Date(2020, 9, 21), Date(2020, 9, 21), Date(2020, 9, 21), Date(2020, 10, 7)]
)

#  Row │ permno  event_date 
#      │ Int64   Date       
# ─────┼────────────────────
#    1 │  61516  2020-06-22 
#    2 │  76185  2020-06-22 
#    3 │  87445  2020-06-22 
#    4 │  14763  2020-09-21 
#    5 │  15291  2020-09-21 
#    6 │  51369  2020-09-21 
#    7 │  82515  2020-10-07 

# initialize cached data, adding a column for total market return
df_market = ff_data(conn, Date(2018), today())
df_market[!, :mkt] = df_market.mktrf .+ df_market.rf
MarketData(df_market)
FirmData(crsp_data(conn, df.permno, Date(2018), today(); cols=["ret"]))

# run the Fama French 3 factor model over an estimation period
df[!, :reg] = cache_reg.(df.permno, df.event_date - Day(300), df.event_date - Day(50); cols_market=["intercept", "mktrf", "smb", "hml"])

# calculate the standard deviation during the estimation period
df[!, :std] = std.(df.reg)

# calculate the buy and hold abnormal returns over the event period
df[!, :bhar] = bhar.(df.permno, df.event_date - Day(3), df.event_date + Day(3), df.reg)

# compare bhar for Fama French vs bhar relative to the market
df[!, :bhar_market] = bhar.(df.permno, df.event_date - Day(3), df.event_date + Day(3); cols_market="mkt")

# remove reg column to clean up and make dataframe sortable later
select!(df, Not(:reg))
#  Row │ permno  event_date  std        bhar         bhar_market 
#      │ Int64   Date        Float64    Float64      Float64     
# ─────┼─────────────────────────────────────────────────────────
#    1 │  61516  2020-06-22  0.0179959  -0.0463641   -0.0287815
#    2 │  76185  2020-06-22  0.013956   -0.0252314    0.00200653
#    3 │  87445  2020-06-22  0.0205844  -0.0531      -0.0503591
#    4 │  14763  2020-09-21  0.020595   -0.00303077  -0.0091872
#    5 │  15291  2020-09-21  0.0360563   0.0321485    0.0723931
#    6 │  51369  2020-09-21  0.02035     0.0175293    0.0143943
#    7 │  82515  2020-10-07  0.020721    0.017211     0.0310354

```

## Disclaimer

This package is still early in development and many things could change. `WRDSMerger.jl` also has no association with WRDS or Wharton.
