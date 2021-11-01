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

### Calculating Abnormal Returns

Another common task is calculating abnormal returns around a firm event, such as an earnings announcement or when a firm enters the S&P 500. This package provides a variety of functions to calculate those, ranging from simple returns relative to the market to Fama-French 4 factor models. First, you need a DataFrame with identifiers (should be Permno) and the event dates:
```julia
df = DataFrame(
    permno=[10104, 71563, 79637, 89002, 90993],
    date=[Date(2020, 12, 1), Date(2020, 12, 20), Date(2020, 7, 3), Date(2020, 9, 30), Date(2020, 10, 15)]
)
```

#### Simple CAR

For simple abnormal returns (just the difference between the firm's return and the market return):
```julia
df_car = calculate_car(
    conn,
    df,
    EventWindow(BDay(-3, :USNYSE), BDay(3, :USNYSE))
)
```
This function will retrieve the necessary data from the database and calculate the abnormal returns 3 business days before and after (7 days total) the event day for each firm. `EventWindow` accepts any `DatePeriod` type, so you can also use `Day`, `Month`, or `Year` and mix and match as necessary. `BDay` relies on [BusinessDays.jl](https://github.com/JuliaFinance/BusinessDays.jl), which only goes back to 1970, so older events might have innacurate results.

#### Fama French CAR

To calculate Fama French abnormal returns, the first steps are similar, but you need an `FFEstMethod` type instead of an `EventWindow`. This package provides a function with some reasonable defaults:
```julia
function FFEstMethod(
    ;
    estimation_window::Union{Missing, EventWindow}=EventWindow(BDay(-150, :USNYSE), Day(0)),
    gap_to_event::Union{String, DatePeriod}=BDay(-15, :USNYSE),
    min_est::Int=120,
    ff_sym::Vector{Symbol}=[:mktrf, :smb, :hml],
    event_window::Union{Missing, EventWindow}=missing
)
    FFEstMethod(
        estimation_window,
        gap_to_event,
        min_est,
        ff_sym,
        event_window
    )
end
```
The `estimation_window` is the period over which a regression model is estimated, which is then used to predict what would happen in the `event_window`. The `gap_to_event` specifies the day relative to the start of the `event_window` that the `estimation_window` ends. This function provides a lot of flexibility:
- If `estimation_window` is `missing`, then the CAR calculation assumes you specified an `estimation_window_start` and `estimation_window_end` column in the DataFrame, this allows for estimation lengths that vary by firm (for example, between two firm events).
- If `gap_to_event` is a `String` instead of a `DatePeriod`, then the function expects that `String` to be a column name in the DataFrame that contains Dates. The `estimation_window` will then be relative to that Date. This allows for Gaps that vary by firm-event. For example, if you want the estimation window to be over a firm's fiscal year, then set `gap_to_event="fye_date"` and `estimation_window=EventWindow(Year(-1), Day(0))` which will make the estimation over the firm's entire fiscal year.
- `ff_sym` is the set of estimation parameters, a 1 factor model (CAPM model) uses just `:mktrf`, the 3-factor model is the default, and to use the 4-factor model set `ff_sym=[:mktrf, :smb, :hml, :umd]`.
- `event_window` defaults to `missing` which allows for custom windows (as before, to allow for abnormal returns between two firm events). Like `estimation_window`, if `event_window` is `missing` then the function expects  the DataFrame to include `event_window_start` and `event_window_end`. Instead, you can include a `date` column in the DataFrame and set `event_window=EventWindow(BDay(-10, :USNYSE), BDay(10, :USNYSE))` and the function will create the event start and end dates for you.

Since the function contains some reasonable defaults, to estimate the Fama French 3-factor model, simply run:
```julia
ff = FFEstMethod(event_window=EventWindow(BDay(-10, :USNYSE), BDay(10, :USNYSE)))
df_out = calculate_car(conn, df, ff)
```

## Disclaimer

This package is still early in development and many things could change. `WRDSMerger.jl` also has no association with WRDS or Wharton.
