[![Build status](https://github.com/junder873/WRDSMerger.jl/workflows/CI/badge.svg)](https://github.com/junder873/WRDSMerger.jl/actions)

# WRDSMerger.jl

This package is designed to perform common tasks using the Wharton Research Database Services (WRDS). In particular, there is a focus on data from CRSP (stock market data), Compustat (firm balance sheet and income data) and the links between these datasets. It also implements an abnormal return calculation.

## Installation

This package is registered, so in the Julia REPL:
```julia
]add WRDSMerger
```
or
```
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
WRDSMerger.default_tables.comp_funda = "compa_funda"
WRDSMerger.default_tables.comp_fundq = "compa_fundq"
...
```

## Examples

### Links between WRDS Identifiers

A common task is linking two tables in WRDS. The most common identifiers are Permno (used in CRSP datasets), Cusip (used in a variety of datasets, and its historical version, NCusip), GVKey (Compustat datasets), and IBES Tickers (used in IBES). This package provides the function `link_identifiers` to link between these different identifiers (CIK and normal Tickers as well). To use, you first need a DataFrame with the identifier and a date column, the date column is required since most links are conditional on a specific date. For example, if your IBES Ticker is "ORCL":

```julia
df = DataFrame(
    ibes_ticker=["ORCL"],
    date=[Date(2020)]
)
```

Note that `ibes_ticker` and `ticker` need to be labeled correctly at the start. While there is functionality for acceptaing different names (through the `ibes_ticker_name` and `ticker_name` keyword arguments), the functionality if finicky at best. My recommendation is to rename the column before passing it to the `link_identifiers` function.

Once you have your DataFrame with an identifier and date, then pass that to the `link_identifier` function, setting the new identifiers you want to true:

```julia
df_temp = link_identifiers(conn, df; permno=true, gvkey=true)
```
This will provide the links as of that date. It is then straightforward to merge this new `df_temp` into your existing database.

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
