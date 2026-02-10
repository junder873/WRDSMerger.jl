
# Downloading WRDS Data

This page covers functions for exploring the WRDS database and downloading
data from Compustat, CRSP, and Fama-French. All functions take a database
connection as the first argument (see [Establish DB Connection](@ref)).

These functions query WRDS tables whose names are stored in
`WRDSMerger.default_tables`. If your database uses different table names,
update the dictionary before calling these functions.

## Explore WRDS

These functions help you discover what data is available in WRDS.

```@docs
list_libraries
list_tables
describe_table
get_table
raw_sql
```


## Compustat

Download fundamental financial data from Compustat. By default downloads
annual data; set `annual=false` for quarterly. Note that column names
differ between annual and quarterly datasets (e.g., `sale` vs `saleq`).

```@docs
comp_data
```

## CRSP

Functions for downloading CRSP stock data, market indices, delisting
returns, and making common adjustments (split-adjusting prices, incorporating
delisting returns, etc.).

To use monthly data instead of daily, update the default tables:
```julia
WRDSMerger.default_tables["crsp_stock_data"] = "crsp.msf"
WRDSMerger.default_tables["crsp_index"] = "crsp.msi"
WRDSMerger.default_tables["crsp_delist"] = "crsp.msedelist"
```

```@docs
crsp_stocknames
crsp_market
crsp_data
crsp_delist
crsp_adjust
```

## Fama-French Factors

Download daily Fama-French factor data, including the market, size, value,
and momentum factors.

```@docs
ff_data
```
