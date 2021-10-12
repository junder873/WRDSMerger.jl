[![Build status](https://github.com/junder873/WRDSMerger.jl/workflows/CI/badge.svg)](https://github.com/junder873/WRDSMerger.jl/actions)

# WRDSMerger.jl

This package is designed to perform common tasks using the Wharton Research Database Services (WRDS). In particular, there is a focus on data from CRSP (stock market data), Compustat (firm balance sheet and income data) and the links between these datasets. It also implements an abnormal return calculation.

## General Usage

This package requires a subscription to WRDS and can only access datasets that are included in your subscription. To initiate a connection, this package relies on LibPQ.jl, where you can start a connection by running:

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

Note, running the above too many times may cause WRDS to temporarily block your connections for having too many. Run the connection at the start of your script and only rerun that part when necessary.

## Disclaimer

This package is still early in development and many things could change. `WRDSMerger.jl` also has no association with WRDS or Wharton.
