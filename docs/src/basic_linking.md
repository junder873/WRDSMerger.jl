
```@setup default_behavior
using DuckDB, DBInterface, DataFrames, WRDSMerger, Dates
db = DBInterface.connect(DuckDB.DB, joinpath("..", "..", "test", "data", "test_data_final.duckdb"))
funs = [
    generate_crsp_links,
    generate_comp_crsp_links,
    generate_comp_cik_links,
    generate_ibes_links,
    generate_option_crsp_links,
    generate_ravenpack_links
]
for fun in funs
    fun(db)
end
create_all_links()
```
# Basics of Linking Identifiers

A core part of this package is to provide a simple and consistent interface for linking different identifiers in WRDS. One of the primary goals is to reduce the overhead of remembering how exactly to link one dataset to another.

## Downloading and Saving Data

To do so, first download the necessary data from WRDS. This package provides download functions to do so (see [Linking Download Functions](@ref)), which are automatically called by respective generating functions (see [Generating LinkPair Functions](@ref)). The generating functions take in a `DataFrame` (which expects certain column names) and creates the necessary functions between its identifiers. Finally, calling `create_all_links()` will create the remaining links that the tables do not provide.

To provide an example:
```julia
julia> conn = LibPQ.Connection("host=wrds-pgdata.wharton.upenn.edu port=9737 ...");
julia> generate_crsp_links(conn) # downloads the data, creates links between
# Permno <-> Permco, Permno <-> NCusip, etc.
# and returns the data that is downloaded

julia> generate_comp_crsp_links(conn) # similar to generate_crsp_links

julia> create_all_links() # defines functions between NCusip <-> GVKey,
# Ticker <-> GVKey, etc.
```

For CRSP V2 data, use `generate_crsp_links_v2` instead of `generate_crsp_links`:
```julia
julia> generate_crsp_links_v2(conn) # uses crsp.stocknames_v2 and crsp.dsf_v2
```

The generate functions return the DataFrame that is downloaded so you can save it locally (with [CSV.jl](https://github.com/JuliaData/CSV.jl), [Arrow.jl](https://github.com/apache/arrow-julia), etc.) and can use again as opposed to re-downloading the data.

This package also provides a simple function that runs all of these:
```julia
julia> download_all_links(conn)
```
Which downloads all 6 default tables and returns those 6 DataFrames. Note that if your WRDS account lacks access to one of the tables, you need to change which items are downloaded.

For example, the code I use when starting a project is:
```julia
data_dir = joinpath(path_to_saved_files)
dfs = download_all_links(conn)
files = [
    "crsp_links",
    "crsp_comp_links",
    "gvkey_cik_links",
    "ibes_links",
    "option_links",
    "ravenpack_links"
]
# I prefer Arrow.jl and feather files, replace with CSV.jl if desired
for (df, file) in zip(dfs, files)
    Arrow.write(joinpath(data_dir, file * ".feather"), df)
end
```

Then, whenever I reload the project:
```julia
funs=[
    generate_crsp_links,
    generate_comp_crsp_links,
    generate_comp_cik_links,
    generate_ibes_links,
    generate_option_crsp_links,
    generate_ravenpack_links
]
for (file, f) in zip(files, funs)
    @chain joinpath(data_dir, file * ".feather") begin
        Arrow.Table
        DataFrame
        copy
        f
    end
end
create_all_links()
```

## Using Local Files with DuckDB

An alternative to downloading data from WRDS and saving individual link DataFrames
is to use [DuckDB.jl](https://github.com/duckdb/duckdb) to read local copies of
WRDS tables directly. DuckDB can read Parquet, CSV, and many other file formats
without loading them into memory first.

Since the `generate_*` functions accept a database connection and table name
arguments, you can point them at local files instead of WRDS tables. When using
DuckDB, local file paths must be wrapped in single quotes so that DuckDB treats
them as file references in the SQL query:

```julia
using DuckDB, WRDSMerger
conn = DBInterface.connect(DuckDB.DB, ":memory:")

# For CRSP V2 with local Parquet files:
generate_crsp_links_v2(conn, "'stocknames_v2.parquet'", "'dsf_v2.parquet'")
generate_comp_crsp_links(conn, "'ccmxpf_lnkhist.parquet'")
generate_comp_cik_links(conn, "'comp_company.parquet'")
generate_ibes_links(conn, "'ibcrsphist.parquet'")
generate_option_crsp_links(conn, "'secnmd.parquet'")
# generate_ravenpack_links requires a cusip_list argument as well
create_all_links()
```

This approach avoids the save/reload cycle entirely and is especially convenient
if you maintain a personal copy of WRDS tables as Parquet files. You can also use
a persistent DuckDB database file instead of `:memory:` if you have already loaded
the data:

```julia
conn = DBInterface.connect(DuckDB.DB, "my_wrds_data.duckdb")
generate_crsp_links_v2(conn, "crsp.stocknames_v2", "crsp.dsf_v2")
# ... etc.
```

## Linking Identifiers

Once the initial data is downloaded and necessary functions are created, the package provides a consistent set of methods to convert one identifier to any other. This follows the pattern:
```
(ID You Want)((ID You Have)(value), Date for conversion)
```
For example:
```@repl default_behavior
GVKey(Permno(47896), Date(2020))
NCusip(CIK(19617), Date(2020)) # works for Int or String
CIK(Permno(47896), Date(2020))
CIK(NCusip("46625H21"), Date(2020))
```
As you can see, this includes cases where there is not a table providing a direct link (CIK <-> Permno, CIK <-> NCusip). This makes it easy to link the varied datasets in WRDS.

These functions can be easily used with broadcasting:
```@repl
GVKey.(Permno.([47896, 44206, 46703]), Date(2020))
GVKey.(Permno.([47896, 44206, 46703]), [Date(2018), Date(2019), Date(2020)])
```

Or with other packages such as [DataFramesMeta.jl](https://juliadata.github.io/DataFramesMeta.jl/stable/):
```julia
@chain df begin
    @rtransform(:gvkey = GVKey(Permno(:permno), :date))
end
```


All of the identifiers that this package provides by default are seen in [Identifier Types](@ref). This is expandable as discussed in [Adding New Identifiers](@ref).

## Generating LinkPair Functions

This section describes the default functions that exist to generate the necessary links.

```@docs
generate_crsp_links
generate_crsp_links_v2
generate_comp_crsp_links
generate_comp_cik_links
generate_ibes_links
generate_option_crsp_links
generate_ravenpack_links
```
