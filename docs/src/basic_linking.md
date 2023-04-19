
```@setup default_behavior
data_dir = joinpath("..", "..", "test", "data")
using CSV, DataFrames, WRDSMerger, Dates
files = [
    "crsp_links",
    "crsp_comp_links",
    "gvkey_cik_links",
]
funs=[
    generate_crsp_links,
    generate_comp_crsp_links,
    generate_comp_cik_links,
]
for (file, fun) in zip(files, funs)
    fun(
        DataFrame(
            CSV.File(joinpath(data_dir, file * ".csv"))
        )
    )
end
create_all_links()
```
# Basics of Linking Identifiers

A core part of this package is to provide a simple and consistent interface for linking different identifiers in WRDS. One of the primary goals is to reduce the overhead of remembering how exactly to link one dataset to another.

## Downloading and Saving Data

To do so, first download the necessary data from WRDS. This package provides download functions to do so (see [Linking Download Functions](@ref)), which are automatically called by respective generating functions (see [Generating LinkPair Functions](@ref)). The generating functions take in a `DataFrame` (which expects certain column names) and creates the necessary functions between its identifiers. Finally, calling `create_all_links()` will create the remaining links that the tables do not provide.

To provide an example:
```julia
julia> db = ODBC.Connection("wrds-pgdata-64");
julia> generate_crsp_links(db) # downloads the data, creates links between 
# Permno <-> Permco, Permno <-> NCusip, etc.
# and returns the data that is downloaded

julia> generate_comp_crsp_links(db) # similar to generate_crsp_links

julia> create_all_links() # defines functions between NCusip <-> GVKey, 
# Ticker <-> GVKey, etc.
```

The generate functions return the DataFrame that is downloaded so you can save it locally (with CSV.jl, Arrow.jl, etc.) and can use again as opposed to re-downloading the data.

This package also provides a simple function that runs all of these:
```julia
julia> download_all_links(db)
```
Which downloads all 6 default tables and returns those 6 DataFrames. Note that if your WRDS account lacks access to one of the tables, you need to change which items are downloaded.

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

All of the identifiers that this package provides by default are seen in [Identifier Types](@ref). This is expandable as discussed in [Adding New Identifiers](@ref).

## Generating LinkPair Functions

This section describes the default functions that exist to generate the necessary links.

```@docs
generate_crsp_links
generate_comp_crsp_links
generate_comp_cik_links
generate_ibes_links
generate_option_crsp_links
generate_ravenpack_links
```