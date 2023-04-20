```@setup default_behavior
data_dir = joinpath("..", "..", "test", "data")
using CSV, DataFrames, WRDSMerger, Dates
files = [
    "crsp_links",
    "crsp_comp_links",
    "gvkey_cik_links",
    "ibes_links",
    "option_links",
    "ravenpack_links"
]
funs=[
    generate_crsp_links,
    generate_comp_crsp_links,
    generate_comp_cik_links,
    generate_ibes_links,
    generate_option_crsp_links,
    generate_ravenpack_links
]
for (file, fun) in zip(files, funs)
    fun(
        DataFrame(
            CSV.File(joinpath(data_dir, file * ".csv"))
        )
    )
end
```

# Default Behavior

This package has some defaults that are important to be aware of during use.

## Different Return Types

The general design principal in Julia is that if a type is a function name, it should return that type. In this package, this is not always the case. When an [`AbstractIdentifier`](@ref) uses an external type (e.g. `Int`), it will return that `AbstractIdentifier`. However, when an `AbstractIdentifier` is used on another `AbstractIdentifier`, it will most often return the underlying value. For example:
```@repl default_behavior
Permno(47896) # returns the type Permno
Permno(Permco(20436), Date(2020)) # an Int type
```

The reason for this difference is that the `AbstractIdentifier` types are primarily meant for internal use and communicating information to the functions, but it is more often necessary to have the common Julia type for later joins. If it is needed to have the `AbstractIdentifier`, then run:
```@repl default_behavior
WRDSMerger.convert_identifier(Permno, Permco(20436), Date(2020))
```

## Default Options in Conversions

### Parent Firms

Certain [`SecurityIdentifier`](@ref)s have a direct link to a parent firm, most obviously [`Cusip`](@ref) and [`NCusip`](@ref) (with [`Cusip6`](@ref) and [`NCusip6`](@ref)). In certain situations, it can make sense to allow a match to occur through these parent firms, such as when the end goal is to match a `SecurityIdentifier` to a [`FirmIdentifier`](@ref).

For example, consider the case of `NCusip("46625H21")`, which is not in the data. Therefore, when trying to convert his to another `SecurityIdentifier`, it will return `missing` since there is not an exact match:
```@repl default_behavior
Permno(NCusip("46625H21"), Date(2020))
```
However, if trying to match this `NCusip` to a `FirmIdentifier`, then it will return a match:
```@repl default_behavior
Permco(NCusip("46625H21"), Date(2020))
```
This is because while the `NCusip` is not in the data, the `NCusip6("46625H")` is:
```@repl default_behavior
Permco(NCusip6("46625H"), Date(2020))
```
The logic here is that it should not matter if a particular security does not match to a firm if the parent firm of that security does match to a firm. This is very useful if the integrity of the `Cusip` values is in question. This behavior can be disabled or enabled by setting `allow_parent_firm`;
```@repl default_behavior
Permno(NCusip("46625H21"), Date(2020); allow_parent_firm=true)
Permco(NCusip("46625H21"), Date(2020); allow_parent_firm=false)
```

### Outside of Date Ranges and Singular Matches

Many links are supposed to be only valid for a specific date range. For example, linking `NCusip("16161A10")` to `Permno(47896)` is only valid between 1996-04-01 to 2001-01-01. However, this `NCusip` only ever links to that `Permno`, so the default behavior in this package is to provide that match:
```@repl default_behavior
Permno(NCusip("16161A10"), Date(2020)) # outside date range
```
If the link does not only provide one potential result (e.g., if that `NCusip` also could go to a different `Permno`), then this will return `missing`. The default behavior can be disabled by setting `allow_inexact_date=false`:
```@repl default_behavior
Permno(NCusip("16161A10"), Date(2020); allow_inexact_date=false) # outside date range
```

## Supremacy of Permno

In WRDS, Permnos are one of the easiest items to link. For example, there are easily accessible tables for linking GVKey <-> Permno, IbesTicker <-> Permno, and NCusip <-> Permno. This makes it very useful for most links. Therefore, when this package is determining the best path for linking two identifiers that are not directly linked (e.g., RPEntity <-> GVKey), this package will default to using Permno even if other paths exist of equal length.

For example, by default, this package links RPEntity to NCusip6. NCusip6 has direct links to both Permno and Permco, both of which directly link to GVKey. The default in this package will choose the path that goes through Permno (RPentity -> NCusip6 -> Permno -> GVKey).

!!! note
    If there is a shorter path, then it will still choose that (e.g., SecID -> NCusip -> NCusip6 -> RPEntity instead of SecID -> NCusip -> Permno -> NCusip6 -> RPEntity).