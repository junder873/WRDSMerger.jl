
# Linking Internals

## Underlying Methodology of `convert_identifier`

```@docs
WRDSMerger.convert_identifier
```

## LinkPairs

This packages primary storage method for links is an `AbstractLinkPair`, which is typically a `LinkPair`:
```@docs
LinkPair
```

Conceptually, a `LinkPair` provides a one direction link between T1 -> T2. These are typically stored in a dictionary for quick lookup:
```julia
Dict{T1, Vector{LinkPair{T1, T2}}}()
```
and this package adds a function to `Dict` to convert an abstract vector of `LinkPair`s to such a dictionary:
```@docs
Dict
```

### Defining New `AbstractLinkPair`s

While this package currently makes use of `LinkPair`, it might be easier for other identifiers to define a more complex `AbstractLinkPair`. A key component of an `AbstractLinkPair` is being able to compare when one link should be used as opposed to another, which this package refers to as priority. In some cases, there might be multiple values that determine a priority. For example, the link between [`GVKey`](@ref) <-> [`Permno`](@ref) has two columns, depending on the direction (e.g., going from `Permno` -> `GVKey`, "LC" > "LU" > "LS"...). This package converts these into a single number with decimals ("LC" = 8, "LU" = 7... and the other column, "P" = 0.3, "C" = 0.2 ..., added together). This is switched when defining the link between `GVKey` -> `Permno` ("LC" = 0.8, "P" = 3). An alternative way to define this would be to create a separate `AbstractLinkPair` type that would avoid adding and, perhaps, be clearer on methodology. For example, something like:
```julia
struct CrspCompLinkPair{T1<:AbstractIdentifier, T2<:AbstractIdentifier} <: AbstractLinkPair{T1, T2}
    parent::T1
    child::T2
    dt1::Date# first date valid
    dt2::Date# last date valid
    comp_crsp_priority::Int
    crsp_comp_priority::Int
    function CrspCompLinkPair(
        t1::T1,
        t2::T2,
        dt1::Date,
        dt2::Date,
        linktype::AbstractString,
        linkprim::AbstractString
    ) where {T1, T2}
        comp_crsp_priority = if linkprim == "P"
            3
        elseif linkprim == "C"
            2
        elseif linkprim == "J"
            1
        end
        crsp_comp_priority = if linktype == "LC"
            8
        elseif linktype == "LU"
            7
        elseif linktype == "LS"
            6
        elseif linktype == "LX"
            5
        elseif linktype == "LD"
            4
        elseif linktype == "LN"
            3
        elseif linktype == "NR"
            2
        elseif linktype == "NU"
            1
        end
        new{T1, T2}(t1, t2, dt1, dt2, comp_crsp_priority, crsp_comp_priority)
    end
end
```

While most of the default functions for `AbstractLinkPair` would work with this new type (`parentID`, `childID`, `min_date`, `max_date`, `Base.in`), the one that does not is `priority`, which determines which `AbstractLinkPair` is preferable. Since the direction of the link matters, two new `priority` functions are required:
```julia
function WRDSMerger.priority(data::CrspCompLinkPair{GVKey, T2}) where {T2<:AbstractIdentifier}
    data.comp_crsp_priority + data.crsp_comp_priority / 10
end

function WRDSMerger.priority(data::CrspCompLinkPair{T1, GVKey}) where {T1<:AbstractIdentifier}
    data.crsp_comp_priority + data.comp_crsp_priority / 10
end
```

While this case is not used by default in this package, following similar methodology could allow for more complex priority structures.

## Linking Download Functions

```@docs
WRDSMerger.download_crsp_links
WRDSMerger.download_comp_crsp_links
WRDSMerger.download_comp_cik_links
WRDSMerger.download_ibes_links
WRDSMerger.download_option_crsp_links
WRDSMerger.download_ravenpack_links
```

## Changing The Priority for Permno

A single company can have many securities, therefore, there might be multiple options when linking these items. For example, a single [`GVKey`](@ref) or [`Permco`](@ref) will match to multiple [`Permno`](@ref)s. In some tables in WRDS (such as in the case of `GVKey` <-> `Permno`), there are explicit primary identifier markers provided, improving the match. In others, there are not (as in `Permco` <-> `Permno`). This is a particular problem for `Permno` since this package prioritizes matches through `Permno` (as discussed in [Supremacy of Permno](@ref)).

The most common method to resolve these matches is to find the `Permno` that has the largest market capitalization on the day of the match since that should be the primary identifier. This is difficult to do in a package like this where the values are, ideally, predetermined. Therefore, the default behavior is to average the market capitalization over the period of the link and choose the higher average market capitalization. This behavior is convenient (requiring only a single SQL download), but potentially inconsistent with the end goal. Specifically, if one link has a lower average market capitalization (perhaps due to a long time window where the value was lower) than another link, this package might pick the `Permno` with a smaller market capitalization on the day of the match.

This is a proposed alternative that makes use of the [AbnormalReturns.jl](https://github.com/junder873/AbnormalReturns.jl) package to provide a quick lookup of the market capitalization just before the link:

First, stock price data is required:
```julia
using WRDSMerger, DataFramesMeta, AbnormalReturns
df = raw_sql(wrds_conn, "SELECT permno, date, abs(prc) * shrout AS mkt_cap FROM crsp.dsf")
@rtransform!(df, :mkt_cap = coalesce(:mkt_cap, 0.0))
```

!!! note
    It is recommended to provide some filter on the WRDS download as the `crsp.dsf` file has over 100 million rows, downloading this data takes a lot of ram, peaking at ~20 GB. Most obviously, selecting dates beyond a certain point helps a lot.

AbnormalReturns needs a market calendar, instead of downloading something, just reuse the dates from `df` and load that into a `MarketData` object:
```julia
mkt_data = MarketData(
    @by(df, :date, :full_mkt = mean(:mkt_cap)),
    df
)
```

Then we need to redefine how WRDSMerger goes about choosing between two links when the outcome is a `Permno`. It is also important to do some error checking since AbnormalReturns does not accept cases when the date is out of the range available or the `Permno` is not in the dataset. WRDSMerger determines priority uses the `is_higher_priority` function, which checks the priority of two `AbstractLinkPair`s and compares them. Therefore, changing the `priority` function slightly when the outcome is a `Permno` will create the necessary changes:
```julia
function WRDSMerger.priority(
    data::AbstractLinkPair{T1, Permno},
    dt::Date;
    mkt_data=mkt_data # need the market data defined above
) where {T1}
    if dt < AbnormalReturns.cal_dt_min(mkt_data.calendar) || dt > AbnormalReturns.cal_dt_min(mkt_data.calendar)
        return 0.0
    end
    if dt > AbnormalReturns.cal_dt_min(mkt_data.calendar)
        # typically, the market cap on the day before is checked
        # but it is also important to avoid going outside the calendar
        # range
        dt = BusinessDays.advancebdays(mkt_data.calendar, dt, -1)
    end
    permno_val = WRDSMerger.childID(data)
    if haskey(mkt_data.firmdata, permno_val)
        coalesce(
            mkt_data[permno_val, dt, :mkt_cap], # returns value or missing
            0.0
        )
    else
        0.0
    end
end
```

This method is obviously slower than the default setup, but would provide the market capitalization on the day before the match.

This is not the default in this package since many of these operations are costly, particularly downloading the data.

## Adding New Identifiers

There are likely other identifiers in WRDS that are not included by default in this package, making it necessary to define a new identifier. This is quite easy. First, define a new type:
```julia
struct IdentiferName <: FirmIdentifier
    val::String
    IdentifierName(x::AbstractString) = new(x)
end

WRDSMerger.value(x::IdentifierName) = x.val
```
Replacing `FirmIdentifier` with `SecurityIdentifier` if necessary and choosing between `String` or `Int` or some other type.

Next, provide the information that links this new identifier to some other identifier in the package. This is done by calling `new_link_method`:
```@docs
WRDSMerger.new_link_method
```
Specifically the method with a vector of `AbstractLinkPair`s or the dictionary version. Therefore, you need to create a vector of these links, I will assume use of the `LinkPair` type, but this can be adjusted as discussed in [Defining New `AbstractLinkPair`s](@ref). A `LinkPair` requires 5 elements: the ID it is coming from (parent ID), the ID it is going to (child ID), a start and end date, and a priority (though the start and end date and priority have defaults). Therefore, it is easiest if you create a DataFrame that has similar data (i.e., a column of parent ID, child ID, start date, end date, priority). This package then has a function that allows you to create the bi-directional links required, `create_link_pair`:
```@docs
WRDSMerger.create_link_pair
```

Since this returns a tuple of dictionaries, each needs to be passed to `new_link_method` to create the bidirectional links. Then, to create links beyond just T1 <-> T2, call `create_all_links()`.

## Other Functions

```@docs
WRDSMerger.choose_best_match
WRDSMerger.check_priority_errors
WRDSMerger.is_higher_priority
WRDSMerger.identify_overlaps
WRDSMerger.value
WRDSMerger.all_pairs
```