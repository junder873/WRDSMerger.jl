
"""
This function looks for overlapping periods. It takes a list of all dates
and checks if individual sub periods are a subset of multiple periods.
"""
function identify_overlaps(dts1::AbstractVector, dts2::AbstractVector)
    out = Set{Date}()
    cur_dates = sort(vcat(dts1, dts2))
    for i in 1:length(cur_dates)-1
        c = 0
        for j in eachindex(dts1, dts2)
            if cur_dates[i] >= dts1[j] && cur_dates[i+1] <= dts2[j]
                c += 1
            end
        end
        if c ≥ 2
            push!(out, cur_dates[i])
            push!(out, cur_dates[i+1])
        end
    end
    out
end

"""
This function tests whether there are any dates that are in multiple
`AbstractLinkPair`s and those links have equivalent priority. If this function
returns `true`, then there is at least a date where there is no distinction
between two links. The way [`choose_best_match`](@ref) works, the first in
the vector will be chosen.
"""
function check_priority_errors(data::AbstractVector{T}) where {T<:AbstractLinkPair}
    if length(data) == 1
        return false
    end
    dates_to_check = identify_overlaps(min_date.(data), max_date.(data))
    for d in dates_to_check
        possible = T[]
        for v in data
            if d in v
                push!(possible, v)
            end
        end
        best = 0
        for (i, v) in enumerate(possible)
            # either first or the current one is higher priority
            if best == 0 || is_higher_priority(possible[i], possible[best])
                best = i
            end
        end
        for i in 1:length(possible)
            if i == best
                continue
            end
            if !(is_higher_priority(possible[best], possible[i])) && childID(possible[i]) != childID(possible[best])
                # println(possible)
                return true
            end
        end
    end
    false
end

"""
    Dict(data::AbstractVector{L}) where {T1, T2, L<:AbstractLinkPair{T1, T2}}

Converts a vector of `AbstractLinkPair`s to a dictionary where each T1 is a key
in the dictionary and the values are vectors of L. It also checks whether those
vectors ever have overlapping inconsistent priorities.
"""
function Base.Dict(data::AbstractVector{L}) where {T1, T2, L<:AbstractLinkPair{T1, T2}}
    out = Dict{T1, Vector{L}}()
    sizehint!(out, length(data))
    for v in data
        if !haskey(out, parentID(v))
            out[parentID(v)] = Vector{L}()
        end
        push!(out[parentID(v)], v)
    end
    temp = check_priority_errors.(values(out))
    if any(temp)
        @warn("There are $(sum(temp)) cases of overlapping identifiers linking " *
        "$T1 -> $T2 that do not have a priority, this might create unintended links")
    end
    out
end

"""
    function create_link_pair(
        ::Type{LP},
        ::Type{T1},
        ::Type{T2},
        df::DataFrame,
        cols...
    ) where {T1<:AbstractIdentifier, T2<:AbstractIdentifier, LP<:AbstractLinkPair}

Generic function that creates an AbstractLinkPair based on the types
and a DataFrame. `cols...` should be a list of column names in the DataFrame,
the first being ready to convert to type T1 and the second ready to convert
to type T2. This function returns a tuple of two dictionaries:
`(Dict{T1, LP{T1, T2}},Dict{T2, LP{T2, T1}})`
which is easily passed to [`new_link_method`](@ref).

## Example
```julia
create_link_pair(
    LinkPair,
    Permno,
    NCusip,
    df,
    :permno,
    :ncusip,
    :namedt,
    :nameenddt,
    :priority
)
```
"""
function create_link_pair(
    ::Type{LP},
    ::Type{T1},
    ::Type{T2},
    df::DataFrame,
    cols...
) where {T1<:AbstractIdentifier, T2<:AbstractIdentifier, LP<:AbstractLinkPair}
    df = select(df, cols...) |> unique
    cols1 = [cols...]
    cols2 = [cols[2], cols[1], cols[3:end]...]
    dropmissing!(df, [cols[1], cols[2]])
    df[!, cols[1]] = T1.(df[:, cols[1]])
    df[!, cols[2]] = T2.(df[:, cols[2]])
    data1 = [LP(x...) for x in Tuple(eachrow(df[:, cols1]))]
    data2 = [LP(x...) for x in Tuple(eachrow(df[:, cols2]))]
    (
        Dict(data1),
        Dict(data2)
    )
end

"""
    generate_ibes_links(
        conn;
        main_table=default_tables["wrdsapps_ibcrsphist"]
    )

    generate_ibes_links(df::AbstractDataFrame)

Generates the methods between IbesTicker and Permno/NCusip based on a standard
WRDS file. If a database connection is provided, then it will download
the table, otherwise, it can use a provided DataFrame.
"""
function generate_ibes_links(
    conn;
    main_table=default_tables["wrdsapps_ibcrsphist"]
)
    df = download_ibes_links(conn; main_table)
    generate_ibes_links(df)
end
function generate_ibes_links(
    df_in::AbstractDataFrame
)
    df = select(df_in, :ticker, :permno, :ncusip, :sdate, :edate, :score) |> copy
    df = dropmissing(df, [:permno, :ncusip])
    df[!, :priority] = 1 ./ df[:, :score]
    temp = create_link_pair(
        LinkPair,
        IbesTicker,
        Permno,
        df,
        :ticker,
        :permno,
        :sdate,
        :edate,
        :priority
    )
    new_link_method(temp[1])
    new_link_method(temp[2])
    temp = create_link_pair(
        LinkPair,
        IbesTicker,
        Cusip,
        df,
        :ticker,
        :ncusip,
        :sdate,
        :edate,
        :priority
    )
    new_link_method(temp[1])
    new_link_method(temp[2])
    df_in
end

"""
    generate_crsp_links(
        conn;
        main_table=default_tables["crsp_stocknames"],
        stockfile=default_tables["crsp_stock_data"]
    )

    generate_crsp_links(df::AbstractDataFrame)

Generates the methods linking 
Permno, Permco, Cusip, NCusip, Cusip6, NCusip6 and Ticker to each other.
If a database connection is provided, then it will download
the table, otherwise, it can use a provided DataFrame.

The file used (`crsp.stocknames`), does not have a clear way to differentiate
different priorities. The most common way is to calculate the market cap
of any conflicting securities to determine the best option. The ideal is the
market cap on the relevant day, but since this needs a static value, the
default download is to average the market cap over the relevant period.
"""
function generate_crsp_links(
    conn;
    main_table=default_tables["crsp_stocknames"],
    stockfile=default_tables["crsp_stock_data"]
)
    df = download_crsp_links(conn; main_table, stockfile)
    generate_crsp_links(df)
end 
function generate_crsp_links(
    df_in::AbstractDataFrame;
    priority_col=:mkt_cap,
    cols = [
        :permno,
        :permco,
        :ncusip,
        :cusip,
        :ticker,
        :namedt,
        :nameenddt
    ]
)
    df = select(
        df_in,
        vcat(cols, priority_col)...
    ) |> copy
    df[!, :ncusip2] = df[:, :ncusip]
    df[!, :cusip2] = df[:, :cusip]
    ids = [
        (Permno, :permno),
        (Permco, :permco),
        (NCusip, :ncusip),
        (NCusip6, :ncusip2),
        (Ticker, :ticker),
        (Cusip, :cusip),
        (Cusip6, :cusip2)
    ]
    for (i, v1) in enumerate(ids)
        for v2 in ids[i+1:end]
            if v1[2] == v2[2] # for when the two values are equal
                continue
            end

            temp = create_link_pair(
                LinkPair,
                v1[1],
                v2[1],
                df,
                v1[2],
                v2[2],
                :namedt,
                :nameenddt,
                priority_col
            )
            if !(
                (v1[1] == NCusip && v2[1] == NCusip6)
                || (v1[1] == Cusip && v2[1] == Cusip6)
            )# don't create links for NCusip -> NCusip6 or Cusip -> Cusip6
            # since there is a simpler definition
                new_link_method(temp[1])
            end
            new_link_method(temp[2])
        end
    end
    df_in
end

"""
    generate_comp_crsp_links(
        conn;
        main_table=default_tables["crsp_a_ccm_ccmxpf_lnkhist"]
    )

    generate_comp_crsp_links(df::AbstractDataFrame)

Generates the methods linking GVKey and Permno/Permco based on 
the CRSP/Compustat merged annual file link history
(`crsp_a_ccm.ccmxpf_lnkhist`).
If a database connection is provided, then it will download
the table, otherwise, it can use a provided DataFrame.
"""
function generate_comp_crsp_links(
    conn;
    main_table=default_tables["crsp_a_ccm_ccmxpf_lnkhist"]
)
    df = download_comp_crsp_links(conn; main_table)
    generate_comp_crsp_links(df)
end
function generate_comp_crsp_links(
    df_in::AbstractDataFrame;
    cols=[
        :gvkey,
        :lpermno,
        :lpermco,
        :linkdt,
        :linkenddt,
        :linkprim,
        :linktype
    ]
)
    df = select(df_in, cols...) |> copy
    for i in 1:nrow(df)
        # the notes specifically point out that if the linktype is
        # "LS", then the gvkey - permco link is not valid, so this
        # specifies that
        allowmissing!(df, :lpermco)
        if df[i, :linktype] == "LS"
            df[i, :lpermco] = missing
        end
    end
    ids = [
        (Permco, :lpermco),
        (Permno, :lpermno),
    ]
    for v in ids
        temp = create_link_pair(
            LinkPair,
            GVKey,
            v[1],
            df,
            :gvkey,
            v[2],
            :linkdt,
            :linkenddt,
            :linkprim,
            :linktype
        )
        new_link_method(temp[1])
        new_link_method(temp[2])
    end
    df_in
end

"""
    generate_comp_cik_links(
        conn;
        main_table=default_tables["comp_company"]
    )

    generate_comp_cik_links(df::AbstractDataFrame)

Generates the methods linking GVKey and CIK based on 
the Compustat company name file (`comp.company`). GVKey and CIK do not have
any date conditions, so this download is relatively simple.
If a database connection is provided, then it will download
the table, otherwise, it can use a provided DataFrame.
"""
function generate_comp_cik_links(
    conn;
    main_table=default_tables["comp_company"]
)
    df = download_comp_cik_links(conn; main_table)
    generate_comp_cik_links(df)
end
function generate_comp_cik_links(
    df_in::AbstractDataFrame;
    cols=[
        :gvkey,
        :cik
    ]
)
    df = select(df_in, cols...) |> copy
    temp = create_link_pair(
        LinkPair,
        GVKey,
        CIK,
        df,
        :gvkey,
        :cik
    )
    new_link_method(temp[1])
    new_link_method(temp[2])
    df_in
end

function prev_value(x::AbstractVector{T}) where {T}
    out = Vector{Union{Missing, T}}(missing, length(x))
    for i in 1:length(x)-1
        out[i] = x[i+1] - Day(1)
    end
    out
end

"""
    generate_option_crsp_links(
        conn;
        main_table=default_tables["optionm_all_secnmd"]
    )

    generate_option_crsp_links(df::AbstractDataFrame)

Generates the methods linking SecID and NCusip based on 
the option names file (`optionm_all.secnmd`). This file only provides an
"effective date", so it is assumed that once the next "effective date" 
occurs, the link is no longer valid.
If a database connection is provided, then it will download
the table, otherwise, it can use a provided DataFrame.
"""
function generate_option_crsp_links(
    conn;
    main_table=default_tables["optionm_all_secnmd"]
)
    df = download_option_crsp_links(conn; main_table)
    generate_option_crsp_links(df)
end
function generate_option_crsp_links(
    df_in::AbstractDataFrame;
    cols=[
        :secid,
        :cusip,
        :effect_date
    ]
)
    df = select(df_in, cols...) |> copy
    for i in 1:nrow(df)
        allowmissing!(df, :cusip)
        if df[i, :cusip] == "99999999"
            df[i, :cusip] = missing
        end
    end
    df[!, :effect_date] = Date.(df[:, :effect_date])
    sort!(df, [:secid, :effect_date])
    df = transform(groupby(df, :secid), :effect_date => prev_value => :end_date)
    temp = create_link_pair(
        LinkPair,
        SecID,
        NCusip,
        df,
        :secid,
        :cusip,
        :effect_date,
        :end_date
    )
    new_link_method(temp[1])
    new_link_method(temp[2])
    df_in
end


function adjust_next_day(s, e)
    for i in 1:length(e)-1
        # if e[i] |> ismissing
        #     e[i] = s[i+1] - Day(1)
        if !ismissing(e[i]) && s[i+1] == e[i]
            e[i] = e[i] - Day(1)
        end
    end
    e
end

"""
    generate_ravenpack_links(
        conn;
        main_table=default_tables["ravenpack_common_rp_entity_mapping"]
    )

    generate_ravenpack_links(df::AbstractDataFrame)

Generates the methods linking RPEntity and NCusip6 based on 
the RavenPack Entity Mapping file (`ravenpack_common.rp_entity_mapping`).
This file is very messy, so the automatic options make several assumptions
and filters. First, when downloading the data, it filters any NCusip in the
RavenPack file that is not in the `crsp.stocknames` file. Second, for each
RPEntity, if the end date is missing, it assumes the next start date is
the appropriate end date for the link.
If a database connection is provided, then it will download
the table, otherwise, it can use a provided DataFrame.
"""
function generate_ravenpack_links(
    conn;
    main_table=default_tables["ravenpack_common_rp_entity_mapping"]
)
    df = download_ravenpack_links(conn; main_table)
    generate_ravenpack_links(df)
end
function generate_ravenpack_links(
    df_in::AbstractDataFrame;
    cols=[
        :rp_entity_id,
        :range_start,
        :range_end,
        :ncusip
    ]
)
    df = select(df_in, cols...) |> copy
    df = sort(df, [:rp_entity_id, :range_start])
    df[!, :range_start] = Date.(df[:, :range_start])
    df = transform(groupby(df, :rp_entity_id), [:range_start, :range_end] => (x, y) -> adjust_next_day(x, y) => :range_end)
    temp = create_link_pair(
        LinkPair,
        RPEntity,
        NCusip6,
        df,
        :rp_entity_id,
        :ncusip,
        :range_start,
        :range_end
    )
    new_link_method(temp[1])
    new_link_method(temp[2])
    df_in
end

function create_all_links()
    needed_links=all_pairs(AbstractIdentifier, AbstractIdentifier; test_fun=method_is_missing)
    base_links=all_pairs(AbstractIdentifier, AbstractIdentifier)
    for l in needed_links
        new_link_method(l...; current_links=base_links)
    end
end