
"""
This function tests whether there are any dates that are in multiple
`AbstractLinkPair`s and those links have equivalent priority. If this function
returns `true`, then there is at least a date where there is no distinction
between two links. The way [`choose_best_match`](@ref) works, the first in
the vector will be chosen.

The algorithm uses a sweep-line approach: intervals are sorted by start date
to efficiently find overlapping pairs. When an overlap between two links with
different children is found, a representative date is checked against *all*
active links to determine whether a higher-priority link resolves the tie.
This avoids false positives where two low-priority links tie but a third
higher-priority link takes precedence.
"""
function check_priority_errors(data::AbstractVector{T}) where {T<:AbstractLinkPair}
    n = length(data)
    n ≤ 1 && return false

    # Fast path: if every link maps to the same child, overlapping
    # intervals can never cause an ambiguous conflict.
    first_child = childID(data[1])
    all_same = true
    for i in 2:n
        if childID(data[i]) != first_child
            all_same = false
            break
        end
    end
    all_same && return false

    # Collect all unique boundary dates, sort them. The set of active
    # links only changes at these boundaries, so checking at each
    # boundary is sufficient to find all conflicts.
    boundary_dates = Set{Date}()
    for v in data
        push!(boundary_dates, min_date(v))
        push!(boundary_dates, max_date(v))
    end
    sorted_dates = sort!(collect(boundary_dates))

    # Pre-sort data by start date to allow early termination when
    # scanning for active links at a given date.
    perm = sortperm(data; by=min_date)

    # Reusable buffer for links active at a given date
    possible = T[]

    for d in sorted_dates
        # Find all links active on this date using sorted order
        empty!(possible)
        for idx in 1:n
            k = perm[idx]
            min_date(data[k]) > d && break  # remaining links start after d
            if max_date(data[k]) >= d
                push!(possible, data[k])
            end
        end

        length(possible) ≤ 1 && continue

        # Find the highest-priority link
        best = 1
        for i in 2:length(possible)
            if is_higher_priority(possible[i], possible[best])
                best = i
            end
        end

        # Check whether any other link ties the best with a different child
        for i in 1:length(possible)
            i == best && continue
            if !(is_higher_priority(possible[best], possible[i])) && childID(possible[i]) != childID(possible[best])
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
        vec = get!(Vector{L}, out, parentID(v))
        push!(vec, v)
    end
    error_count = 0
    for group in values(out)
        error_count += check_priority_errors(group)
    end
    if error_count > 0
        @warn("There are $error_count cases of overlapping identifiers linking " *
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
        sym1::Symbol,
        sym2::Symbol,
        dt1::Union{Symbol, Missing}=missing,
        dt2::Union{Symbol, Missing}=missing,
        priority_sym::Union{Symbol, Missing}=missing,
        priority_sym2::Union{Symbol, Missing}=missing
    ) where {T1<:AbstractIdentifier, T2<:AbstractIdentifier, LP<:AbstractLinkPair}

Generic function that creates an `AbstractLinkPair` based on the types
and a DataFrame. `sym1` and `sym2` are the column names in `df` whose values
will be converted to types `T1` and `T2`, respectively. `dt1` and `dt2` are
optional column names for the start and end dates of each link. `priority_sym`
is an optional column name used as the priority when building links from `T1`
to `T2`, and `priority_sym2` is an optional separate priority column for the
reverse (`T2` to `T1`) direction; when `priority_sym2` is `missing`, `priority_sym`
is used for both directions, and when both are `missing` a default priority of
`0.0` is used.

The function selects only the relevant columns, drops rows where `sym1` or
`sym2` are `missing`, and returns a tuple of two dictionaries:
`(Dict{T1, LP{T1, T2}}, Dict{T2, LP{T2, T1}})`
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
    sym1::Symbol,
    sym2::Symbol,
    dt1::Union{Symbol, Missing}=missing,
    dt2::Union{Symbol, Missing}=missing,
    priority_sym::Union{Symbol, Missing}=missing,
    priority_sym2::Union{Symbol, Missing}=missing
) where {T1<:AbstractIdentifier, T2<:AbstractIdentifier, LP<:AbstractLinkPair}
    cols = [sym1, sym2]
    if !ismissing(dt1)
        push!(cols, dt1)
    end
    if !ismissing(dt2)
        push!(cols, dt2)
    end
    if !ismissing(priority_sym)
        push!(cols, priority_sym)
    end
    if !ismissing(priority_sym2) && priority_sym2 != priority_sym
        push!(cols, priority_sym2)
    end
    df = select(df, cols) |> unique

    dropmissing!(df, [sym1, sym2])
    df[!, sym1] = T1.(df[:, sym1])
    df[!, sym2] = T2.(df[:, sym2])
    data1 = LP.(
        df[:, sym1],
        df[:, sym2],
        ismissing(dt1) ? missing : df[:, dt1],
        ismissing(dt2) ? missing : df[:, dt2],
        ismissing(priority_sym) ? 0.0 : df[:, priority_sym]
    )
    data2 = LP.(
        df[:, sym2],
        df[:, sym1],
        ismissing(dt1) ? missing : df[:, dt1],
        ismissing(dt2) ? missing : df[:, dt2],
        ismissing(priority_sym2) ? (ismissing(priority_sym) ? 0.0 : df[:, priority_sym]) : df[:, priority_sym2]
    )
    (
        Dict(data1),
        Dict(data2)
    )
end

"""
    generate_ibes_links(
        conn,
        main_table=default_tables["wrdsapps_ibcrsphist"]
    )

    generate_ibes_links(df::AbstractDataFrame)

Generates the methods between IbesTicker and Permno/NCusip based on a standard
WRDS file. If a database connection is provided, then it will download
the table, otherwise, it can use a provided DataFrame.
"""
function generate_ibes_links(
    conn,
    main_table=default_tables["wrdsapps_ibcrsphist"]
)
    df = download_ibes_links(conn, main_table)
    generate_ibes_links(df)
end
function generate_ibes_links(
    df_in::AbstractDataFrame
)
    df = select(df_in, :ticker, :permno, :ncusip, :sdate, :edate, :score) |> copy
    dropmissing!(df, [:permno, :ncusip])
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
        NCusip,
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


same_cusip(::Type{Cusip{HistCode}}, ::Type{Cusip6{HistCode}}) where {HistCode} = true
same_cusip(::Type{<:AbstractIdentifier}, ::Type{<:AbstractIdentifier}) = false

"""
    generate_crsp_links(
        conn;
        main_table=default_tables["crsp_stocknames"],
        stockfile=default_tables["crsp_stock_data"]
    )

    generate_crsp_links(df::AbstractDataFrame)

Generates the methods linking 
Permno, Permco, HdrCusip, NCusip, HdrCusip6, NCusip6 and Ticker to each other.
If a database connection is provided, then it will download
the table, otherwise, it can use a provided DataFrame.

The file used (`crsp.stocknames`), does not have a clear way to differentiate
different priorities. The most common way is to calculate the market cap
of any conflicting securities to determine the best option. The ideal is the
market cap on the relevant day, but since this needs a static value, the
default download is to average the market cap over the relevant period.
"""
function generate_crsp_links(
    conn,
    main_table=default_tables["crsp_stocknames"],
    stockfile=default_tables["crsp_stock_data"]
)
    df = download_crsp_links(conn, main_table, stockfile)
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
        (HdrCusip, :cusip),
        (HdrCusip6, :cusip2)
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
            if !same_cusip(v1[1], v2[1])# don't create links for Cusip -> Cusip6
            # since there is a simpler definition
                new_link_method(temp[1])
            end
            new_link_method(temp[2])
        end
    end
    df_in
end

"""
    generate_crsp_links_v2(
        conn,
        main_table=default_tables["crsp_stocknames_v2"],
        stockfile=default_tables["crsp_stock_data_v2"]
    )

    generate_crsp_links_v2(df::AbstractDataFrame)

Generates the methods linking Permno, Permco, Cusip, NCusip, Cusip6, NCusip6 and Ticker to each other.
If a database connection is provided, then it will download
the table, otherwise, it can use a provided DataFrame.

Somewhat confusingly, the v2 files have HdrCusip and Cusip, but the Cusip is
equivalent to the old NCusip and the HdrCusip is equivalent to the old Cusip.
"""
function generate_crsp_links_v2(
    conn,
    main_table=default_tables["crsp_stocknames_v2"],
    stockfile=default_tables["crsp_stock_data_v2"]
)
    df = download_crsp_links_v2(conn, main_table, stockfile)
    generate_crsp_links_v2(df)
end
function generate_crsp_links_v2(
    df_in::AbstractDataFrame;
    priority_col=:mkt_cap,
    cols = [
        :permno,
        :permco,
        :cusip,
        :hdrcusip,
        :ticker,
        :namedt,
        :nameenddt
    ]
)
    df = select(
        df_in,
        vcat(cols, priority_col)...
    ) |> copy
    df[!, :hdrcusip2] = df[:, :hdrcusip]
    df[!, :cusip2] = df[:, :cusip]
    ids = [
        (Permno, :permno),
        (Permco, :permco),
        (NCusip, :cusip),
        (NCusip6, :cusip2),
        (Ticker, :ticker),
        (HdrCusip, :hdrcusip),
        (HdrCusip6, :hdrcusip2)
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
            if !same_cusip(v1[1], v2[1])# don't create links for Cusip -> Cusip6
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
        conn,
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
    conn,
    main_table=default_tables["crsp_a_ccm_ccmxpf_lnkhist"]
)
    df = download_comp_crsp_links(conn, main_table)
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
    transform!(df, [:linkprim, :linktype] => ByRow((x, y) -> gvkey_crsp_priority(x, y)) => :priority1)
    transform!(df, [:linkprim, :linktype] => ByRow((x, y) -> crsp_gvkey_priority(x, y)) => :priority2)
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
            :priority1,
            :priority2
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
    conn,
    main_table=default_tables["comp_company"]
)
    df = download_comp_cik_links(conn, main_table)
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
    conn,
    main_table=default_tables["optionm_all_secnmd"]
)
    df = download_option_crsp_links(conn, main_table)
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
        conn,
        main_table=default_tables["ravenpack_common_rp_entity_mapping"],
        cusip_list=default_tables["crsp.stocknames"]
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
    conn,
    main_table=default_tables["ravenpack_common_rp_entity_mapping"],
    cusip_list=default_tables["crsp_stocknames"]
)
    df = download_ravenpack_links(conn, main_table, cusip_list)
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

"""
    create_all_links()

Create indirect links between all identifier types that do not yet have a direct
link method. This should be called after all `generate_*` functions have been run.
It finds all missing identifier pairs and creates linking methods that route through
intermediate identifiers (preferring paths through [`Permno`](@ref), see
[Supremacy of Permno](@ref)).
"""
function create_all_links()
    needed_links=all_pairs(AbstractIdentifier, AbstractIdentifier; test_fun=method_is_missing)
    base_links=all_pairs(AbstractIdentifier, AbstractIdentifier)
    filter!(l -> l ∉ base_links, needed_links)
    for l in needed_links
        new_link_method(l...; current_links=base_links)
    end
end