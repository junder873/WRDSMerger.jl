
function identify_overlaps(dts1::AbstractVector, dts2::AbstractVector)
    out = Set{Date}()
    cur_dates = sort(vcat(dts1, dts2))
    for i in 1:length(cur_dates)-1
        c = 0
        for j in eachindex(dts1, dts2)
            if cur_dates[i] >= dts1[j] && cur_dates[i+1] <= dts2[j]#test ⊆ d
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

function check_priority_errors(data::AbstractVector{T}) where {T}
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
        cur_max = argmax(possible)
        for i in 1:length(possible)
            if i == cur_max
                continue
            end
            if !(possible[i] < possible[cur_max]) && childID(possible[i]) != childID(possible[cur_max])
                # println(possible)
                return true
            end
        end
    end
    false
end

function LinkSet(data::Vector{L}) where {T1, T2, L<:LinkPair{T1, T2}}
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
        @warn("There are $(sum(temp)) cases of overlapping identifiers linking \
        $T1 -> $T2 that do not have a priority, links might be inconsistent")
    end
    LinkSet(out)
end
##

function create_link_pair(
    ::Type{T1},
    ::Type{T2},
    df::DataFrame,
    cols...
) where {T1<:AbstractIdentifier, T2<:AbstractIdentifier}
    df = select(df, cols...) |> unique
    cols1 = [cols...]
    cols2 = [cols[2], cols[1], cols[3:end]...]
    dropmissing!(df, [cols[1], cols[2]])
    df[!, cols[1]] = T1.(df[:, cols[1]])
    df[!, cols[2]] = T2.(df[:, cols[2]])
    data1 = Vector{LinkPair{T1, T2}}(undef, nrow(df))
    data2 = Vector{LinkPair{T2, T1}}(undef, nrow(df))
    for i in 1:nrow(df)
        data1[i] = LinkPair(Tuple(df[i, cols1])...)
        data2[i] = LinkPair(Tuple(df[i, cols2])...)
    end
    (
        LinkSet(data1),
        LinkSet(data2)
    )
end

function generate_ibes_links(
    conn::Union{LibPQ.Connection, DBInterface.Connection};
    linkdata = GENERAL_LINK_DATA,
    main_table=default_tables["wrdsapps_ibcrsphist"]
)
    df = download_ibes_links(conn; main_table)
    generate_ibes_links(df; linkdata)
end
function generate_ibes_links(
    df_in::AbstractDataFrame;
    linkdata = GENERAL_LINK_DATA
)
    df = DataFrame(df_in)
    df = dropmissing(df, [:permno, :ncusip])
    df[!, :priority] = 1 ./ df[:, :score]
    temp = create_link_pair(
        IbesTicker,
        Permno,
        df,
        :ticker,
        :permno,
        :sdate,
        :edate,
        :priority
    )
    update_links!(linkdata, temp[1])
    update_links!(linkdata, temp[2])
    temp = create_link_pair(
        IbesTicker,
        Cusip,
        df,
        :ticker,
        :ncusip,
        :sdate,
        :edate,
        :priority
    )
    update_links!(linkdata, temp[1])
    update_links!(linkdata, temp[2])
    df_in
end


function generate_crsp_links(
    conn::Union{LibPQ.Connection, DBInterface.Connection};
    linkdata = GENERAL_LINK_DATA,
    main_table=default_tables["crsp_stocknames"],
    stockfile=default_tables["crsp_stock_data"]
)
    df = download_crsp_links(conn; main_table, stockfile)
    generate_crsp_links(df; linkdata)
end 
function generate_crsp_links(
    df_in::AbstractDataFrame;
    linkdata = GENERAL_LINK_DATA
)
    df = DataFrame(df_in)
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
                v1[1],
                v2[1],
                df,
                v1[2],
                v2[2],
                :namedt,
                :nameenddt,
                :mkt_cap
            )
            if !(
                (v1[1] == NCusip && v2[1] == NCusip6)
                || (v1[1] == Cusip && v2[1] == Cusip6)
            )# don't create links for NCusip -> NCusip6 or Cusip -> Cusip6
            # since there is a simpler definition
                update_links!(linkdata, temp[1])
            end
            update_links!(linkdata, temp[2])
        end
    end
    df_in
end

function generate_comp_crsp_links(
    conn::Union{LibPQ.Connection, DBInterface.Connection};
    linkdata = GENERAL_LINK_DATA,
    main_table=default_tables["crsp_a_ccm_ccmxpf_lnkhist"]
)
    df = download_comp_crsp_links(conn; main_table)
    generate_comp_crsp_links(df; linkdata)
end
function generate_comp_crsp_links(
    df_in::AbstractDataFrame;
    linkdata = GENERAL_LINK_DATA
)
    df = DataFrame(df_in)
    for i in 1:nrow(df)
        # the notes specifically point out that if the linktype is
        # "LS", then the gvkey - permco link is not valid, so this
        # specifies that
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
        update_links!(linkdata, temp[1])
        update_links!(linkdata, temp[2])
    end
    df_in
end

function generate_comp_cik_links(
    conn::Union{LibPQ.Connection, DBInterface.Connection};
    linkdata = GENERAL_LINK_DATA,
    main_table=default_tables["comp_company"]
)
    df = download_comp_cik_links(conn; main_table)
    generate_comp_cik_links(df; linkdata)
end
function generate_comp_cik_links(
    df_in::AbstractDataFrame;
    linkdata = GENERAL_LINK_DATA
)
    df = DataFrame(df_in)
    temp = create_link_pair(
        GVKey,
        CIK,
        df,
        :gvkey,
        :cik
    )
    update_links!(linkdata, temp[1])
    update_links!(linkdata, temp[2])
    df_in
end

function prev_value(x::AbstractVector{T}) where {T}
    out = Vector{Union{Missing, T}}(missing, length(x))
    for i in 1:length(x)-1
        out[i] = x[i+1] - Day(1)
    end
    out
end

function generate_option_crsp_links(
    conn::Union{LibPQ.Connection, DBInterface.Connection};
    linkdata = GENERAL_LINK_DATA,
    main_table=default_tables["optionm_all_secnmd"]
)
    df = download_option_crsp_links(conn; main_table)
    generate_option_crsp_links(df; linkdata)
end
function generate_option_crsp_links(
    df_in::AbstractDataFrame;
    linkdata = GENERAL_LINK_DATA
)
    df = DataFrame(df_in)
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
        SecID,
        NCusip,
        df,
        :secid,
        :cusip,
        :effect_date,
        :end_date
    )
    update_links!(linkdata, temp[1])
    update_links!(linkdata, temp[2])
    df_in
end


function adjust_next_day(s, e)
    for i in 1:length(e)-1
        if e[i] |> ismissing
            e[i] = s[i+1] - Day(1)
        elseif s[i+1] == e[i]
            e[i] = e[i] - Day(1)
        end
    end
    e
end

function generate_ravenpack_links(
    conn::Union{LibPQ.Connection, DBInterface.Connection};
    linkdata = GENERAL_LINK_DATA,
    main_table=default_tables["ravenpack_common_rp_entity_mapping"]
)
    df = download_ravenpack_links(conn; main_table)
    generate_ravenpack_links(df; linkdata)
end
function generate_ravenpack_links(
    df_in::AbstractDataFrame;
    linkdata = GENERAL_LINK_DATA
)
    df = DataFrame(df_in)
    df = sort(df, [:rp_entity_id, :range_start])
    df[!, :range_start] = Date.(df[:, :range_start])
    df = transform(groupby(df, :rp_entity_id), [:range_start, :range_end] => (x, y) -> adjust_next_day(x, y) => :range_end)
    temp = create_link_pair(
        RPEntity,
        NCusip6,
        df,
        :rp_entity_id,
        :ncusip,
        :range_start,
        :range_end
    )
    update_links!(linkdata, temp[1])
    update_links!(linkdata, temp[2])
    df_in
end