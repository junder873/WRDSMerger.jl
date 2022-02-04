
# struct LinkResult{T<:FirmIdentifier, D1<:Union{Nothing, Date}, D2<:Union{Nothing, Date}}
#     id::T
#     date_start::D1
#     date_end::D2
# end

# function LinkResult(id_vec::Vector{T}, date1_vec::Vector{Date}, date2_vec::Vector{Date}) where {T <: FirmIdentifier}
#     out = Dict{T, Tuple{Date, Date}}()
#     for i in 1:length(id_vec)
#         if haskey(out, id_vec[i]) && # this is a problem, how do deal with the multiple cases



# struct IDLinkDict{T<:FirmIdentifier, W, D1, D2}
#     links::Dict{T, Vector{LinkResult{W, D1, D2}}}
# end

# get_id(x::LinkResult) = x.id


# function Base.getindex(links::IDLinkDict{T}, id::T) where {T <: FirmIdentifier}
#     get_id.(links[id].links)
# end

# function Base.getindex(links::IDLinkDict{T, W, D1, D2}, id::T, date::Date) where {T, W <: FirmIdentifier, D1, D2 <: Date}
#     res = links[id]
#     out = W[]
#     for v in res.links
#         if v.date_start <= date <= v.date_end
#             push!(out, get_id(v))
#         end
#     end
#     return out
# end

# function Base.getindex(links::IDLinkDict{T, W, D1, D2}, id::T, date::Date) where {T, W <: FirmIdentifier, D1 <: Date, D2 <: Nothing}
#     res = links[id]

#     # assumes the vector is sorted by dates
#     for v in res.links
#         if v.date_start <= date <= v.date_end
#             return get_id(v)
#         end
#     end
#     return W[]
# end

mutable struct LinkTable
    table::String # table name
    id_cols::Vector{String} # id cols as they exist in the database
    date_col_min::Union{Missing, String} # date column as it is in the database, missing for
    # cases where the link is 1:1
    date_col_max::Union{Missing, String} # date column in the database, if missing and min
    # is not missing, then date_col_min is assumed to be the minimum date until a new
    # date is specified
    filters::Dict{String, <:Any} # filters that will be applied when downloading data
    type_translations::Vector{Pair{String, Type{<:FirmIdentifier}}} # translate the column
    # as downloaded to the correct type
end


LinkTable(::Type{Permno}, ::Type{T}) where {T<:CusipAll} = LinkTable(
    default_tables["crsp_stocknames"],
    ["permno", lowercase(string(T))],
    "namedt",
    "nameenddt",
    Dict{String, Any}("ncusip" => missing),
    ["permno" => Permno, lowercase(string(T)) => T]
)
LinkTable(::Type{T}, ::Type{Permno}) where {T<:CusipAll} = LinkTable(Permno, T)

LinkTable(::Type{K}, ::Type{T}) where {K<:CusipAll,T<:CusipAll} = LinkTable(
    default_tables["crsp_stocknames"],
    [lowercase(string(K)), lowercase(string(T))],
    "namedt",
    "nameenddt",
    Dict{String, Any}("ncusip" => missing),
    [lowercase(string(T)) => T, lowercase(string(K)) => K]
)

LinkTable(::Type{Ticker}, ::Type{T}) where {T<:CusipAll} = LinkTable(
    default_tables["crsp_stocknames"],
    ["ticker", lowercase(string(T))],
    "namedt",
    "nameenddt",
    Dict{String, Any}("ncusip" => missing),
    ["ticker" => Ticker, lowercase(string(T)) => T]
)
LinkTable(::Type{T}, ::Type{Ticker}) where {T<:CusipAll} = LinkTable(Ticker, T)

LinkTable(::Type{Permno}, ::Type{Ticker}) = LinkTable(
    default_tables["crsp_stocknames"],
    ["permno", "ticker"],
    "namedt",
    "nameenddt",
    Dict{String, Any}("ncusip" => missing),
    ["permno" => Permno, "ticker" => Ticker]
)
LinkTable(::Type{Ticker}, ::Type{Permno}) = LinkTable(Permno, Ticker)

LinkTable(::Type{Permno}, ::Type{GVKey}) = LinkTable(
    default_tables["crsp_a_ccm_ccmxpf_lnkhist"],
    ["lpermno", "gvkey"],
    "linkdt",
    "linkenddt",
    Dict{String, Any}(
        "linktype" => ["LU", "LC"],
        "linkprim" => ["P", "C"],
        "lpermno" => missing
    ),
    ["lpermno" => Permno, "gvkey" => GVKey]
)
LinkTable(::Type{GVKey}, ::Type{Permno}) = LinkTable(Permno, GVKey)

LinkTable(::Type{Permno}, ::Type{IbesTicker}) = LinkTable(
    default_tables["wrdsapps_ibcrsphist"],
    ["ticker", "permno"],
    "sdate",
    "edate",
    Dict{String, Any}(
        "score" => 1:4,
        "permno" => missing
    ),
    ["permno" => Permno, "ticker" => IbesTicker]
)
LinkTable(::Type{IbesTicker}, ::Type{Permno}) = LinkTable(Permno, IbesTicker)

LinkTable(::Type{NCusip}, ::Type{IbesTicker}) = LinkTable(
    default_tables["wrdsapps_ibcrsphist"],
    ["ticker", "ncusip"],
    "sdate",
    "edate",
    Dict{String, Any}(
        "score" => 1:4,
        "ncusip" => missing
    ),
    ["ncusip" => NCusip, "ticker" => IbesTicker]
)
LinkTable(::Type{IbesTicker}, ::Type{NCusip}) = LinkTable(NCusip, IbesTicker)

LinkTable(::Type{GVKey}, ::Type{CIK}) = LinkTable(
    default_tables["comp_company"],
    ["gvkey", "cik"],
    missing,
    missing,
    Dict{String, Any}(
        "cik" => missing
    ),
    ["gvkey" => GVKey, "cik" => CIK]
)
LinkTable(::Type{CIK}, ::Type{GVKey}) = LinkTable(GVKey, CIK)

# generic function to convert a pair into the appropriate table
LinkTable(x::Pair{Type{<:FirmIdentifier}, Type{<:FirmIdentifier}}) = LinkTable(x[1], x[2])

function Base.merge(
    t1::LinkTable,
    t2::LinkTable
)
    @assert(t1.table == t2.table)
    LinkTable(
        t1.table,
        vcat(t1.id_cols, t2.id_cols) |> unique,
        t1.date_col_min,
        t1.date_col_max,
        merge(t1.filters, t2.filters),
        vcat(t1.type_translations, t2.type_translations) |> unique,
    )
end

function date_cols(table::LinkTable)
    x = String[]
    if !ismissing(table.date_col_min)
        push!(x, table.date_col_min)
    end
    if !ismissing(table.date_col_max)
        push!(x, table.date_col_max)
    end
    x
end

function date_subset(
    x::Tuple{Date, Date},
    y::Tuple{Date, Date}
)
    x[1] >= y[1] && x[2] <= y[2]
end
function date_checks(
    d_vec1,
    d_vec2
)
    if length(d_vec1) == 1
        return [true]
    end
    out = ones(Bool, length(d_vec1))
    for (i, x) in enumerate(zip(d_vec1, d_vec2))
        for y in zip(d_vec1, d_vec2)
            if x != y && date_subset(x, y)
                out[i] = false
                break
            end
        end
    end
    out
end

"""
function link_table(
    conn,
    table::LinkTable,
    fil_type::Vector{T}=FirmIdentifier[]
) where {T<:FirmIdentifier}

Generic function to download data from a linking table
"""
function link_table(
    conn,
    table::LinkTable,
    fil_type::Vector{T}=FirmIdentifier[]
) where {T<:FirmIdentifier}
    if 0 < length(fil_type) <= 1000
        temp_filter = Dict{String, Any}()
        for (key, val) in table.filters
            temp_filter[key] = val
        end
        table.filters = temp_filter
        col_str_temp = ""
        for (col, t) in table.type_translations
            if t == T
                col_str_temp = col
            end
        end
        table.filters[col_str_temp] = value.(fil_type)
    end
    fil = create_filter(table.filters)
    col_str = join(vcat(table.id_cols, date_cols(table)), ", ")
    query = """
    SELECT DISTINCT $col_str FROM $(table.table)
    $fil
    """
    df = WRDSMerger.run_sql_query(conn, query) |> DataFrame
    for (col, t) in table.type_translations
        df[!, col] = t.(df[:, col])
        rename!(df, col => string(t))
    end
    return df
end


"""
Takes a built tree and converts it into a list of pairs
"""
function find_item(T::Type{<:FirmIdentifier}, node::FirmIdentifierNode)
    if node.data == T
        return node
    end
    out = 0
    for x in node

        if x.data == T
            out = x
            break
        end
        out = find_item(T, x)
        if out != 0
            break
        end
    end

    out
end


function parent_list(node::FirmIdentifierNode; out=Type{<:FirmIdentifier}[])
    push!(out, node.data)
    if parent(node) !== nothing
        return parent_list(parent(node); out)
    end
    out
end

function build_list(T::Type{<:FirmIdentifier}, tree::FirmIdentifierNode)
    bot_node = find_item(T, tree)
    if bot_node == 0
        error("The identifier $T is not in the tree, check that all the links work.")
    end
    out = parent_list(bot_node) |> reverse
    out2 = Pair{Type{<:FirmIdentifier}, Type{<:FirmIdentifier}}[]
    for i in 1:length(out)-1
        push!(out2, out[i] => out[i+1])
    end
    out2
end



function adjust_date_cols(df::DataFrame, table::LinkTable, date_min::Date, date_max::Date)
    if ismissing(table.date_col_max) && !ismissing(table.date_col_min)
        col = Dict(table.type_translations...)[table.id_cols[1]] |> string
        df[!, table.date_col_min] = coalesce.(df[:, table.date_col_min], date_min)
        sort!(df, [col, table.date_col_min])
        gdf = groupby(df, [col])
        df = transform(gdf, table.date_col_min => lead => "date_max")
        df[!, "date_max"] = coalesce.(df[:, "date_max"] .- Day(1), date_max)# I subtract a day since use <= later
        table.date_col_max = "date_max"
    elseif ismissing(table.date_col_min) && ismissing(table.date_col_max)
        table.date_col_min = "date_min"
        table.date_col_max = "date_max"
        df[!, "date_min"] .= date_min
        df[!, "date_max"] .= date_max
    else
        df[!, table.date_col_min] = coalesce.(df[:, table.date_col_min], date_min)
        df[!, table.date_col_max] = coalesce.(df[:, table.date_col_max], date_max)

        """
        CRSP seems to have updated the main table used (crsp.stocknames) so that
        there are a bunch of repeated values where a linked pair is a complete
        subset of another set of dates, this tries to correct for that.
    
        This is only necessary in the case that all data comes from WRDS,
        if both date columns are missing then this does not matter and if
        one is missing the adjust_date_cols function handles it as well.
        """
        d_cols = [table.date_col_min, table.date_col_max]
        g_cols = [string(t) for (i, t) in table.type_translations]
        transform!(df, d_cols => ByRow((x, y) -> x:Day(1):y) => :date_range)
        df = combine(groupby(df, g_cols), :date_range => merge_date_ranges => :date_range)
        transform!(df, :date_range => ByRow(x -> (f=x[1], g=x[end])) => d_cols)
        df = subset(
            groupby(df, g_cols),
            d_cols => (x, y) -> date_checks(x, y)
        )
    end
    df[!, table.date_col_min] = Date.(df[:, table.date_col_min])
    df[!, table.date_col_max] = Date.(df[:, table.date_col_max])
    return df
end


function unique_tables(
    list,
    tables
)
    table_names = String[]
    out_l = Pair{Type{<:FirmIdentifier}, Type{<:FirmIdentifier}}[]
    out_tables = LinkTable[]
    for (l, table) in zip(list, tables)
        if table.table ∉ table_names
            push!(out_l, l)
            push!(out_tables, table)
            push!(table_names, table.table)
        else
            i = findfirst(table.table .== table_names)
            out_tables[i] = merge(out_tables[i], table)
        end
    end
    (out_l, out_tables)
end

"""
    function link_identifiers(
        conn,
        cur_ids::Vector{T},
        dates::Vector{Date},
        new_types::Type{<:FirmIdentifier}...;
        convert_to_values::Bool=true
    ) where {T<:FirmIdentifier}

Provides links between a firm identifier (on a specific date) and
other firm identifiers provided. Generally, these can then be joined into
a DataFrame.

This relies on the ability to build a tree from the provided type to other types,
that function in turn relies on `LinkTable` existing for pairs of functions.

Returns a DataFrame with a column of the ID provided, date, and a column of each
of the requested identifiers. The identifiers have capitalization that follows the
type (i.e., the GVKey column will be titled "GVKey").

## Example

```julia
df = DataFrame(
    cik=["0001341439", "0000004447", "0000723254"],
    date=[Date(2020), Date(2019), Date(2020)]
)

leftjoin(
    df,
    link_identifiers(
        CIK.(df.cik),
        df.date,
        Permno,
        Ticker
    ),
    on=["cik" => "CIK", "date"]
)
```

"""
function link_identifiers(
    conn,
    cur_ids::Vector{T},
    dates::Vector{Date},
    new_types::Type{<:FirmIdentifier}...;
    convert_to_values::Bool=true,
    validate::Bool=true,
    show_tree::Bool=false,
    colnames::AbstractVector{<:Pair}=Pair{String, String}[]
) where {T<:FirmIdentifier}
    df = DataFrame(
        ids=cur_ids,
        date=dates
    ) |> unique
    rename!(df, "ids" => string(T))
    tree = build_tree(T) # tree starts from provided type and goes to all available types
    show_tree && print_tree(tree)
    list = vcat([
        build_list(K, tree) for K in new_types
    ]...) |> unique # pairs of types from new_types to the provided type
    tables = LinkTable.(list) # get the list of tables, already in order
    list, tables = unique_tables(list, tables) # make the tables unique, prevents downloading unnecessary data
    for (l, table) in zip(list, tables)
        new_table = link_table(conn, table, collect(skipmissing(df[:, string(l[1])])))
        if nrow(new_table) == 0
            continue
        end
        new_table = adjust_date_cols(new_table, table, minimum(df.date), maximum(df.date))
        df = range_join(
            df,
            new_table,
            [string(l[1])],
            [
                Conditions("date", >=, table.date_col_min),
                Conditions("date", <=, table.date_col_max)
            ],
            validate=(validate, false),
            jointype=:left
        )
        select!(df, Not([table.date_col_min, table.date_col_max]))
    end
    select!(df, unique(string.(vcat([string(T), "date"], [x for x in new_types]))))

    if convert_to_values
        for col in names(df)
            if col != "date"
                df[!, col] = value.(df[:, col])
            end
        end
    end
    for col in colnames
        if col[1] ∈ names(df)
            rename!(df, col)
        end
    end
    df

end