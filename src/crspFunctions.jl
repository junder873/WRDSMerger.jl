
"""
    function crsp_stocknames(
        conn;
        cols::Array{String}=["permno", "cusip", "ncusip", "comnam", "namedt", "nameenddt", "ticker"]
    )

    function crsp_stocknames(
        conn,
        cusip::Array{String};
        cols::Array{String}=["permno", "cusip", "ncusip", "comnam", "namedt", "nameenddt", "ticker"],
        cusip_col="cusip", # either "cusip" "ncusip" or "ticker"
    )

    function crsp_stocknames(
        conn,
        permno::Array{<:Number};
        cols::Array{String}=["permno", "cusip", "ncusip", "comnam", "namedt", "nameenddt", "ticker"],
    )

Download crsp.stockname data (with non-missing ncusip).
If an array of strings is passed, by default assumes it is a list of cusips, can be a list of
ncusip or tickers (change `cusip_col`). Can also take an array of numbers which is assumed to be
a list of permnos.

"""
function crsp_stocknames(
    conn;
    cols::Array{String}=["permno", "cusip", "ncusip", "comnam", "namedt", "nameenddt", "ticker"],
    filters::Dict{String, <:Any}=Dict(
        "ncusip" => missing
    )
)
    col_str = join(cols, ", ")
    fil = create_filter(filters)

    query = """
        SELECT DISTINCT $col_str
        FROM $(default_tables["crsp_stocknames"])
        $fil
    """
    return run_sql_query(conn, query) |> DataFrame
end

function crsp_stocknames(
    conn,
    cusip::Array{String};
    cols::Array{String}=["permno", "cusip", "ncusip", "comnam", "namedt", "nameenddt", "ticker"],
    cusip_col="cusip", # either "cusip" "ncusip" or "ticker"
    filters::Dict{String, <:Any}=Dict(
        "ncusip" => missing
    )
)
    if cusip_col ∉ ["cusip", "ncusip", "ticker"]
        @error("`cusip_col` must be one of \"cusip\", \"ncusip\" or \"ticker\"")
    end
    if 0 < length(cusip) <= 1000
        temp_filters = Dict{String, Any}()
        for (key, val) in filters
            temp_filters[key] = val
        end
        filters = temp_filters
        filters[cusip_col] = cusip
    end
    return crsp_stocknames(
        conn;
        cols,
        filters
    )
end

function crsp_stocknames(
    conn,
    permno::Array{<:Number};
    cols::Array{String}=["permno", "cusip", "ncusip", "comnam", "namedt", "nameenddt", "ticker"],
    filters::Dict{String, <:Any}=Dict(
        "ncusip" => missing
    )
)
    if 0 < length(permno) <= 1000
        temp_filters = Dict{String, Any}()
        for (key, val) in filters
            temp_filters[key] = val
        end
        filters = temp_filters
        filters["permno"] = permno
    end
    return crsp_stocknames(
        conn;
        cols,
        filters
    )
end



"""
    function crsp_market(
        conn,
        dateStart::Union{Date,Int}=1950,
        dateEnd::Union{Date,Int}=Dates.today();
        col::Union{String, Array{String}}="vwretd"
    )

Downloads the data from the daily or monthly stock index file (dsi and msi) for a range of dates with one value
for each day (with various return columns). Available columns are:

- "vwretd": Value weighted return with dividends
- "vwretx": Value weighted return without dividends
- "ewretd": Equal weighted return with dividends
- "ewretx": Equal weighted return without dividends
- "sprtrn": S&P500 return
"""
function crsp_market(
    conn,
    dateStart::Union{Date,Int}=1950,
    dateEnd::Union{Date,Int}=Dates.today();
    cols::Union{String, Array{String}}="vwretd"
)

    if typeof(dateStart) == Int
        dateStart = Dates.Date(dateStart, 1, 1)
    end
    if typeof(dateEnd) == Int
        dateEnd = Dates.Date(dateEnd, 12, 31)
    end

    cols = if typeof(cols) <: String
        [cols, "date"]
    elseif "date" ∉ cols
        vcat(cols, ["date"])
    else
        cols
    end

    col_str = join(cols |> unique, ", ")

    query = """
                        select $col_str
                        from $(default_tables["crsp_index"])
                        where date between '$dateStart' and '$dateEnd'
                        """
    return run_sql_query(conn, query) |> DataFrame
end

# This set of functions is for compiling a series of SQL statements
# or for estimating the amount of time it would take to download a table
# of a given size.


date_filter(date_range::StepRange) = "date BETWEEN '$(date_range[1])' AND '$(date_range[end])'"
date_filter(date_range::AbstractArray{<:StepRange}) = "((" * join(date_filter.(date_range), ") OR (") * "))"
date_filter(dates::AbstractArray{Date}) = "date IN ('$(join(dates, "','"))')"

function sql_query_basic(
    permno::Int,
    dates;
    cols::Vector{String}=["permno", "date", "ret"]
)
    "SELECT $(join(cols, ",")) FROM $(default_tables["crsp_stock_data"]) WHERE permno = $permno AND $(date_filter(dates))"
end

function sql_query_full(
    permnos,
    dates;
    cols=["permno", "date", "ret"]
)
    #"(" * 
    join(sql_query_basic.(permnos, dates; cols), " UNION ")
    # * ")"
end

function merge_date_ranges(x::AbstractArray{<:StepRange})
    x = sort(x)
    out = eltype(x)[]
    s = x[1][1]
    e = x[1][end]
    for i in 2:length(x)
        if e >= x[i][1]
            e = x[i][end]
        else
            push!(out, s:Day(1):e)
            s = x[i][1]
            e = x[i][end]
        end
    end
    push!(out, s:Day(1):e)
    out
end


"""
    function crsp_data(
        conn,
        [permnos::Vector{<:Real},]
        s::Date=Date(1925),
        e::Date=today();
        cols = ["ret", "vol", "shrout"],
        filters::Dict{String, <:Any}=Dict{String, Any}(),
        adjust_crsp_data::Bool=true
    )

    function crsp_data(
        conn,
        permnos::Vector{<:Real},
        dates::Vector{Date},
        [dates_end::Vector{Date}];
        cols=["ret", "vol", "shrout"],
        adjust_crsp_data::Bool=true,
        query_size_limit::Int=3000
    )


Downloads data from the crsp stockfiles, which are individual stocks. To download the data from
the monthly stockfile, change the default table to the monthly stockfile:
```julia
WRDSMerger.default_tables["crsp_stock_data"] = "crsp.msf"
WRDSMerger.default_tables["crsp_index"] = "crsp.msi"
WRDSMerger.default_tables["crsp_delist"] = "crsp.msedelist"
```

# Arguments

- `permnos::Vecotr{<:Real}`: A vector of firm IDs, if provided, will only download data for those firms
- `s::Date=Date(1925)` and `e::Date=today()`: Downloads all data between two dates provided
- `dates::Vector{Date}`: Downloads data for a set of permnos on the date provided
    - `dates_end::Vector{Date}`: If provided, then treats the `dates` as the start of a period
      and will download data for the permnos between the two dates
- `adjust_crsp_data::Bool=true`: This will call `crsp_adjust` with all options
    set to true, it will only do the operations that it has the data for.
"""
function crsp_data(
    conn,
    permnos::Vector{<:Real},
    date_start::Date=Date(1926),
    date_end::Date=today();
    cols = ["ret", "vol", "shrout"],
    adjust_crsp_data::Bool=true,
    filters::Dict{String, <:Any} = Dict{String, Any}()
)
    @assert all(isinteger.(permnos)) "All of the Permnos must be convertable to an Integer"

    filters["permno"] = Int.(permnos)
    return crsp_data(
        conn,
        date_start,
        date_end;
        cols,
        filters,
        adjust_crsp_data
    )
end

function crsp_data(
    conn,
    s::Date=Date(1925),
    e::Date=today();
    cols = ["ret", "vol", "shrout"],
    filters::Dict{String, <:Any}=Dict{String, Any}(),
    adjust_crsp_data::Bool=true
)
    for col in ["permno", "date"]
        if col ∉ cols
            push!(cols, col)
        end
    end
    colString = join(cols, ", ")

    filter_str = create_filter(filters, "WHERE date BETWEEN '$s' AND '$e'")

    query = """
        select $colString
        from $(default_tables["crsp_stock_data"])
        $filter_str
    """
    crsp = run_sql_query(conn, query) |> DataFrame
    if adjust_crsp_data
        crsp = crsp_adjust(conn, crsp)
    end
    return crsp
end


function crsp_data(
    conn,
    permnos::Vector{<:Real},
    dates::Vector{Date};
    cols=["ret", "vol", "shrout"],
    adjust_crsp_data::Bool=true,
    query_size_limit::Int=3000
)

    @assert all(isinteger.(permnos))  "All of the Permnos must be convertable to an Integer"
    @assert length(permnos) == length(dates) "Must have same number of dates as Permnos"

    for col in ["permno", "date"]
        if col ∉ cols
            push!(cols, col)
        end
    end

    permnos = Int.(permnos)

    df = DataFrame(
        permno=permnos,
        date=dates
    ) |> unique

    if length(unique(dates)) < nrow(df) / 5
        temp_market = crsp_market(dsn, minimum(dates), maximum(dates); cols=["dates"])
        df = innerjoin(
            df,
            temp_market,
            on=:date,
            validate=(false, true)
        )
    end

    df = combine(
        groupby(df, :permno),
        :date => (x -> [Vector(x)])
    )
    rename!(df, [:permno, :date])
    
    queries = sql_query_full.(
        collect(Iterators.partition(df.permno, query_size_limit)),
        collect(Iterators.partition(df.date, query_size_limit));
        cols
    )
    crsp = DataFrame()
    for q in queries
        temp = run_sql_query(conn, q)
        if nrow(crsp) == 0
            crsp = temp[:, :]
        else
            crsp = vcat(crsp, temp)
        end
    end

    if adjust_crsp_data
        crsp = crsp_adjust(conn, crsp)
    end
    return crsp
end

function crsp_data(
    conn,
    permnos::Vector{<:Real},
    dates_min::Vector{Date},
    dates_max::Vector{Date};
    cols=["ret", "vol", "shrout"],
    adjust_crsp_data::Bool=true,
    query_size_limit::Int=3000
)
    @assert all(isinteger.(permnos))  "All of the Permnos must be convertable to an Integer"
    @assert length(permnos) == length(dates_min) == length(dates_max) "Must have same number of dates as Permnos"


    for col in ["permno", "date"]
        if col ∉ cols
            push!(cols, col)
        end
    end

    permnos = Int.(permnos)

    df = DataFrame(
        permno=permnos
    )
    df[!, :date_range] = [d1:Day(1):d2 for (d1, d2) in zip(dates_min, dates_max)]
    df = combine(
        groupby(df, :permno),
        "date_range" => x -> [merge_date_ranges(x)]
    )
    rename!(df, [:permno, :date_range])

    queries = sql_query_full.(
        collect(Iterators.partition(df.permno, query_size_limit)),
        collect(Iterators.partition(df.date_range, query_size_limit));
        cols
    )
    crsp = DataFrame()
    for q in queries
        temp = run_sql_query(conn, q)
        if nrow(crsp) == 0
            crsp = temp[:, :]
        else
            crsp = vcat(crsp, temp)
        end
    end

    if adjust_crsp_data
        crsp = crsp_adjust(conn, crsp)
    end
    return crsp
end

function crsp_data(
    conn,
    df::DataFrame;
    cols = ["ret", "vol", "shrout"],
    pull_method::Symbol=:optimize, # :optimize, :minimize, :stockonly, :alldata,
    date_start::String="dateStart",
    date_end::String="dateEnd",
    adjust_crsp_data::Bool=true
)
    df = df[:, :]
    for col in ["permno", date_start, date_end]
        if col ∉ names(df)
            @error("$col must be in the DataFrame")
        end
    end
    pull_methods = [:optimize, :minimize, :stockonly, :alldata]
    if pull_method ∉ pull_methods
        @error("pull_method must be one of $pull_methods")
    end

    for col in ["permno", "date"]
        if col ∉ cols
            push!(cols, col)
        end
    end

    if pull_method == :optimize || pull_method == :minimize
        return crsp_data(
            conn,
            df.permno,
            df[:, date_start],
            df[:, date_end];
            cols,
            adjust_crsp_data
        )
    elseif pull_method == :stockonly
        return crsp_data(
            conn,
            df.permno |> unique,
            minimum(df[:, date_start]),
            maximum(df[:, date_end]);
            cols,
            adjust_crsp_data
        )
    else
        return crsp_data(
            conn,
            minimum(df[:, date_start]),
            maximum(df[:, date_end]);
            cols,
            adjust_crsp_data
        )
    end
end




"""
    function crsp_delist(
        conn;
        cols::Array{String}=[
            "permno",
            "dlstdt",
            "dlret"
        ],
        date_start::Date=Date(1926),
        date_end::Date=today()
    )

Fetches the CRSP delist dataset, typically for the returns
on the day of delisting.
"""
function crsp_delist(
    conn,
    date_start::Date=Date(1926),
    date_end::Date=today();
    cols::Array{String}=[
        "permno",
        "dlstdt",
        "dlret"
    ],
    filters::Dict{String, <:Any}=Dict(
        "dlret" => missing
    )
)
    fil = create_filter(filters, "WHERE dlstdt BETWEEN '$date_start' AND '$date_end' AND dlret != 0")
    col_str = join(cols, ", ")
    query = """
    SELECT $col_str FROM $(default_tables["crsp_delist"]) $fil
    """
    return run_sql_query(conn, query) |> DataFrame
end

"""
    function crsp_adjust(
        conn,
        df::DataFrame;
        kwargs...
    )

This makes 4 common adjustments to CRSP data:
1. When the price in CRSP is negative, that means there was not a
   specific close price. Typically, researchers take the absolute value
   to get the actual price.
2. Prices are not adjusted for splits (returns are). CRSP includes
   the number (cfascpr) that will adjust prices to be comparable through time
3. Similar to prices, shares outstanding are not adjusted. The number used to
   adjust is different due to various events.
4. Prices are not adjusted for delisting, so this downloads the
   necessary dataset and adjusts returns accordingly.

# Arguments

## Options on what to adjust

- adjust_prc_negatives::Bool=true: Corresponds to (1) above
- adjust_prc_splits::Bool=true: Corresponds to (2) above
- adjust_shr_splits::Bool=true: Corresponds to (3) above
- adjust_delist::Bool=true: Corresponds to (4) above

## Pre-existing column names

- date::String="date": date column
- idcol::String="permno": primary identifier (permno) column
- prc_col::String="prc": price column
- ret_col::String="ret": return column
- prc_splits_col::String="cfacpr": price split adjustment factor
- shrout_col::String="shrout": shares outstanding
- shrout_splits_col::String="cfacshr": share split adjustment factor

## Options to relabel columns

- adjusted_neg_prc_col::String="prc": relabel prices to this after taking absolute value
- adjusted_prc_col::String="prc_adj": relabel prices to this after adjusting for splits
- adjusted_shrout_col::String="shrout_adj":relabel shares outstanding to this after adjusting for splits

"""
function crsp_adjust(
    conn,
    df::DataFrame;
    adjust_prc_negatives::Bool=true,
    adjust_prc_splits::Bool=true,
    adjust_shr_splits::Bool=true,
    adjust_delist::Bool=true,
    date::String="date",
    idcol::String="permno",
    prc_col::String="prc",
    adjusted_neg_prc_col::String="prc",
    prc_splits_col::String="cfacpr",
    adjusted_prc_col::String="prc_adj",
    shrout_col::String="shrout",
    shrout_splits_col::String="cfacshr",
    adjusted_shrout_col::String="shrout_adj",
    ret_col::String="ret"
)
    df = copy(df)
    if adjust_prc_negatives && prc_col ∈ names(df)
        df[!, adjusted_neg_prc_col] = abs.(df[!, prc_col])
    end

    if adjust_prc_splits && prc_col ∈ names(df) && prc_splits_col ∈ names(df)
        df[!, adjusted_prc_col] = abs.(df[:, prc_col]) ./ df[:, prc_splits_col]
    end

    if adjust_shr_splits && shrout_col ∈ names(df) && shrout_splits_col ∈ names(df)
        df[!, adjusted_shrout_col] = df[:, shrout_col] .* df[:, shrout_splits_col]
    end

    if adjust_delist && ret_col ∈ names(df)
        delist_ret = crsp_delist(
            conn,
            minimum(df[:, date]),
            maximum(df[:, date])
        ) |> dropmissing |> unique
        df = leftjoin(
            df,
            delist_ret,
            on=[idcol => "permno", date => "dlstdt"],
            validate=(true, true)
        )
        df[!, :dlret] = coalesce.(df.dlret, 0.0)
        df[!, ret_col] = (1 .+ df[:, ret_col]) .* (1 .+ df.dlret) .- 1
        select!(df, Not(:dlret))
    end
    return df
end