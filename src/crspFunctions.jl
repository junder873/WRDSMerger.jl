
"""
    function crsp_stocknames(
        dsn::Union{LibPQ.Connection, DBInterface.Connection};
        cols::Array{String}=["permno", "cusip", "ncusip", "comnam", "namedt", "nameenddt", "ticker"]
    )

Download all crsp.stockname data (with non-missing ncusip), expects a case of a LibPQ.Connection

"""
function crsp_stocknames(
    dsn::Union{LibPQ.Connection, DBInterface.Connection};
    cols::Array{String}=["permno", "cusip", "ncusip", "comnam", "namedt", "nameenddt", "ticker"]
)
    col_str = join(cols, ", ")

    query = """
        SELECT DISTINCT $col_str
        FROM $(default_tables.crsp_stocknames)
        WHERE ncusip != ''
    """
    return run_sql_query(dsn, query) |> DataFrame
end

"""
    function crsp_stocknames(
        dsn::Union{LibPQ.Connection, DBInterface.Connection},
        cusip::Array{String};
        cols::Array{String}=["permno", "cusip", "ncusip", "comnam", "namedt", "nameenddt", "ticker"],
        cusip_col="cusip", # either "cusip" or "ncusip"
        warn_on_long=true
    )

Download crsp.stockname data (with non-missing ncusip) that matches a cusip or ncusip, expects a case of a LibPQ.Connection
`cusip` must be an array of strings that match either a cusip or ncusip, if using ncusip, then add then set
`cusip_cols="ncusip"` to filter on the correct column.

"""
function crsp_stocknames(
    dsn::Union{LibPQ.Connection, DBInterface.Connection},
    cusip::Array{String};
    cols::Array{String}=["permno", "cusip", "ncusip", "comnam", "namedt", "nameenddt", "ticker"],
    cusip_col="cusip", # either "cusip" "ncusip" or "ticker"
    warn_on_long=true
)
    if cusip_col ∉ ["cusip", "ncusip", "ticker"]
        @error("`cusip_col` must be one of \"cusip\", \"ncusip\" or \"ticker\"")
    end
    if length(cusip) == 0 || length(cusip) > 1000
        crsp_stocknames(dsn; cols)
    end
    col_str = join(cols, ", ")
    fil_str = join(cusip, "', '")

    query = """
        SELECT DISTINCT $col_str
        FROM $(default_tables.crsp_stocknames)
        WHERE ncusip != ''
        AND $cusip_col IN ('$(fil_str)')
    """
    return run_sql_query(dsn, query) |> DataFrame
end

"""
    function crsp_stocknames(
        dsn::Union{LibPQ.Connection, DBInterface.Connection},
        permno::Array{<:Number};
        cols::Array{String}=["permno", "cusip", "ncusip", "comnam", "namedt", "nameenddt", "ticker"],
        warn_on_long=true
    )

Download crsp.stockname data (with non-missing ncusip) that matches a permno, expects a case of a LibPQ.Connection
`permno` must be an array of numbers.

"""
function crsp_stocknames(
    dsn::Union{LibPQ.Connection, DBInterface.Connection},
    permno::Array{<:Number};
    cols::Array{String}=["permno", "cusip", "ncusip", "comnam", "namedt", "nameenddt", "ticker"],
)
    if length(permno) == 0 || length(permno) > 1000
        crsp_stocknames(dsn; cols)
    end
    col_str = join(cols, ", ")
    fil_str = join(permno, ", ")

    query = """
        SELECT DISTINCT $col_str
        FROM $(default_tables.crsp_stocknames)
        WHERE ncusip != ''
        AND permno IN ($(fil_str))
    """
    return run_sql_query(dsn, query) |> DataFrame
end



"""
    function crsp_market(
        dsn::Union{LibPQ.Connection, DBInterface.Connection};
        dateStart = Dates.Date(1925, 1, 1),
        dateEnd = Dates.today(),
        col = "vwretd"
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
    dsn::Union{LibPQ.Connection, DBInterface.Connection};
    dateStart = Dates.Date(1925, 1, 1),
    dateEnd = Dates.today(),
    col = "vwretd"
    )

    cols = if typeof(col) <: String
        [col, "date"]
    elseif "date" ∉ col
        vcat(col, ["date"])
    else
        col
    end

    col_str = join(cols, ", ")

    query = """
                        select $col_str
                        from $(default_tables.crsp_index)
                        where date between '$dateStart' and '$dateEnd'
                        """
    crsp = run_sql_query(dsn, query) |> DataFrame;
    return crsp
end

# This set of functions is for compiling a series of SQL statements
# or for estimating the amount of time it would take to download a table
# of a given size.

function main_and_statement(permno, date_start, date_end)
    "(date BETWEEN '$(date_start)' AND '$(date_end)' AND permno = $(permno))"
end

function partial_sql_statement(
        permno::Real,
        date_start::Date,
        date_end::Date;
        identifier::String="permno",
        date_col::String="date"
    )
    permno = Int(permno)
    "($date_col BETWEEN '$(date_start)' AND '$(date_end)' AND $identifier = $(permno))"
end

function partial_sql_statement(
        permno::String,
        date_start::Date,
        date_end::Date;
        identifier::String="permno",
        date_col::String="date"
    )
    "($date_col BETWEEN '$(date_start)' AND '$(date_end)' AND $identifier IN ($(permno)))"
end

create_permno_str(permnos::AbstractArray{<:Real}) = join(Int.(unique(permnos)), ",")

function partial_sql_statement(
        permnos::AbstractArray{<:Real},
        date_starts::AbstractArray{Date},
        date_ends::AbstractArray{Date};
        identifier::String="permno",
        date_col::String="date"
    )
    if length(permnos) == 1
        return partial_sql_statement(permnos[1], date_starts[1], date_ends[1]; identifier, date_col)
    end
    permno_str = create_permno_str(permnos)
    min_date = minimum(date_starts)
    max_date = maximum(date_ends)
    partial_sql_statement(permno_str, min_date, max_date; identifier, date_col)
end
function partial_sql_statement(
        df::AbstractDataFrame;
        date_start::String="dateStart",
        date_end::String="dateEnd",
        identifier::String="permno",
        date_col::String="date"
    )
    if nrow(df) == 1
        return partial_sql_statement(df[1, :]; date_start, date_end, identifier, date_col)
    end
    permnos = join(Int.(unique(df[:, identifier])), ",")
    min_date = minimum(df[:, date_start])
    max_date = maximum(df[:, date_end])
    partial_sql_statement(permnos, min_date, max_date; identifier, date_col)
end

function partial_sql_statement(
        df::DataFrameRow;
        date_start::String="dateStart",
        date_end::String="dateEnd",
        identifier::String="permno",
        date_col::String="date"
    )
    "($date_col BETWEEN '$(df[date_start])' AND '$(df[date_end])' AND $identifier = $(df[identifier]))"
end

function file_size_estimate(
        df;
        date_start::String="dateStart",
        date_end::String="dateEnd",
        cluster_col::String="cluster",
        firm_col::String="permno"
    )
    temp = combine(
        groupby(df, cluster_col),
        date_start => minimum => "min_date",
        date_end => maximum => "max_date",
        firm_col => length ∘ unique => "firm_count"
        )
    temp[!, :total_days] = bdayscount.(:USNYSE, temp.min_date, temp.max_date) .+ isbday.(:USNYSE, temp.max_date)
    return sum(temp.firm_count .* temp.total_days)
end
function data_time_estimate(
        obs;
        intercept=1000,
        slope=0.0200079
    )
    intercept + slope * obs
end
function cluster_time_estimate(
        clusters;
        intercept=32.3731,
        slope=0.0236368,
    )
    (intercept + clusters * slope) ^ 2
end
function total_time_estimate(
        df;
        date_start::String="dateStart",
        date_end::String="dateEnd",
        cluster_col::String="cluster",
        firm_col::String="permno"
    )
    row_count = file_size_estimate(df; date_start, date_end, cluster_col, firm_col)
    data_time = data_time_estimate(row_count)
    cluster_time = cluster_time_estimate(length(unique(df[:, cluster_col])))
    return data_time + cluster_time
end


"""
    function crsp_data(
        dsn::Union{LibPQ.Connection, DBInterface.Connection},
        df::DataFrame;
        cols = ["ret", "vol", "shrout"],
        pull_method::Symbol=:optimize, # :optimize, :minimize, :stockonly, :alldata,
        date_start::String="dateStart",
        date_end::String="dateEnd",
    )

Downloads data from the crsp stockfiles, which are individual stocks.

# Arguments
- `pull_method::Symbol=:optimize`: designates the method the function will pull the data,
        to help minimize the amount of data needed. The available methods are:
    - `:optimize`: uses a clustering algorithm (kmeans) to find common or nearby dates
        which then is used to pull data for that group of stocks around that date. Since
        finding the appropriate number of clusters takes time, it does so in steps of 500
        and estimates the time to download that much data and for WRDS to process the request
        (more data = longer time, more chunks = longer time)
    - `:minimize`: pulls data for each stock individually, resulting in only necessary
        data being downloaded. However, due to WRDS taking exponentially more time for
        each `OR` statement, this can get very slow with large datasets, ideal for
        small datasets with disperse data
    - `:stockonly`: retrieves all data between the minimum and maximum date in the dataset
        for all firms that are listed, ideal if the minimim and maximum date needed are close
        to each other
    - `:alldata`: retrieves all data between the minimum and maximum date
"""
function crsp_data(
    dsn::Union{LibPQ.Connection, DBInterface.Connection},
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

    colString = join(cols, ", ")
    
    query = "SELECT $colString FROM $(default_tables.crsp_stock_data) WHERE "

    if pull_method == :optimize
        df[!, :date_val] = Float64.(Dates.value.(df[:, date_start]))
        cluster_max = length(unique(df[:, date_start]))
        t_ests = Float64[]
        for clusters in 1:500:cluster_max+500
            c = min(clusters, cluster_max)
            t1 = now()
            x = ParallelKMeans.kmeans(Hamerly(), Matrix(df[:, [:date_val]])', c)
            if clusters > 1
                old_clusters = df.cluster
            end
            df[!, :cluster] = x.assignments
            t2 = now()
            t_est = total_time_estimate(df; date_start, date_end, cluster_col="cluster", firm_col="permno")
            if length(t_ests) > 0 && t_ests[end] - t_est < Dates.value(t2 - t1)
                if t_ests[end] < t_est
                    df[!, :cluster] = old_clusters
                end
                break
            else
                push!(t_ests, t_est)
            end
        end
            
        gdf = groupby(df, :cluster)

        temp = combine(
            gdf,
            :permno => create_permno_str => :permno_str,
            date_start => minimum => :date_min,
            date_end => maximum => :date_max
        )

        temp[!, :s] = partial_sql_statement.(temp.permno_str, temp.date_min, temp.date_max)
        query *= join(temp.s, " OR ")
    
    elseif pull_method == :stockonly
        permnos = join(Int.(unique(df[:, :permno])), ",")
        query *= "date between '$(minimum(df[:, date_start]))' and '$(maximum(df[:, date_end]))' and permno IN ($permnos)"
        
    elseif pull_method == :alldata
        query *= "date between '$(minimum(df[:, date_start]))' and '$(maximum(df[:, date_end]))'"
    else
        query *= join(main_and_statement.(df.permno, df[:, date_start], df[:, date_end]), " OR ")
    end

    crsp = run_sql_query(dsn, query) |> DataFrame
    if adjust_crsp_data
        crsp = crsp_adjust(dsn, crsp)
    end
    return crsp
end

"""
    function crsp_data(
        dsn::Union{LibPQ.Connection, DBInterface.Connection},
        s::Date,
        e::Date;
        cols = ["ret", "vol", "shrout"],
        adjust_crsp_data::Bool=true
    )

Downloads all crsp stock data between the start and end date.

## Arguments
- `adjust_crsp_data::Bool=true`: This will call `crsp_adjust` with all options
  set to true, it will only do the operations that it has the data for.
"""
function crsp_data(
    dsn::Union{LibPQ.Connection, DBInterface.Connection},
    s::Date,
    e::Date;
    cols = ["ret", "vol", "shrout"],
    adjust_crsp_data::Bool=true
)
    for col in ["permno", "date"]
        if col ∉ cols
            push!(cols, col)
        end
    end
    colString = join(cols, ", ")


    query = """
        select $colString
        from $(default_tables.crsp_stock_data)
        where date between '$(s)' and '$(e)'
    """
    crsp = run_sql_query(dsn, query) |> DataFrame
    if adjust_crsp_data
        crsp = crsp_adjust(dsn, crsp)
    end
    return crsp
end


"""
    function crsp_delist(
        dsn::Union{LibPQ.Connection, DBInterface.Connection};
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
    dsn::Union{LibPQ.Connection, DBInterface.Connection};
    cols::Array{String}=[
        "permno",
        "dlstdt",
        "dlret"
    ],
    date_start::Date=Date(1926),
    date_end::Date=today()
)
    col_str = join(cols, ", ")
    query = """
    SELECT $col_str FROM $(default_tables.crsp_delist)
    WHERE dlret IS NOT NULL
    AND dlret != 0
    AND dlstdt BETWEEN '$date_start' AND '$date_end'
    """
    return run_sql_query(dsn, query) |> DataFrame
end

"""
    function crsp_adjust(
        dsn,
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
"""
function crsp_adjust(
    dsn,
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
            dsn;
            date_start=minimum(df[:, date]),
            date_end=maximum(df[:, date])
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