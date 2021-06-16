function crspStocknames(dsn;
    cusip::Array{String} = String[],
    ncusip::Array{String} = String[],
    permno::Array{<:Number} = Int[],
    cols::Array{String} = ["permno", "cusip", "ncusip", "comnam", "namedt", "nameenddt", "ticker"])

    if sum(length.([cusip, ncusip, permno]) .> 0) > 1 # Tests if any array has data, sums to test if multiple have data
        println("Only one of the identifying columns can have values in it")
        println("Please retry where only one of cusip, ncusip, and permno")
        return 0
    end

    stocknames = DataFrame()

    colString = ""
    for col in cols
        if colString == ""
            colString = col
        else
            colString = colString * ", " * col
        end
    end

    query = ""
    if sum(length.([cusip, ncusip, permno]) .> 0) == 0 ||  sum(length.([cusip, ncusip, permno]) .> 100) > 0# If no restrictions, get all data
        query = """
                            select distinct $colString
                            from crsp.stocknames
                            where ncusip != ''
                            """
        
    else
        for arg in [cusip, ncusip, permno]
            if length(arg) > 0
                var = ""
                queryL = String[]
                if length(permno) > 0
                    var = "permno"
                    for x in arg
                        temp_query = """
                                        (select $colString
                                        from crsp.stocknames
                                        where ncusip != '' and $var = $x)
                                        """
                        push!(queryL, temp_query)
                    end
                else
                    if length(ncusip) > 0
                        var = "ncusip"
                    else
                        var = "cusip"
                    end
                        for x in arg
                        temp_query = """
                                        (select $colString
                                        from crsp.stocknames
                                        where ncusip != '' and $var = '$x')
                                        """
                        push!(queryL, temp_query)
                    end
                end
                
                
                query = join(queryL, " UNION ")
            end
        end
    end

    stocknames = LibPQ.execute(dsn, query) |> DataFrame;
    if "namedt" in cols
        stocknames[!, :namedt] = Dates.Date.(stocknames[:, :namedt])
    end
    if "nameenddt" in cols
        stocknames[!, :nameenddt] = Dates.Date.(stocknames[:, :nameenddt]);
    end
    return stocknames
end

"""
crspWholeMarket(dsn;
    stockFile = "dsi",
    dateStart = Dates.Date(1925, 1, 1),
    dateEnd = Dates.today(),
    col = "vwretd")

Downloads the data from the daily or monthly stock index file (dsi and msi) for a range of dates with one value
for each day (with various return columns). Available columns are:

- "vwretd": Value weighted return with dividends
- "vwretx": Value weighted return without dividends
- "ewretd": Equal weighted return with dividends
- "ewretx": Equal weighted return without dividends
- "sprtrn": S&P500 return
"""
function crspWholeMarket(dsn;
    stockFile = "dsi",
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

    col_str = createColString(cols)

    query = """
                        select $col_str
                        from crsp.$stockFile
                        where date between '$dateStart' and '$dateEnd'
                        """
    crsp = LibPQ.execute(dsn, query) |> DataFrame;
    crsp[!, :date] = Dates.Date.(crsp[:, :date]);
    return crsp
end

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

function create_permno_str(permnos::AbstractArray{<:Real})
    join(Int.(unique(permnos)), ",")
end
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
    permnos_str = join(Int.(unique(permnos)), ",")
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
    
function crspData(dsn,
    df::DataFrame;
    stockFile = "dsf",
    columns = ["ret", "vol", "shrout"],
    pull_method::Symbol=:optimize, # :optimize, :minimize, :stockonly, :alldata,
    date_start::String="dateStart",
    date_end::String="dateEnd",
    exe=LibPQ.execute
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
        if col ∉ columns
            push!(columns, col)
        end
    end

    colString = createColString(columns)
    
    query = "SELECT $colString FROM crsp.$stockFile WHERE "

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
        #println("Cluster count: ", length(gdf))
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
    crsp = exe(dsn, query) |> DataFrame

    crsp[!, :date] = Dates.Date.(crsp[:, :date]);

    return crsp
end

function crspData(
    dsn,
    s::Date,
    e::Date;
    stockFile = "dsf",
    columns = ["ret", "vol", "shrout"]
)
    for col in ["permno", "date"]
        if col ∉ columns
            push!(columns, col)
        end
    end
    colString = ""
    for col in columns
        if colString == ""
            colString = col
        else
            colString = colString * ", " * col
        end
    end

    query = """
        select $colString
        from crsp.$stockFile
        where date between '$(s)' and '$(e)'
    """
    crsp = LibPQ.execute(dsn, query) |> DataFrame
    crsp[!, :date] = Dates.Date.(crsp[:, :date]);

    return crsp
end