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

    stocknames = DBInterface.execute(dsn, query) |> DataFrame;
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
    crsp = DBInterface.execute(dsn, query) |> DataFrame;
    crsp[!, :date] = Dates.Date.(crsp[:, :date]);
    return crsp
end

function crspData(dsn,
    df::DataFrame;
    stockFile = "dsf",
    columns = ["ret", "vol", "shrout"],
    pull_method::Symbol=:optimize, # :optimize, :minimize, :stockonly, :alldata,
    date_start::String="dateStart",
    date_end::String="dateEnd"
)

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


    
    if (pull_method == :optimize && 100 < size(df, 1) && length(unique(df[:, :permno])) < 1000) || pull_method == :stockonly
        permnos = join(Int.(unique(df[:, :permno])), ",")
        query = """
                    select $colString
                    from crsp.$stockFile
                    where date between '$(minimum(df[:, date_start]))' and '$(maximum(df[:, date_end]))' and permno IN ($permnos)
                """
        
    elseif (pull_method == :optimize && size(df, 1) > 100) || pull_method == :alldata
        query = """
            select $colString
            from crsp.$stockFile
            where date between '$(minimum(df[:, date_start]))' and '$(maximum(df[:, date_end]))'
        """
    else
        query = "SELECT $colString from crsp.$stockFile WHERE "
        for i in 1:size(df, 1)
            if i != 1
                query *= " OR "
            end

            query *= "(WHERE date BETWEEN '$(df[i, date_start])' AND '$(df[i, date_end])' AND permno = $(df[i, :permno]))"
        end
    end
    crsp = DBInterface.execute(dsn, query) |> DataFrame

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
    crsp = DBInterface.execute(dsn, query) |> DataFrame
    crsp[!, :date] = Dates.Date.(crsp[:, :date]);

    return crsp
end