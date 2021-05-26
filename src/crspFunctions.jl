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
    columns = ["ret", "vol", "shrout"])

    for col in ["permno", "dateStart", "dateEnd"]
        if col ∉ names(df)
            println("$col must be in the DataFrame")
            return 0
        end
    end

    for col in ["permno", "date"]
        if col ∉ columns
            push!(columns, col)
        end
    end

    colString = createColString(columns)


    
    if 100 < size(df, 1) && length(unique(df[:, :permno])) < 1000
        permnos = join(Int.(unique(df[:, :permno])), ",")
        query = """
                    select $colString
                    from crsp.$stockFile
                    where date between '$(minimum(df[:, :dateStart]))' and '$(maximum(df[:, :dateEnd]))' and permno IN ($permnos)
                """
        crsp = DBInterface.execute(dsn, query) |> DataFrame
    elseif 100 < size(df, 1)
        query = """
            select $colString
            from crsp.$stockFile
            where date between '$(minimum(df[:, :dateStart]))' and '$(maximum(df[:, :dateEnd]))'
        """
        crsp = DBInterface.execute(dsn, query) |> DataFrame
    else
        query = String[]
        for i in 1:size(df, 1)
            temp_query = """
                            (select $colString
                            from crsp.$stockFile
                            where date between '$(df[i, :dateStart])' and '$(df[i, :dateEnd])' and permno = $(df[i, :permno]))
                            """
            push!(query, temp_query)
        end

        crsp = DBInterface.execute(dsn, join(query, " UNION ")) |> DataFrame;
    end
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