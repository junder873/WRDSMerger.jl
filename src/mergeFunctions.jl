function compustatCrspLink(dsn;
    gvkey::Array{String} = String[],
    lpermno::Array{<:Number} = Int[],
    cols::Array{String} = ["gvkey", "lpermno", "linkdt", "linkenddt"])

    if sum(length.([gvkey, lpermno]) .> 0) > 1 # Tests if any array has data, sums to test if multiple have data
        println("Only one of the identifying columns can have values in it")
        println("Please retry where either gvkey or lpermno have data")
        return 0
    end

    dfLink = DataFrame()

    colString = ""
    for col in cols
        if colString == ""
            colString = col
        else
            colString = colString * ", " * col
        end
    end

    query = ""
    if sum(length.([gvkey, lpermno]) .> 0) == 0 # If no restrictions, get all data
        query = """
                            select distinct $colString
                            from crsp_a_ccm.ccmxpf_lnkhist
                            where lpermno IS NOT NULL
                            """
    elseif length(gvkey) > 100 || length(lpermno) > 100 # If a lot of data, get all of it
        query = """
                select distinct $colString
                from crsp_a_ccm.ccmxpf_lnkhist
                where lpermno IS NOT NULL
                """
        
    else
        if length(gvkey) > 0
            queryL = String[]
            for x in gvkey
                temp_query = """
                                (select $colString
                                from crsp_a_ccm.ccmxpf_lnkhist
                                where lpermno IS NOT NULL and gvkey = '$x')
                                """
                push!(queryL, temp_query)
            end
            query = join(queryL, " UNION ")
        else
            queryL = String[]
            for x in lpermno
                temp_query = """
                                (select $colString
                                from crsp_a_ccm.ccmxpf_lnkhist
                                where lpermno IS NOT NULL and lpermno = $x)
                                """
                push!(queryL, temp_query)
            end
            query = join(queryL, " UNION ")
        end
    end

    dfLink = ODBC.query(dsn, query);
    if "linkdt" in cols
        dfLink[!, :linkdt] = Dates.Date.(dfLink[:, :linkdt])
    end
    if "linkenddt" in cols
        dfLink[!, :linkenddt] = coalesce.(dfLink[:, :linkenddt], Dates.today())
        dfLink[!, :linkenddt] = Dates.Date.(dfLink[:, :linkenddt]);
    end
    if "lpermno" in cols
        rename!(dfLink, :lpermno => :permno)
    end
    return dfLink
end

function addIdentifiers(dsn,
    df::DataFrame;
    ncusip::Bool = false,
    cusip::Bool = false,
    gvkey::Bool = false,
    permno::Bool = false
)

    if :date ∉ names(df)
        println("DataFrame must include a date column")
        return 0
    end
    if sum([x in names(df) for x in [:ncusip, :cusip, :gvkey, :permno]]) == 0
        println("DataFrame must include identifying column: cusip, ncusip, gvkey, or permno")
        return 0
    end
    if sum([x in names(df) for x in [:ncusip, :cusip, :gvkey, :permno]]) > 1
        println("Function has a preset order on which key will be used first, it is optimal to start with one key")
    end

    if :ncusip in names(df) && !ncusip
        ncusip = true
    end
    if :cusip in names(df) && !cusip
        cusip = true
    end
    if :gvkey in names(df) && !gvkey
        gvkey = true
    end
    if :permno in names(df) && !permno
        permno = true
    end

    if :gvkey in names(df) # If gvkey exists and no other identifier does, permno must be fetched
        if :permno ∉ names(df) && :cusip ∉ names(df) && :ncusip ∉ names(df)
            comp = unique(compustatCrspLink(dsn, gvkey=df[:, :gvkey]))
            df = join(df, comp, on=:gvkey, kind=:left)
            df[!, :linkdt] = coalesce.(df[:, :linkdt], Dates.today())
            df[!, :linkenddt] = coalesce.(df[:, :linkenddt], Dates.today())
            df = df[df[:, :linkdt] .<= df[:, :date] .<= df[:, :linkenddt], :]
            select!(df, Not([:linkdt, :linkenddt]))
            df[!, :permno] = coalesce.(df[:, :permno])
        end
    end

    if :ncusip in names(df) || :cusip in names(df)
        if :permno ∉ names(df) # to fetch either cusip, ncusip, or gvkey, permno is either necessary or trivial
            if :cusip in names(df)
                df = getCrspNames(dsn, df, :cusip, [:ncusip])
            else
                df = getCrspNames(dsn, df, :ncusip, [:cusip])
            end
            for col in [:permno, :cusip, :ncusip]
                if col in names(df)
                    df[!, col] = coalesce.(df[:, col])
                end
            end
        end
    end
                
    if :permno in names(df)
        if (ncusip && :ncusip ∉ names(df)) || (cusip && :cusip ∉ names(df))
            df = getCrspNames(dsn, df, :permno, [:ncusip, :cusip])
        end
        if gvkey && :gvkey ∉ names(df)
            comp = unique(compustatCrspLink(dsn, lpermno=df[:, :permno]))
            df = join(df, comp, on=:permno, kind=:left)
            df = df[.&(df[:, :date] .>= df[:, :linkdt], df[:, :date] .<= df[:, :linkenddt]), :]
            select!(df, Not([:linkdt, :linkenddt]))
            
        end
    end
    for pair in [(ncusip, :ncusip), (cusip, :cusip), (gvkey, :gvkey), (permno, :permno)]
        if !pair[1] && pair[2] in names(df)
            select!(df, Not(pair[2]))
        end
    end
    for col in [:permno, :cusip, :ncusip, :gvkey]
        if col in names(df)
            df[!, col] = coalesce.(df[:, col])
        end
    end
    return df
end

function ibesCrspLink(dsn)
    query = """
        SELECT * FROM crsp.stocknames
    """
    dfStocknames = ODBC.query(dsn, query);
    query = """
        SELECT * FROM ibes.idsum WHERE usfirm=1
    """
    dfIbesNames = ODBC.query(dsn, query);
    dfIbesNames[!, :sdates] = Dates.Date.(dfIbesNames.sdates)
    for col in [:namedt, :nameenddt, :st_date, :end_date]
        dfStocknames[!, col] = Dates.Date.(dfStocknames[:, col])
    end

    dfIbesNamesTemp = unique(dfIbesNames[:, [:ticker, :cusip, :cname, :sdates]])
    dfStocknamesTemp = unique(dfStocknames[:, [:permno, :ncusip, :comnam, :namedt, :nameenddt]])
    dropmissing!(dfStocknamesTemp, :ncusip)

    dfTemp = aggregate(dfIbesNames[:, [:ticker, :cusip, :sdates]], [:ticker, :cusip], [minimum, maximum])
    dfIbesNamesTemp = join(dfIbesNamesTemp, dfTemp, on=[:ticker, :cusip], kind=:left)
    dfIbesNamesTemp = dfIbesNamesTemp[dfIbesNamesTemp.sdates .== dfIbesNamesTemp.sdates_maximum, :]
    dropmissing!(dfIbesNamesTemp, :cname)

    dfStocknamesTemp = aggregate(dfStocknamesTemp, [:permno, :ncusip, :comnam], [minimum, maximum])
    select!(dfStocknamesTemp, Not([:namedt_maximum, :nameenddt_minimum]))
    sort!(dfStocknamesTemp, [:permno, :ncusip, :namedt_minimum])
    dfStocknamesTemp = vcat([i[[end], :] for i in groupby(dfStocknamesTemp, [:permno, :ncusip])]...)


    dfLink1 = join(dfIbesNamesTemp, dfStocknamesTemp, on=[:cusip => :ncusip])
    dfLink1[!, :nameDist] = [compare(dfLink1.cname[i], dfLink1.comnam[i], Levenshtein()) for i in 1:size(dfLink1, 1)]
    dfLink1[!, :score] .= 3

    minimum_ratio = quantile(dfLink1.nameDist, .1)
    for i in 1:size(dfLink1, 1)
        between = dfLink1.namedt_minimum[i] <= dfLink1.sdates_maximum[i] && dfLink1.nameenddt_maximum[i] >= dfLink1.sdates_minimum[i]
        namesMatch = dfLink1.nameDist[i] >= minimum_ratio
        dfLink1[i, :score] = if between && namesMatch
            0
        elseif between
            1
        elseif namesMatch
            2
        else
            3
        end
    end



    dfTemp = unique(dfLink1[:, [:ticker]])
    dfTemp[!, :match] .= 1
    dfMissings = join(dfIbesNames, dfTemp, on=:ticker, kind=:left)
    dfMissings = dfMissings[typeof.(dfMissings.match) .== Missing, :]
    select!(dfMissings, Not(:match))
    dfIbesNamesTemp = dfMissings[:, [:ticker, :cname, :oftic, :sdates, :cusip]]
    dfTemp = aggregate(dfIbesNames[:, [:ticker, :oftic, :sdates]], [:ticker, :oftic], [minimum, maximum])
    dfIbesNamesTemp = join(dfIbesNamesTemp, dfTemp, on=[:ticker, :oftic], kind=:left)
    dfIbesNamesTemp = dfIbesNamesTemp[dfIbesNamesTemp.sdates .== dfIbesNamesTemp.sdates_maximum, :]
    dropmissing!(dfIbesNamesTemp, :cname)

    dfStocknamesTemp = dfStocknames[:, [:ticker, :comnam, :permno, :ncusip, :namedt, :nameenddt]]
    dropmissing!(dfStocknamesTemp, :ticker)
    dfStocknamesTemp = aggregate(dfStocknamesTemp, [:permno, :ticker, :ncusip, :comnam], [minimum, maximum])
    select!(dfStocknamesTemp, Not([:namedt_maximum, :nameenddt_minimum]))
    sort!(dfStocknamesTemp, [:permno, :ticker, :namedt_minimum])
    dfStocknamesTemp = vcat([i[[end], :] for i in groupby(dfStocknamesTemp, [:permno, :ticker])]...)
    rename!(dfStocknamesTemp, :ticker => :ticker_crsp)

    dfLink2 = join(dfIbesNamesTemp, dfStocknamesTemp, on=[:oftic => :ticker_crsp])
    dfLink2 = dfLink2[.&(dfLink2.sdates_maximum .>= dfLink2.namedt_minimum,
                        dfLink2.sdates_minimum .<= dfLink2.nameenddt_maximum), :]
    dfLink2[!, :nameDist] = [compare(dfLink2.cname[i], dfLink2.comnam[i], Levenshtein()) for i in 1:size(dfLink2, 1)]

    dfLink2[!, :score] .= 6
    for i in 1:size(dfLink2, 1)
        cusipMatch = dfLink2[i, :cusip][1:6] == dfLink2[i, :ncusip][1:6]
        namesMatch = dfLink2.nameDist[i] >= minimum_ratio
        dfLink2[i, :score] = if cusipMatch && namesMatch
            0
        elseif cusipMatch
            4
        elseif namesMatch
            5
        else
            6
        end
    end

    dfOutput = unique(vcat(dfLink1[:, [:ticker, :permno, :score]], dfLink2[:, [:ticker, :permno, :score]]))
    dropmissing!(dfOutput)
    sort!(dfOutput, [:ticker, :permno, :score])
    rename!(dfOutput, :score => :matchQuality)
    dfOutput = vcat([i[[1], :] for i in groupby(dfOutput, [:ticker, :permno])]...)
    return dfOutput
end
