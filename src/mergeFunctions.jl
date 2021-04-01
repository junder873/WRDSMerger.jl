function compustatCrspLink(dsn;
    gvkey::Array{String} = String[],
    lpermno::Array{<:Number} = Int[],
    cols::Array{String} = ["gvkey", "lpermno", "linkdt", "linkenddt"])

    if sum(length.([gvkey, lpermno]) .> 0) > 1 # Tests if any array has data, sums to test if multiple have data
        @error("Only one of the identifying columns can have values in it")
        throw("Please retry where either gvkey or lpermno have data")
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
                            where lpermno IS NOT NULL AND
                            linktype in ('LU', 'LC') AND
                            linkprim in ('P', 'C')       
                            """
    elseif length(gvkey) > 100 || length(lpermno) > 100 # If a lot of data, get all of it
        query = """
                select distinct $colString
                from crsp_a_ccm.ccmxpf_lnkhist
                where lpermno IS NOT NULL AND
                linktype in ('LU', 'LC') AND
                linkprim in ('P', 'C')
                """
        
    else
        if length(gvkey) > 0
            queryL = String[]
            for x in gvkey
                temp_query = """
                                (select $colString
                                from crsp_a_ccm.ccmxpf_lnkhist
                                where lpermno IS NOT NULL and gvkey = '$x' AND
                                linktype in ('LU', 'LC') AND
                                linkprim in ('P', 'C'))
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
                                where lpermno IS NOT NULL and lpermno = $x AND
                                linktype in ('LU', 'LC') AND
                                linkprim in ('P', 'C'))
                                """
                push!(queryL, temp_query)
            end
            query = join(queryL, " UNION ")
        end
    end

    dfLink = DBInterface.execute(dsn, query) |> DataFrame;
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

function cik_to_gvkey(
    dsn;
    cik::Array{String}=String[],
    gvkey::Array{String}=String[],
    cols::Array{String}=["gvkey", "cik"]
)
    if sum(length.([gvkey, cik]) .> 0) > 1 # Tests if any array has data, sums to test if multiple have data
        @error("Only one of the identifying columns can have values in it")
        throw("Please retry where either gvkey or cik have data")
    end
    colString = ""
    for col in cols
        if colString == ""
            colString = col
        else
            colString = colString * ", " * col
        end
    end
    if (sum(length.([gvkey, cik]) .> 0) == 0) || (length(gvkey) > 100 || length(cik) > 100)
        query = "SELECT DISTINCT $colString FROM comp.company WHERE cik IS NOT NULL"
    else
        if length(gvkey) > 0
            queryL = String[]
            for x in gvkey
                temp_query = "SELECT DISTINCT $colString FROM comp.company WHERE cik IS NOT NULL AND gvkey = $x"
                push!(queryL, temp_query)
            end
            query = join(queryL, " UNION ")
        else
            queryL = String[]
            for x in cik
                temp_query = "SELECT DISTINCT $colString FROM comp.company WHERE cik IS NOT NULL AND cik = $x"
                push!(queryL, temp_query)
            end
            query = join(queryL, " UNION ")
        end
    end

    dfLink = DBInterface.execute(dsn, query) |> DataFrame
    return dfLink
end

function join_permno_gvkey(
    dsn,
    df::DataFrame;
    forceUnique::Bool=false,
    col1::String="gvkey",
    col2::String="gvkey",
    datecol::String="date"
)
    if col2 == "gvkey"
        comp = unique(compustatCrspLink(dsn, gvkey=df[:, col1]))
    else
        comp = unique(compustatCrspLink(dsn, lpermno=df[:, col1]))
    end
    comp[!, :linkdt] = coalesce.(comp[:, :linkdt], minimum(df[:, datecol]) - Dates.Day(1))
    comp[!, :linkenddt] = coalesce.(comp[:, :linkenddt], Dates.today())
    try
        df = range_join(
            df,
            comp,
            [col1 => col2],
            [(>, Symbol(datecol), :linkdt), (<=, Symbol(datecol), :linkenddt)],
            validate=(false, true)
        )
    catch
        if forceUnique
            sort!(comp, [:gvkey, :linkdt])
            for i in 1:size(comp, 1)-1
                if comp[i, :gvkey] != comp[i+1, :gvkey]
                    continue
                end
                if comp[i+1, :linkdt] <= comp[i, :linkenddt]
                    comp[i+1, :linkdt] = comp[i, :linkenddt] + Dates.Day(1)
                    if comp[i+1, :linkenddt] < comp[i+1, :linkdt]
                        comp[i+1, :linkenddt] = comp[i+1, :linkdt]
                    end
                end
            end
            validate=(true, false)
        else
            validate=(false, false)
            @warn "There are multiple PERMNOs per GVKEY, be careful on merging with other datasets"
            @warn "Pass forceUnique=true to the function to prevent this error"
        end
        df = range_join(
            df,
            comp,
            [col1 => col2],
            [(>, Symbol(datecol), :linkdt), (<=, Symbol(datecol), :linkenddt)];
            validate
        )
    end
    select!(df, Not([:linkdt, :linkenddt]))
    return df
end


function addIdentifiers(
    dsn,
    df::DataFrame;
    cik::Bool=false,
    ncusip::Bool=false,
    cusip::Bool=false,
    gvkey::Bool=false,
    permno::Bool=false,
    forceUnique::Bool=false,
    datecol::String="date",
    cik_name::String="cik",
    cusip_name::String="cusip",
    gvkey_name::String="gvkey",
    permno_name::String="permno",
    ncusip_name::String="ncusip",
)
    df = df[:, :]
    if datecol ∉ names(df)
        throw("DataFrame must include a date column")
    end
    col_count = sum([x in names(df) for x in [cik_name, ncusip_name, cusip_name, gvkey_name, permno_name]])
    if col_count == 0
        throw("DataFrame must include identifying column: cik, cusip, ncusip, gvkey, or permno")
    end
    if col_count > 1
        @warn("Function has a preset order on which key will be used first, it is optimal to start with one key")
    end
    if cik_name in names(df)
        @warn("Observations with a CIK that does not have a matching GVKEY will be dropped")
    end
    if ncusip_name in names(df) && any(length.(df[:, ncusip_name]) .> 8)
        throw("Cusip or NCusip value must be of length 8 to match CRSP values")
    end
    if cusip_name in names(df) && any(length.(df[:, cusip_name]) .> 8)
        throw("Cusip or NCusip value must be of length 8 to match CRSP values")
    end

    if cik_name in names(df)
        cik = true
        identifying_col=cik_name
        identifier_was_int=false
        if typeof(df[:, cik_name]) <: Array{<:Real}
            dropmissing!(df, cik_name)
            df[!, cik_name] = lpad.(df[:, cik_name], 10, "0")
            identifier_was_int=true
        end
    end
    if ncusip_name in names(df)
        ncusip = true
        identifying_col=ncusip_name
        identifier_was_int=false
        dropmissing!(df, ncusip_name)
    end
    if cusip_name in names(df)
        cusip = true
        identifying_col=cusip_name
        identifier_was_int=false
        dropmissing!(df, cusip_name)
    end
    if gvkey_name in names(df)
        gvkey = true
        identifying_col=gvkey_name
        identifier_was_int=false
        dropmissing!(df, gvkey_name)
        if typeof(df[:, gvkey_name]) <: Array{<:Real}
            df[!, gvkey_name] = lpad.(df[:, gvkey_name], 6, "0")
            identifier_was_int=true
        end
    end
    if permno_name in names(df)
        permno = true
        identifying_col=permno_name
        identifier_was_int=true
        dropmissing!(df, permno_name)
    end

    

    if cik_name in names(df)
        ciks = cik_to_gvkey(dsn, cik=unique(df[:, cik_name]))
        df = innerjoin(
            df,
            ciks,
            on=[cik_name => "cik"],
            validate=(false, true)
        )
        dropmissing!(df, "gvkey")
    end


    if gvkey_name in names(df) # If gvkey exists and no other identifier does, permno must be fetched
        if permno_name ∉ names(df) && cusip_name ∉ names(df) && ncusip_name ∉ names(df)
            df = join_permno_gvkey(dsn, df; forceUnique, col1="gvkey", datecol)
        end
    end

    if ncusip_name in names(df) || cusip_name in names(df)
        if permno_name ∉ names(df) # to fetch either cusip, ncusip, or gvkey, permno is either necessary or trivial
            if cusip_name in names(df)
                df = getCrspNames(dsn, df, cusip_name, [:ncusip], datecol=datecol, identifying_col="cusip")
            else
                df = getCrspNames(dsn, df, ncusip_name, [:cusip], datecol=datecol, identifying_col="ncusip")
            end
            dropmissing!(df, [permno_name, cusip_name, ncusip_name])
        end
    end
                
    if permno_name in names(df)
        if (ncusip && ncusip_name ∉ names(df)) || (cusip && cusip_name ∉ names(df))
            df = getCrspNames(dsn, df, permno_name, [:ncusip, :cusip], datecol=datecol)
        end
        if (gvkey && gvkey_name ∉ names(df)) || (cik && cik_name ∉ names(df) && gvkey_name ∉ names(df))
            df = join_permno_gvkey(dsn, df; forceUnique, col1=permno_name, col2="permno", datecol)
        end
    end

    if cik && cik_name ∉ names(df)
        temp = unique(dropmissing(df[:, [gvkey_name]]))
        ciks = cik_to_gvkey(dsn, cik=temp[:, gvkey_name])
        dropmissing!(ciks)
        df = leftjoin(
            df,
            ciks,
            on=[gvkey_name => gvkey_name],
            validate=(false, true),
            matchmissing=:equal
        )
    end

    for pair in [(ncusip, ncusip_name), (cusip, cusip_name), (gvkey, gvkey_name), (permno, permno_name), (cik, cik_name)]
        if !pair[1] && pair[2] in names(df)
            select!(df, Not(pair[2]))
        end
    end

    if identifier_was_int && typeof(df[:, identifying_col]) <: Array{String}
        df[!, identifying_col] = parse.(Int, df[:, identifying_col])
    end
    return df
end

function ibesCrspLink(dsn)
    query = """
        SELECT * FROM crsp.stocknames
    """
    dfStocknames = DBInterface.execute(dsn, query) |> DataFrame;
    query = """
        SELECT * FROM ibes.idsum WHERE usfirm=1
    """
    dfIbesNames = DBInterface.execute(dsn, query) |> DataFrame;
    dfIbesNames[!, :sdates] = Dates.Date.(dfIbesNames.sdates)
    for col in [:namedt, :nameenddt, :st_date, :end_date]
        dfStocknames[!, col] = Dates.Date.(dfStocknames[:, col])
    end

    dfIbesNamesTemp = unique(dfIbesNames[:, [:ticker, :cusip, :cname, :sdates]])
    dfStocknamesTemp = unique(dfStocknames[:, [:permno, :ncusip, :comnam, :namedt, :nameenddt]])
    dropmissing!(dfStocknamesTemp, :ncusip)

    gd = groupby(dfIbesNames[:, [:ticker, :cusip, :sdates]], [:ticker, :cusip])
    dfTemp = combine(gd, valuecols(gd) .=> [minimum, maximum])
    dfIbesNamesTemp = leftjoin(dfIbesNamesTemp, dfTemp, on=[:ticker, :cusip])
    dfIbesNamesTemp = dfIbesNamesTemp[dfIbesNamesTemp.sdates .== dfIbesNamesTemp.sdates_maximum, :]
    dropmissing!(dfIbesNamesTemp, :cname)

    gd = groupby(dfStocknamesTemp, [:permno, :ncusip, :comnam])
    dfStocknamesTemp = combine(gd, :namedt => minimum, :nameenddt => maximum)
    sort!(dfStocknamesTemp, [:permno, :ncusip, :namedt_minimum])
    dfStocknamesTemp = vcat([i[[end], :] for i in groupby(dfStocknamesTemp, [:permno, :ncusip])]...)


    dfLink1 = innerjoin(dfIbesNamesTemp, dfStocknamesTemp, on=[:cusip => :ncusip])
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
    dfMissings = leftjoin(dfIbesNames, dfTemp, on=:ticker)
    dfMissings = dfMissings[typeof.(dfMissings.match) .== Missing, :]
    select!(dfMissings, Not(:match))
    dfIbesNamesTemp = dfMissings[:, [:ticker, :cname, :oftic, :sdates, :cusip]]
    gd = groupby(dfIbesNames[:, [:ticker, :oftic, :sdates]], [:ticker, :oftic])
    dfTemp = combine(gd, valuecols(gd) .=> [minimum, maximum])
    dropmissing!(dfTemp, [:ticker, :oftic])
    dropmissing!(dfIbesNamesTemp, [:ticker, :oftic])
    dfIbesNamesTemp = leftjoin(dfIbesNamesTemp, dfTemp, on=[:ticker, :oftic])
    dfIbesNamesTemp = dfIbesNamesTemp[dfIbesNamesTemp.sdates .== dfIbesNamesTemp.sdates_maximum, :]
    dropmissing!(dfIbesNamesTemp, :cname)

    dfStocknamesTemp = dfStocknames[:, [:ticker, :comnam, :permno, :ncusip, :namedt, :nameenddt]]
    dropmissing!(dfStocknamesTemp, :ticker)
    gd = groupby(dfStocknamesTemp, [:permno, :ticker, :ncusip, :comnam])
    dfStocknamesTemp = combine(gd, :namedt => minimum, :nameenddt => maximum)
    sort!(dfStocknamesTemp, [:permno, :ticker, :namedt_minimum])
    dfStocknamesTemp = vcat([i[[end], :] for i in groupby(dfStocknamesTemp, [:permno, :ticker])]...)
    rename!(dfStocknamesTemp, :ticker => :ticker_crsp)

    dfLink2 = innerjoin(dfIbesNamesTemp, dfStocknamesTemp, on=[:oftic => :ticker_crsp])
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
