function compustatCrspLink(
    dsn;
    cols::Array{String}=["gvkey", "lpermno", "linkdt", "linkenddt"]
)
    col_str = join(cols, ", ")
    query = """
        select distinct $col_str
        from crsp_a_ccm.ccmxpf_lnkhist
        where lpermno IS NOT NULL AND
        linktype in ('LU', 'LC') AND
        linkprim in ('P', 'C')       
    """
    dfLink = LibPQ.execute(dsn, query) |> DataFrame;
    if "linkenddt" in cols
        dfLink[!, :linkenddt] = coalesce.(dfLink[:, :linkenddt], Dates.today())
    end
    if "lpermno" in cols
        rename!(dfLink, :lpermno => :permno)
    end
    return dfLink
end


function compustatCrspLink(
    dsn,
    vals;
    id_col::String="gvkey", # either "gvkey" or "lpermno"
    cols::Array{String}=["gvkey", "lpermno", "linkdt", "linkenddt"]
)
    if length(vals) == 0 || length(vals) > 1000
        return compustatCrspLink(dsn; cols)
    end
    if id_col == "permno"
        id_col = "lpermno"
    end
    col_str = join(cols, ", ")
    fil = if id_col == "gvkey"
        "('" * join(vals, "', '") * "')"
    else
        "(" * join(vals, ", ") * ")"
    end
    query = """
        select distinct $col_str
        from crsp_a_ccm.ccmxpf_lnkhist
        where lpermno IS NOT NULL AND
        linktype in ('LU', 'LC') AND
        linkprim in ('P', 'C') AND
        $id_col IN $fil
    """
    dfLink = LibPQ.execute(dsn, query) |> DataFrame;
    if "linkenddt" in cols
        dfLink[!, :linkenddt] = coalesce.(dfLink[:, :linkenddt], Dates.today())
    end
    if "lpermno" in cols
        rename!(dfLink, :lpermno => :permno)
    end
    return dfLink
end

function cik_to_gvkey(
    dsn::LibPQ.Connection;
    cols::Array{String}=["gvkey", "cik"]
)
    colString = join(cols, ", ")
    query = "SELECT DISTINCT $colString FROM comp.company WHERE cik IS NOT NULL"
    return LibPQ.execute(dsn, query) |> DataFrame
end

function cik_to_gvkey(
    dsn,
    vals::Array{String};
    id_col::String="cik", # either "cik" or "gvkey"
    cols::Array{String}=["gvkey", "cik"]
)
    if length(vals) == 0 || length(vals) > 1000
        return cik_to_gvkey(dsn; cols)
    end

    colString = join(cols, ", ")
    fil_str = join(unique(vals), "', '")
    query = """
        SELECT DISTINCT $colString
        FROM comp.company
        WHERE cik IS NOT NULL
        AND $id_col IN ('$(fil_str)')
    """
    return LibPQ.execute(dsn, query) |> DataFrame
end

function join_permno_gvkey(
    dsn,
    df::DataFrame;
    id_col::String="gvkey",
    forceUnique::Bool=false,
    datecol::String="date"
)
    comp = unique(compustatCrspLink(dsn, df[:, id_col]; id_col))

    comp[!, :linkdt] = coalesce.(comp[:, :linkdt], minimum(df[:, datecol]))

    try
        df = range_join(
            df,
            comp,
            [id_col],
            [
                (>=, Symbol(datecol), :linkdt),
                (<=, Symbol(datecol), :linkenddt)
            ],
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
            validate=(false, true)
        else
            validate=(false, false)
            @warn "There are multiple PERMNOs per GVKEY, be careful on merging with other datasets"
            @warn "Pass forceUnique=true to the function to prevent this error"
        end
        df = range_join(
            df,
            comp,
            [id_col],
            [
                (>=, Symbol(datecol), :linkdt),
                (<=, Symbol(datecol), :linkenddt)
            ];
            validate
        )
    end
    select!(df, Not([:linkdt, :linkenddt]))
    return df
end


function link_identifiers(
    dsn::LibPQ.Connection,
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
    df = copy(df)
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
    if ncusip_name in names(df) && any(length.(df[:, ncusip_name]) .> 8)
        throw("Cusip or NCusip value must be of length 8 to match CRSP values")
    end
    if cusip_name in names(df) && any(length.(df[:, cusip_name]) .> 8)
        throw("Cusip or NCusip value must be of length 8 to match CRSP values")
    end

    # Go through a series of checks, makes sure original identifying col is
    # in the output, there are no missing identifiers, and converts those
    # that can be strings or numbers (CIK and GVKEY) into the appropriate
    # format. I also rename all columns to make my life easier later
    if cik_name in names(df)
        cik = true
        identifying_col="cik"
        rename!(df, cik_name => "cik")
        identifier_was_int=false
        if typeof(df[:, "cik"]) <: Array{<:Real}
            dropmissing!(df, "cik")
            df[!, "cik"] = lpad.(df[:, "cik"], 10, "0")
            identifier_was_int=true
        end
    end
    if ncusip_name in names(df)
        ncusip = true
        identifying_col="ncusip"
        rename!(df, ncusip_name => "ncusip")
        identifier_was_int=false
        dropmissing!(df, "ncusip")
    end
    if cusip_name in names(df)
        cusip = true
        identifying_col="cusip"
        rename!(df, cusip_name => "cusip")
        identifier_was_int=false
        dropmissing!(df, "cusip")
    end
    if gvkey_name in names(df)
        gvkey = true
        identifying_col="gvkey"
        rename!(df, gvkey_name => "gvkey")
        identifier_was_int=false
        dropmissing!(df, "gvkey")
        if typeof(df[:, "gvkey"]) <: Array{<:Real}
            df[!, "gvkey"] = lpad.(df[:, "gvkey"], 6, "0")
            identifier_was_int=true
        end
    end
    if permno_name in names(df)
        permno = true
        identifying_col="permno"
        rename!(df, permno_name => "permno")
        identifier_was_int=true
        dropmissing!(df, "permno")
    end


    # cik is only easy to link to gvkey, so that must come first
    if identifying_col == "cik"
        cik_gvkey = cik_to_gvkey(dsn, unique(df[:, "cik"]); id_col="cik")
        df = leftjoin(
            df,
            cik_gvkey,
            on=["cik"],
            validate=(false, true)
        )
        if any([permno, cusip, ncusip])
            comp = link_identifiers(
                dsn,
                df[:, ["gvkey", datecol]] |> dropmissing |> unique;
                permno,
                cusip,
                ncusip,
                datecol
            )
            df = leftjoin(
                df,
                comp,
                on=["gvkey", datecol],
                validate=(false, true),
                matchmissing=:equal
            )
        end
    end

    # If gvkey, need to get the link to permno
    if identifying_col == "gvkey" && any([permno, cusip, ncusip])
        permno_gvkey = join_permno_gvkey(
            dsn,
            df;
            forceUnique,
            id_col="gvkey",
            datecol
        )
        df = leftjoin(
            df,
            permno_gvkey,
            on=["gvkey", datecol],
            validate=(false, true)
        )
        if any([cusip, ncusip])
            crsp = link_identifiers(
                dsn,
                dropmissing(permno_gvkey[:, ["permno", datecol]]) |> unique;
                ncusip,
                cusip,
                datecol
            )
            df = leftjoin(
                df,
                crsp,
                on=["permno", datecol],
                validate=(false, true),
                matchmissing=:equal
            )
        end
    end

    # If gvkey and need cik
    if identifying_col == "gvkey" && cik
        temp = dropmissing(df[:, ["gvkey"]])
        cik_gvkey = cik_to_gvkey(dsn, temp[:, "gvkey"]; id_col="gvkey")
        dropmissing!(cik_gvkey)
        df = leftjoin(
            df,
            cik_gvkey,
            on=["gvkey"],
            validate=(false, true),
            matchmissing=:equal
        )
    end

    # if ncusip or cusip, first need the permno set (which is trivial)
    # if still need gvkey or cik then permno is necessary
    if identifying_col ∈ ["ncusip", "cusip"]
        crsp = crsp_stocknames(
            dsn,
            df[:, identifying_col];
            cusip_col=identifying_col,
            warn_on_long=false
        )
        crsp[!, :namedt] = coalesce.(crsp[:, :namedt], minimum(df[:, datecol]))
        crsp[!, :nameenddt] = coalesce.(crsp[:, :nameenddt], maximum(df[:, datecol]))
        df = range_join(
            df,
            crsp,
            [identifying_col],
            [
                (<=, Symbol(datecol), :nameenddt),
                (>=, Symbol(datecol), :namedt)
            ],
            validate=(false, true)
        )
        select!(df, Not([:namedt, :nameenddt]))
        if gvkey || cik
            crsp = link_identifiers(
                dsn,
                dropmissing(df[:, ["permno", datecol]]) |> unique;
                cik,
                gvkey,
                datecol
            )
            df = leftjoin(
                df,
                crsp,
                on=["permno", datecol],
                validate=(false, true),
                matchmissing=:equal
            )
        end
    end

    if identifying_col == "permno"
        if ncusip || cusip
            crsp = crsp_stocknames(
                dsn,
                df[:, identifying_col];
                warn_on_long=false
            )
            crsp[!, :namedt] = coalesce.(crsp[:, :namedt], minimum(df[:, datecol]))
            crsp[!, :nameenddt] = coalesce.(crsp[:, :nameenddt], maximum(df[:, datecol]))
            df = range_join(
                df,
                crsp,
                [identifying_col],
                [
                    (<=, Symbol(datecol), :nameenddt),
                    (>=, Symbol(datecol), :namedt)
                ],
                validate=(false, true)
            )
            select!(df, Not([:namedt, :nameenddt]))
        end
        if gvkey || cik
            df = join_permno_gvkey(
                dsn,
                df;
                forceUnique,
                id_col="permno",
                datecol
            )
            if cik
                comp = link_identifiers(
                    dsn,
                    df[:, ["gvkey", datecol]] |> dropmissing |> unique;
                    datecol,
                    cik
                )
                df = leftjoin(
                    df,
                    comp,
                    on=["gvkey", datecol],
                    validate=(false, true),
                    matchmissing=:equal
                )
            end
        end
    end

    clean_up = [
        (ncusip, "ncusip", ncusip_name),
        (cusip, "cusip", cusip_name),
        (gvkey, "gvkey", gvkey_name),
        (permno, "permno", permno_name),
        (cik, "cik", cik_name)
    ]

    for (to_include, cur_name, new_name) in clean_up
        if !to_include && cur_name ∈ names(df)
            select!(df, Not(cur_name))
        end
        if cur_name ∈ names(df) && cur_name != new_name
            rename!(df, cur_name => new_name)
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
    dfStocknames = LibPQ.execute(dsn, query) |> DataFrame;
    query = """
        SELECT * FROM ibes.idsum WHERE usfirm=1
    """
    dfIbesNames = LibPQ.execute(dsn, query) |> DataFrame;
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
