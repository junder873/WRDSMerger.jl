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