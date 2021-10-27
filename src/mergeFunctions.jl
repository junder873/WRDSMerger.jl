function compustatCrspLink(
    conn;
    cols::Array{String}=["gvkey", "lpermno", "linkdt", "linkenddt"]
)
    col_str = join(cols, ", ")
    query = """
        select distinct $col_str
        from $(default_tables.crsp_a_ccm_ccmxpf_lnkhist)
        where lpermno IS NOT NULL AND
        linktype in ('LU', 'LC') AND
        linkprim in ('P', 'C')       
    """
    dfLink = run_sql_query(conn, query) |> DataFrame;
    if "linkenddt" in cols
        dfLink[!, :linkenddt] = coalesce.(dfLink[:, :linkenddt], Dates.today())
    end
    if "lpermno" in cols
        rename!(dfLink, :lpermno => :permno)
    end
    return dfLink
end


function compustatCrspLink(
    conn,
    vals;
    id_col::String="gvkey", # either "gvkey" or "lpermno"
    cols::Array{String}=["gvkey", "lpermno", "linkdt", "linkenddt"]
)
    if length(vals) == 0 || length(vals) > 1000
        return compustatCrspLink(conn; cols)
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
        from $(default_tables.crsp_a_ccm_ccmxpf_lnkhist)
        where lpermno IS NOT NULL AND
        linktype in ('LU', 'LC') AND
        linkprim in ('P', 'C') AND
        $id_col IN $fil
    """
    dfLink = run_sql_query(conn, query) |> DataFrame;
    if "linkenddt" in cols
        dfLink[!, :linkenddt] = coalesce.(dfLink[:, :linkenddt], Dates.today())
    end
    if "lpermno" in cols
        rename!(dfLink, :lpermno => :permno)
    end
    return dfLink
end

function ibes_crsp_link(
    conn::Union{LibPQ.Connection, DBInterface.Connection};
    cols::Vector{String}=["ticker", "permno", "sdate", "edate", "score"],
    filter_score::Int=4 # maximum of 6
)
    col_str = join(cols, ", ")
    query = """
        SELECT DISTINCT $col_str FROM $(default_tables.ibes_crsp)
        WHERE score <= $filter_score
        AND permno IS NOT NULL
    """
    df = run_sql_query(conn, query)
    if "edate" ∈ names(df)
        df[!, "edate"] = coalesce.(df[:, "edate"], Dates.today())
    end
    return df
end

function ibes_crsp_link(
    conn::Union{LibPQ.Connection, DBInterface.Connection},
    tickers::Vector{String};
    cols::Vector{String}=["ticker", "permno", "sdate", "edate", "score"],
    filter_score::Int=4 # maximum of 6
)
    if length(tickers) == 0 || length(tickers) > 1000
        return ibes_crsp_link(conn; cols, filter_score)
    end

    col_str = join(cols, ", ")
    fil = "('" * join(tickers, "', '") * "')"

    query = """
        SELECT DISTINCT $col_str FROM $(default_tables.ibes_crsp)
        WHERE score <= $filter_score
        AND permno IS NOT NULL
        AND ticker IN $fil
    """
    df = run_sql_query(conn, query)
    if "edate" ∈ names(df)
        df[!, "edate"] = coalesce.(df[:, "edate"], Dates.today())
    end
    return df
end

function ibes_crsp_link(
    conn::Union{LibPQ.Connection, DBInterface.Connection},
    permno::Vector{<:Number};
    cols::Vector{String}=["ticker", "permno", "sdate", "edate", "score"],
    filter_score::Int=4 # maximum of 6
)
    if length(permno) == 0 || length(permno) > 1000
        return ibes_crsp_link(conn; cols, filter_score)
    end

    col_str = join(cols, ", ")
    fil = "(" * join(permno, ", ") * ")"

    query = """
        SELECT DISTINCT $col_str FROM $(default_tables.ibes_crsp)
        WHERE score <= $filter_score
        AND permno IS NOT NULL
        AND permno IN $fil
    """
    df = run_sql_query(conn, query)
    if "edate" ∈ names(df)
        df[!, "edate"] = coalesce.(df[:, "edate"], Dates.today())
    end
    return df
end

function cik_to_gvkey(
    conn::Union{LibPQ.Connection, DBInterface.Connection};
    cols::Array{String}=["gvkey", "cik"]
)
    colString = join(cols, ", ")
    query = "SELECT DISTINCT $colString FROM $(default_tables.comp_company) WHERE cik IS NOT NULL"
    return run_sql_query(conn, query) |> DataFrame
end

function cik_to_gvkey(
    conn,
    vals::Array{String};
    id_col::String="cik", # either "cik" or "gvkey"
    cols::Array{String}=["gvkey", "cik"]
)
    if length(vals) == 0 || length(vals) > 1000
        return cik_to_gvkey(conn; cols)
    end

    colString = join(cols, ", ")
    fil_str = join(unique(vals), "', '")
    query = """
        SELECT DISTINCT $colString
        FROM $(default_tables.comp_company)
        WHERE cik IS NOT NULL
        AND $id_col IN ('$(fil_str)')
    """
    return run_sql_query(conn, query) |> DataFrame
end

function join_permno_gvkey(
    conn,
    df::DataFrame;
    id_col::String="gvkey",
    forceUnique::Bool=false,
    datecol::String="date"
)
    comp = unique(compustatCrspLink(conn, df[:, id_col] |> unique; id_col))

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
    conn::Union{LibPQ.Connection, DBInterface.Connection},
    df::DataFrame;
    cik::Bool=false,
    ncusip::Bool=false,
    cusip::Bool=false,
    gvkey::Bool=false,
    permno::Bool=false,
    ticker::Bool=false,
    ibes_ticker::Bool=false,
    forceUnique::Bool=false,
    datecol::String="date",
    cik_name::String="cik",
    cusip_name::String="cusip",
    gvkey_name::String="gvkey",
    permno_name::String="permno",
    ncusip_name::String="ncusip",
    ticker_name::String="ticker",
    ibes_ticker_name::String="ibes_ticker"

)
    df = copy(df)
    if datecol ∉ names(df)
        throw("DataFrame must include a date column")
    end
    col_count = sum([x in names(df) for x in [cik_name, ncusip_name, cusip_name, gvkey_name, permno_name, ticker_name, ibes_ticker_name]])
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
    if ticker_name in names(df)
        ticker=true
        identifying_col="ticker"
        rename!(df, ticker_name => "ticker")
        identifier_was_int=false
        dropmissing!(df, "ticker")
    end
    if ibes_ticker_name in names(df)
        ibes_ticker=true
        identifying_col="ibes_ticker"
        rename!(df, ibes_ticker_name => "ibes_ticker")
        identifier_was_int=false
        dropmissing!(df, "ibes_ticker")
    end


    if identifying_col == "ibes_ticker"
        ibes_to_crsp = ibes_crsp_link(
            conn,
            df[:, "ibes_ticker"];
        )
        df[!, :_temp_min] .= 0 # to use the minimize function in the range join
        df = range_join(
            df,
            ibes_to_crsp,
            ["ibes_ticker" => "ticker"],
            [
                Conditions(<=, datecol, "edate"),
                Conditions(>=, datecol, "sdate")
            ],
            validate=(false, true),
            minimize=["_temp_min" => "score"]
        )
        select!(df, Not(["sdate", "edate", "_temp_min"]))
        if any([cusip, ncusip, gvkey, cik])
            temp = link_identifiers(
                conn,
                df[:, ["permno", datecol]] |> dropmissing |> unique;
                cusip,
                ncusip,
                gvkey,
                cik,
                datecol
            )
            df = leftjoin(
                df,
                temp,
                on=["permno", datecol],
                validate=(false, true),
                matchmissing=:equal
            )
        end
    end

    # cik is only easy to link to gvkey, so that must come first
    if identifying_col == "cik"
        cik_gvkey = cik_to_gvkey(conn, unique(df[:, "cik"]); id_col="cik")
        df = leftjoin(
            df,
            cik_gvkey,
            on=["cik"],
            validate=(false, true)
        )
        if any([permno, cusip, ncusip, ticker, ibes_ticker])
            comp = link_identifiers(
                conn,
                df[:, ["gvkey", datecol]] |> dropmissing |> unique;
                permno,
                cusip,
                ncusip,
                ticker,
                ibes_ticker,
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
    if identifying_col == "gvkey" && any([permno, cusip, ncusip, ibes_ticker, ticker])
        permno_gvkey = join_permno_gvkey(
            conn,
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
        if any([cusip, ncusip, ticker, ibes_ticker])
            crsp = link_identifiers(
                conn,
                dropmissing(permno_gvkey[:, ["permno", datecol]]) |> unique;
                ncusip,
                cusip,
                ticker,
                ibes_ticker,
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
        cik_gvkey = cik_to_gvkey(conn, temp[:, "gvkey"]; id_col="gvkey")
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
    if identifying_col ∈ ["ncusip", "cusip", "ticker"]
        crsp = crsp_stocknames(
            conn,
            df[:, identifying_col] |> unique;
            cusip_col=identifying_col,
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
        if gvkey || cik || ibes_ticker
            crsp = link_identifiers(
                conn,
                dropmissing(df[:, ["permno", datecol]]) |> unique;
                cik,
                gvkey,
                ibes_ticker,
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

    # the final case I need to deal with is permno, it is a major link between several
    # of the datasets
    if identifying_col == "permno"
        if ncusip || cusip || ticker
            crsp = crsp_stocknames(
                conn,
                df[:, identifying_col] |> unique;
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
                conn,
                df;
                forceUnique,
                id_col="permno",
                datecol
            )
            if cik
                comp = link_identifiers(
                    conn,
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
        if ibes_ticker
            ibes_to_crsp = ibes_crsp_link(
                conn,
                df[:, "permno"];
            )
            rename!(ibes_to_crsp, "ticker" => "ibes_ticker")
            df[!, :_temp_min] .= 0
            df = range_join(
                df,
                ibes_to_crsp,
                ["permno"],
                [
                    Conditions(<=, datecol, "edate"),
                    Conditions(>=, datecol, "sdate")
                ];
                validate=(false, true),
                minimize=["score" => "_temp_min"]
            )
            select!(df, Not(["sdate", "edate", "_temp_min"]))
        end
    end

    clean_up = [
        (ncusip, "ncusip", ncusip_name),
        (cusip, "cusip", cusip_name),
        (gvkey, "gvkey", gvkey_name),
        (permno, "permno", permno_name),
        (cik, "cik", cik_name),
        (ticker, "ticker", ticker_name),
        (ibes_ticker, "ibes_ticker", ibes_ticker_name)
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
