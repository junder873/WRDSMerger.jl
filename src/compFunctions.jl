
function comp_data(
    conn::Union{LibPQ.Connection, DBInterface.Connection},
    gvkeys::AbstractArray{String},
    dateStart::Union{Date,Int}=1950,
    dateEnd::Union{Date,Int}=Dates.today();
    annual::Bool=true,
    filters::Dict{String,<:Any}=Dict(
        "datafmt" => "STD",
        "indfmt" => "INDL",
        "consol" => "C",
        "popsrc" => "D"
    ),
    cols::Array{String}=["gvkey", "datadate", "fyear", "sale", "revt", "xopr"],
)
    if 0 < length(gvkeys) < 1000 # I limit this to 1000 since larger strings
        # slow down the query more than necessary
        temp_filters = Dict{String, Any}()
        for (key, val) in filters
            temp_filters[key] = val
        end
        filters = temp_filters
        filters["gvkey"] = gvkeys
    end


    return comp_data(
            conn,
            dateStart,
            dateEnd;
            annual,
            filters,
            cols
        )
end


"""
    function comp_data(
        conn::Union{LibPQ.Connection, DBInterface.Connection}[,
        gvkeys::AbstractArray{String},]
        dateStart::Union{Date,Int}=1950,
        dateEnd::Union{Date,Int}=Dates.today();
        annual::Bool=true,
        filters::Dict{String,<:Any}=Dict(
            "datafmt" => "STD",
            "indfmt" => "INDL",
            "consol" => "C",
            "popsrc" => "D"
        ),
        cols::Array{String}=["gvkey", "datadate", "fyear", "sale", "revt", "xopr"]
    )

Downloads data from Compustat for firms (if list of gvkeys is provided,
filters to those firms) available firms over a period. Data can be annual
(set `annual=true`) or quarterly (set `annual=false`). For
quarterly data, you also likely need to change the columns
that are downloaded (ie, sales is "saleq" in quarterly data).

Filters is a dictionary of String => String (or array of String) pairings that
will be applied to the SQL query.
"""
function comp_data(
    conn::Union{LibPQ.Connection, DBInterface.Connection},
    dateStart::Union{Date,Int}=1950,
    dateEnd::Union{Date,Int}=Dates.today();
    annual::Bool=true,
    filters::Dict{String,<:Any}=Dict(
        "datafmt" => "STD",
        "indfmt" => "INDL",
        "consol" => "C",
        "popsrc" => "D"
    ),
    cols::AbstractArray{String}=["gvkey", "datadate", "fyear", "sale", "revt", "xopr"]
)
    
    if typeof(dateStart) == Int
        dateStart = Dates.Date(dateStart, 1, 1)
    end
    if typeof(dateEnd) == Int
        dateEnd = Dates.Date(dateEnd, 12, 31)
    end

    tab = annual ? default_tables["comp_funda"] : default_tables["comp_fundq"]

    colString = join(cols, ", ")
    filterString = create_filter(filters, "WHERE datadate between '$dateStart' AND '$dateEnd'")
    query = """
        select $colString
        from $tab
        $filterString
    """
    
    return run_sql_query(conn, query) |> DataFrame
end
