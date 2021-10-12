function createFilter(filters::Union{Dict{String,String},Dict{String,Array{String}}})

    filterString = ""
    for (key, data) in filters
        if typeof(data) <: Array{String}
            tempString = ""
            for s in data
                if tempString == ""
                    tempString = "'$s'"
                else
                    tempString = "$tempString, '$s'"
                end
            end
            filterString = "$filterString AND $key IN ($tempString)"
        else
            filterString = "$filterString AND $key = '$data'"
        end
    end
    return filterString
end


"""
    function comp_data(
        dsn::Union{LibPQ.Connection, DBInterface.Connection},
        gvkeys::AbstractArray{String},
        dateStart::Union{Date, Int},
        dateEnd::Union{Date, Int};
        annual::Bool=true,
        filters::Union{Dict{String,String},Dict{String,Array{String}}}=Dict(
            "datafmt" => "STD",
            "indfmt" => "INDL",
            "consol" => "C",
            "popsrc" => "D"
        ),
        cols::Array{String}=["gvkey", "datadate", "fyear", "sale", "revt", "xopr"],
    )

Downloads data from Compustat for a group of firms over a period. Data can be annual
(set `annual=true`) or quarterly (set `annual=false`). For
quarterly data, you also likely need to change the columns
that are downloaded (ie, sales is "saleq" in quarterly data).

Filters is a dictionary of String => String pairings that will be applied to the SQL query.
It can also accept an array of Strings.
"""
function comp_data(
    dsn::Union{LibPQ.Connection, DBInterface.Connection},
    gvkeys::AbstractArray{String},
    dateStart::Union{Date,Int}=1950,
    dateEnd::Union{Date,Int}=Dates.today();
    table::String="compa.funda",
    filters::Union{Dict{String,String},Dict{String,Array{String}}}=Dict(
        "datafmt" => "STD",
        "indfmt" => "INDL",
        "consol" => "C",
        "popsrc" => "D"
    ),
    cols::Array{String}=["gvkey", "datadate", "fyear", "sale", "revt", "xopr"],
)
    if typeof(dateStart) == Int
        dateStart = Dates.Date(dateStart, 1, 1)
    end
    if typeof(dateEnd) == Int
        dateEnd = Dates.Date(dateEnd, 12, 31)
    end


    colString = join(cols, ", ")
    filterString = createFilter(filters)
    gvkey_str = "('" * join(gvkeys, "', '") * "')"
    query = """
        SELECT $colString FROM $table
        WHERE datadate BETWEEN '$(dateStart)' and '$(dateEnd)'
        AND gvkey IN $gvkey_str $filterString
        """


    return run_sql_query(dsn, query) |> DataFrame
end


"""
    function comp_data(
        dsn::Union{LibPQ.Connection, DBInterface.Connection},
        dateStart::Union{Date,Int}=1950,
        dateEnd::Union{Date,Int}=Dates.today();
        annual::Bool=true,
        filters::Union{Dict{String,String},Dict{String,Array{String}}}=Dict(
            "datafmt" => "STD",
            "indfmt" => "INDL",
            "consol" => "C",
            "popsrc" => "D"
        ),
        cols::Array{String}=["gvkey", "datadate", "fyear", "sale", "revt", "xopr"]
    )

Downloads data from Compustat for all available firms over a period. Data can be annual
(set `annual=true`) or quarterly (set `annual=false`). For
quarterly data, you also likely need to change the columns
that are downloaded (ie, sales is "saleq" in quarterly data).

Filters is a dictionary of String => String pairings that will be applied to the SQL query.
It can also accept an array of Strings.
"""
function comp_data(
    dsn::Union{LibPQ.Connection, DBInterface.Connection},
    dateStart::Union{Date,Int}=1950,
    dateEnd::Union{Date,Int}=Dates.today();
    table::String="compa.funda",
    filters::Union{Dict{String,String},Dict{String,Array{String}}}=Dict(
        "datafmt" => "STD",
        "indfmt" => "INDL",
        "consol" => "C",
        "popsrc" => "D"
    ),
    cols::Array{String}=["gvkey", "datadate", "fyear", "sale", "revt", "xopr"],
)
    
    if typeof(dateStart) == Int
        dateStart = Dates.Date(dateStart, 1, 1)
    end
    if typeof(dateEnd) == Int
        dateEnd = Dates.Date(dateEnd, 12, 31)
    end

    colString = join(cols, ", ")
    filterString = createFilter(filters)
    query = """
        select $colString
        from $table
        where datadate between '$dateStart' and '$dateEnd' $filterString
    """
    
    return run_sql_query(dsn, query) |> DataFrame
end
