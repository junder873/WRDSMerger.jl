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



function comp_data(
    dsn::LibPQ.Connection,
    df::DataFrame;
    fund::String="funda",
    filters::Union{Dict{String,String},Dict{String,Array{String}}}=Dict("datafmt" => "STD", "indfmt" => "INDL", "consol" => "C", "popsrc" => "D"),
    columns::Array{String}=["gvkey", "datadate", "fyear", "sale", "revt", "xopr"],
    date_start::String="dateStart",
    date_end::String="dateEnd"
)

    for col in ["gvkey", "dateStart", "dateEnd"]
        if col ∉ names(df)
            println("$col must be in the DataFrame")
            return 0
        end
    end

    colString = join(columns, ", ")
    filterString = createFilter(filters)
    gvkey_str = "('" * join(df[:, :gvkey], "', '") * "')"
    date_start = minimum(df[:, date_start])
    date_end = maximum(df[:, date_end])
    query = """
        SELECT $colString FROM compa.$fund
        WHERE datadate BETWEEN '$(date_start)' and '$(date_end)'
        AND gvkey IN $gvkey_str $filterString
        """


    return LibPQ.execute(dsn, query) |> DataFrame
end

function comp_data(
    dsn::LibPQ.Connection,
    dateStart::Union{Date,Int}=1950,
    dateEnd::Union{Date,Int}=Dates.today();
    fund::String="funda",
    filters::Union{Dict{String,String},Dict{String,Array{String}}}=Dict(
        "datafmt" => "STD",
        "indfmt" => "INDL",
        "consol" => "C",
        "popsrc" => "D"
    ),
    columns::Array{String}=["gvkey", "datadate", "fyear", "sale", "revt", "xopr"]
)
    
    if typeof(dateStart) == Int
        dateStart = Dates.Date(dateStart, 1, 1)
    end
    if typeof(dateEnd) == Int
        dateEnd = Dates.Date(dateEnd, 12, 31)
    end

    colString = join(columns, ", ")
    filterString = createFilter(filters)
    query = """
        select $colString
        from compa.$fund
        where datadate between '$dateStart' and '$dateEnd' $filterString
    """
    
    return LibPQ.execute(dsn, query) |> DataFrame
end
