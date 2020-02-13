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

function createColString(columns::Array{String})
    
    colString = ""
    for col in columns
        if colString == ""
            colString = col
        else
            colString = colString * ", " * col
        end
    end
    return colString
end

function getCompData(dsn,
    df::DataFrame;
    fund::String="funda",
    filters::Union{Dict{String,String},Dict{String,Array{String}}}=Dict("datafmt" => "STD", "indfmt" => "INDL", "consol" => "C", "popsrc" => "D"),
    columns::Array{String}=["gvkey", "datadate", "fyear", "sale", "revt", "xopr"])

    for col in [:gvkey, :dateStart, :dateEnd]
        if col ∉ names(df)
            println("$(String(col)) must be in the DataFrame")
            return 0
        end
    end



    colString = createColString(columns)
    filterString = createFilter(filters)

    query = String[]
    for i in 1:size(df, 1)
        temp_query = """
                        (select $colString
                        from comp.$fund
                        where datadate between '$(df[i, :dateStart])' and '$(df[i, :dateEnd])' and gvkey = '$(df[i, :gvkey])' $filterString)
                        """
        push!(query, temp_query)
    end

    comp = ODBC.query(dsn, join(query, " UNION "));
    comp[!, :datadate] = Dates.Date.(comp[:, :datadate]);

    return comp
end

function getCompData(dsn,
        dateStart::Union{Date,Int}=1950,
        dateEnd::Union{Date,Int}=Dates.today();
        fund::String="funda",
        filters::Union{Dict{String,String},Dict{String,Array{String}}}=Dict("datafmt" => "STD", "indfmt" => "INDL", "consol" => "C", "popsrc" => "D"),
        columns::Array{String}=["gvkey", "datadate", "fyear", "sale", "revt", "xopr"])
    
    if typeof(dateStart) == Int
        dateStart = Dates.Date(dateStart, 1, 1)
    end
    if typeof(dateEnd) == Int
        dateEnd = Dates.Date(dateEnd, 12, 31)
    end

    colString = createColString(columns)
    filterString = createFilter(filters)
    query = """
        select $colString
        from comp.$fund
        where datadate between '$dateStart' and '$dateEnd' $filterString"""
    
    comp = ODBC.query(dsn, query);
    comp[!, :datadate] = Dates.Date.(comp[:, :datadate])

    return comp
end
