struct retTimeframe
    subtraction::Int
    addition::Int
    businessDays::Tuple{Bool, Bool}
    monthPeriod::Bool
    marketReturn::String
end

struct ffMethod
    subtraction::Int
    addition::Int
    maxRelativeToMin::Bool
    businessDays::Tuple{Bool, Bool}
    monthPeriod::Tuple{Bool, Bool}
    rf::Symbol
    funSymbols::Array{Symbol}
    dfData::DataFrame
    minObs::Int
end

function setFFMethod(
    subtraction::Int,
    addition::Int,
    df::DataFrame;
    maxRelativeToMin::Bool = true,
    businessDays::Union{Bool, Tuple{Bool, Bool}} = false,
    monthPeriod::Union{Bool, Tuple{Bool, Bool}} = false,
    rf::Symbol = :rf,
    funSymbols::Array{Symbol} = [:mktrf, :smb, :hml],
    minObs::Int = 15
    )
    if typeof(businessDays) == Bool
        businessDays = (businessDays, businessDays)
    end
    if typeof(monthPeriod) == Bool
        monthPeriod = (monthPeriod, monthPeriod)
    end
    return ffMethod(subtraction, addition, maxRelativeToMin, businessDays, monthPeriod, rf, funSymbols, df, minObs)
end

function calculateDays(dates::Array{Date},
    businessDays::Bool,
    change::Int,
    month::Bool;
    includeFirstBDay::Bool = false
)
    newDates = Date[]
    BusinessDays.initcache(:USNYSE)
    if businessDays
        if month
            dates = Dates.lastdayofmonth.(dates)
            newDates = dates .+ Dates.Month(change)
            newDates = tobday.(:USNYSE, newDates, forward=false)
        else
            newDates = advancebdays.(:USNYSE, dates, change - includeFirstBDay)
        end
    else
        if month
            dates = Dates.lastdayofmonth.(dates)
            newDates = dates .+ Dates.Month(change)
        else
            newDates = dates .+ Dates.Day(change)
        end
    end
    return newDates
end

function setRetTimeframe(subtraction,
    addition;
    businessDays::Union{Bool, Tuple{Bool, Bool}} = true,
    monthPeriod::Bool = false,
    marketReturn::String = "vwretd")
    if typeof(businessDays) == Bool
        businessDays = (businessDays, businessDays)
    end
    return retTimeframe(subtraction, addition, businessDays, monthPeriod, marketReturn)
end

function nameConvention(timeframe::retTimeframe)
    s = String[]
    push!(s, "CAR")
    if timeframe.monthPeriod
        push!(s, "m")
    end
    if timeframe.businessDays[1]
        push!(s, "b1")
    end
    if timeframe.businessDays[2]
        push!(s, "b2")
    end
    if timeframe.subtraction < 0
        push!(s, "n$(-timeframe.subtraction)")
    else
        push!(s, "$(timeframe.subtraction)")
    end
    if timeframe.addition < 0
        push!(s, "n$(-timeframe.addition)")
    else
        push!(s, "$(timeframe.addition)")
    end
    push!(s, timeframe.marketReturn)
    return join(s, "_")
end

function setNewDate(col1, col2)
    ret = Date[]
    for i in 1:length(col1)
        if typeof(col1[i]) == Missing
            push!(ret, col2[i])
        else
            push!(ret, col1[i])
        end
    end
    return ret
end

function getCrspNames(dsn, df, col, ignore)
    permno::Array{<:Number} = Int[]
    ncusip::Array{String} = String[]
    cusip::Array{String} = String[]
    if col == :permno
        permno = df[:, :permno]
    elseif col == :cusip
        cusip = df[:, :cusip]
    else
        ncusip = df[:, :ncusip]
    end
    crsp = unique(crspStocknames(dsn, permno=permno, cusip=cusip, ncusip=ncusip, cols=["permno", "ncusip", "cusip", "namedt", "nameenddt"]))
    for x in ignore
        if String(x) in names(df) # Removes the column if it would create a duplicate
            select!(crsp, Not(x))
        end
    end
    df = leftjoin(df, crsp, on=col)
    df[!, :namedt] = setNewDate(df[:, :namedt], df[:, :date])
    df[!, :nameenddt] = setNewDate(df[:, :nameenddt], df[:, :date])
    df = df[df[:, :namedt] .<= df[:, :date] .<= df[:, :nameenddt], :]
    select!(df, Not([:namedt, :nameenddt]))
    return df
end

function myJoin(df1::DataFrame, df2::DataFrame)
    CSV.write("G:\\My Drive\\python\\tests\\carMerge1.csv", df1)
    CSV.write("G:\\My Drive\\python\\tests\\carMerge2.csv", df2)
    t = ndsparse(
        (permno = df2[:, :permno], retDate = df2[:, :retDate]),
        (index = 1:size(df2, 1),),
    )
    ret = DataFrame(index1 = Int[], index2 = Int[])
    for i = 1:size(df1, 1)
        res = t[
            df1[i, :permno],
            collect(df1[i, :dateStart]:Day(1):df1[i, :dateEnd])
        ]
        for v in rows(res)
            push!(ret, (i, v.index))
        end
    end
    df1[!, :index1] = 1:size(df1, 1)
    df2[!, :index2] = 1:size(df2, 1)
    df1 = leftjoin(df1, ret, on=:index1)
    select!(df1, Not(:permno))
    df1 = leftjoin(df1, df2, on=:index2)
    select!(df1, Not([:index1, :index2]))
    return df1
end

function dateRangeJoin(
    df1::AbstractDataFrame,
    df2::AbstractDataFrame;
    on::Union{Array{Symbol}, Array{String}, Array{Union{Symbol, String}}} = Symbol[],
    validate::Bool=false,
    dateColMin::Union{String, Symbol} = "datemin",
    dateColMax::Union{String, Symbol} = "datemax",
    dateColTest::Union{String, Symbol} = "date"
)
    df1 = copy(df1)
    df2 = copy(df2)
    df2[!, :index2] = 1:size(df2, 1)
    gdf2 = groupby(df2, on)
    ret = DataFrame(index1 = Int[], index2 = Int[])
    for i = 1:size(df1, 1)
        temp = get(gdf2, NamedTuple(df1[i, on]), 0)
        if temp == 0
            continue
        end
        temp = temp[df1[i, dateColMin] .<= temp[:, dateColTest] .<= df1[i, dateColMax], :]
        if validate && size(temp, 1) > 1
            throw(ArgumentError("Merge key(s) in df2 are not unique. " *
                            "First duplicate at row $(temp[1, :index2])"))
        end
        for row in eachrow(temp)
            push!(ret, (i, row.index2))
        end
    end
    df1[!, :index1] = 1:size(df1, 1)
    df1 = leftjoin(df1, ret, on=:index1, validate=(true, validate))
    select!(df2, Not(on))
    df1 = leftjoin(df1, df2, on=:index2, validate=(false, validate))
    select!(df1, Not([:index1, :index2]))
    return df1
end