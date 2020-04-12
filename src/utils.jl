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
        if x in names(df) # Removes the column if it would create a duplicate
            select!(crsp, Not(x))
        end
    end
    df = join(df, crsp, on=col, kind=:left)
    df[!, :namedt] = setNewDate(df[:, :namedt], df[:, :date])
    df[!, :nameenddt] = setNewDate(df[:, :nameenddt], df[:, :date])
    df = df[df[:, :namedt] .<= df[:, :date] .<= df[:, :nameenddt], :]
    select!(df, Not([:namedt, :nameenddt]))
    return df
end

function myJoin(df1::DataFrame, df2::DataFrame)
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
    df1 = join(df1, ret, on=:index1, kind=:left)
    select!(df1, Not(:permno))
    df1 = join(df1, df2, on=:index2, kind=:left)
    select!(df1, Not([:index1, :index2]))
    return df1
end