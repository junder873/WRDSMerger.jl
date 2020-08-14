function calculateCARsingle(dsn,
    df::DataFrame,
    timeframe::retTimeframe
    )
    df[!, :dateStart] = calculateDays(df[:, :date], timeframe.businessDays[1], timeframe.subtraction, timeframe.monthPeriod)
    df[!, :dateEnd] = calculateDays(df[:, :date], timeframe.businessDays[2], timeframe.addition, timeframe.monthPeriod)
    if timeframe.monthPeriod
        stockFile1 = "msf"
        stockFile2 = "msi"
    else
        stockFile1 = "dsf"
        stockFile2 = "dsi"
    end
    crsp = crspData(dsn, df, stockFile = stockFile1)

    crspM = crspWholeMarket(dsn,
        stockFile = stockFile2,
        dateStart = minimum(df[:, :dateStart]),
        dateEnd = maximum(df[:, :dateEnd]),
        col = timeframe.marketReturn)

    crsp = leftjoin(crsp, crspM, on=:date)
    crsp[!, :car] = crsp[:, :ret] .- crsp[:, Symbol(timeframe.marketReturn)]
    crsp[!, :plus1] = crsp[:, :ret] .+ 1
    crsp[!, :plus1m] = crsp[:, Symbol(timeframe.marketReturn)] .+ 1
    rename!(crsp, :date => :retDate)
    rename!(crsp, Symbol(timeframe.marketReturn) => :retm)

    df = dateRangeJoin(df, crsp, on=[:permno], dateColMin="dateStart", dateColMax="dateEnd", dateColTest="retDate")

    #df = join(df, crsp, on=:permno, kind=:left)

    #df = df[.&(df[:, :dateStart] .<= df[:, :retDate], df[:, :dateEnd] .>= df[:, :retDate]), :]
    df[!, :businessDays] = bdayscount(:USNYSE, df[:, :dateStart], df[:, :dateEnd]) .+ 1
    select!(df, Not([:dateStart, :dateEnd, :retDate]))
    aggCols = [:permno, :cusip, :date, :businessDays]
    if "name" in names(df)
        push!(aggCols, :name)
    end
    gd = groupby(df[:, vcat(aggCols, [:plus1, :plus1m])], aggCols)
    df2 = combine(gd, valuecols(gd) .=> prod)
    df2[!, :bhar] = df2[:, :plus1_prod] .- df2[:, :plus1m_prod]
    select!(df2, Not([:plus1_prod, :plus1m_prod]))
    select!(df, Not([:plus1, :plus1m]))
    gd = groupby(df, aggCols)
    df = combine(gd, valuecols(gd) .=> sum)
    df = leftjoin(df, df2, on=aggCols)
    return df
end

function calculate_car_single(
    df::AbstractDataFrame,
    timeframe::retTimeframe;
    data::Union{ODBC.Connection, AbstractDataFrame}=DataFrame(),
    date::String="date",
    idcol::String="permno",
    market_data::AbstractDataFrame=DataFrame()
    )
    df[!, :dateStart] = calculateDays(df[:, date], timeframe.businessDays[1], timeframe.subtraction, timeframe.monthPeriod)
    df[!, :dateEnd] = calculateDays(df[:, date], timeframe.businessDays[2], timeframe.addition, timeframe.monthPeriod)
    if timeframe.monthPeriod
        stockFile1 = "msf"
        stockFile2 = "msi"
    else
        stockFile1 = "dsf"
        stockFile2 = "dsi"
    end
    if typeof(data) <: ODBC.Connection
        crsp = crspData(data, df, stockFile = stockFile1)
        crspM = crspWholeMarket(data,
            stockFile = stockFile2,
            dateStart = minimum(df[:, :dateStart]),
            dateEnd = maximum(df[:, :dateEnd]),
            col = timeframe.marketReturn)
    else
        crsp = data
        crspM = market_data
    end

    BusinessDays.initcache(:USNYSE)

    crsp = leftjoin(crsp, crspM, on=:date)
    crsp[!, :car] = crsp[:, :ret] .- crsp[:, Symbol(timeframe.marketReturn)]
    crsp[!, :plus1] = crsp[:, :ret] .+ 1
    crsp[!, :plus1m] = crsp[:, Symbol(timeframe.marketReturn)] .+ 1
    rename!(crsp, :date => :retDate)
    rename!(crsp, Symbol(timeframe.marketReturn) => :retm)

    df = dateRangeJoin(df, crsp, on=[idcol], dateColMin="dateStart", dateColMax="dateEnd", dateColTest="retDate")

    #df = join(df, crsp, on=:permno, kind=:left)

    #df = df[.&(df[:, :dateStart] .<= df[:, :retDate], df[:, :dateEnd] .>= df[:, :retDate]), :]
    df[!, :businessDays] = bdayscount(:USNYSE, df[:, :dateStart], df[:, :dateEnd]) .+ 1
    select!(df, Not([:dateStart, :dateEnd, :retDate]))
    aggCols = [idcol, date, "businessDays"]
    if "name" in names(df)
        push!(aggCols, "name")
    end
    gd = groupby(df[:, vcat(aggCols, ["plus1", "plus1m"])], aggCols)
    df2 = combine(gd, valuecols(gd) .=> prod)
    df2[!, :bhar] = df2[:, :plus1_prod] .- df2[:, :plus1m_prod]
    select!(df2, Not([:plus1_prod, :plus1m_prod]))
    select!(df, Not([:plus1, :plus1m]))
    gd = groupby(df, aggCols)
    df = combine(gd, valuecols(gd) .=> sum)
    df = leftjoin(df, df2, on=aggCols)
    return df
end

function calculateCAR(dsn,
    df::DataFrame,
    timeframes::Union{Array{retTimeframe},retTimeframe}
    )
    for col in ["date"]
        if col ∉ names(df)
            println("$col must be in the DataFrame")
            return 0
        end
    end
    if "permno" ∉ names(df) && "cusip" ∉ names(df)
        println("DataFrame must include cusip or permno")
        return 0
    end


    df = df[:, :]

    if "cusip" in names(df) && "permno" ∉ names(df)
        dfNames = crspStocknames(dsn, cusip=unique(df[:, :cusip]), cols=["permno", "cusip"])
        df = leftjoin(df, unique(dfNames[:, [:permno, :cusip]]), on=:cusip)
    elseif "cusip" ∉ names(df) && "permno" in names(df)
        dfNames = crspStocknames(dsn, permno=unique(df[:, :permno]), cols=["permno", "cusip"])
        df = leftjoin(df, unique(dfNames[:, [:permno, :cusip]]), on=:permno)
    end

    if typeof(timeframes) <: Array
        dfAll = DataFrame()
        for timeframe in timeframes
            df[!, :name] .= nameConvention(timeframe)
            if size(dfAll, 1) == 0
                dfAll = calculateCARsingle(dsn, df, timeframe)
            else
                dfAll = vcat(dfAll, calculateCARsingle(dsn, df, timeframe))
            end
        end
        return dfAll
    else
        df = calculateCARsingle(dsn, df, timeframes)
        return df
    end
end

function calculate_car(
    df::AbstractDataFrame,
    timeframes::Union{Array{retTimeframe},retTimeframe};
    data::Union{AbstractDataFrame, ODBC.Connection} = DataFrame(),
    date::String="date",
    idcol::String="permno",
    market_data::AbstractDataFrame=DataFrame()
    )
    if date ∉ names(df)
        throw(ArgumentError("The $date column is not found in the dataframe"))
    end
    if idcol ∉ names(df)
        throw(ArgumentError("The $idcol column is not found in the dataframe"))
    end
    if typeof(data) == ODBC.Connection && idcol != "permno"
        throw(ArgumentError("If connecting to WRDS, the idcol argument must be permno or cusip"))
    end


    df = df[:, :]

    if typeof(timeframes) <: Array
        dfAll = DataFrame()
        for timeframe in timeframes
            df[!, :name] .= nameConvention(timeframe)
            if size(dfAll, 1) == 0
                dfAll = calculate_car_single(df, timeframe, data=data, date=date, idcol=idcol, market_data=market_data)
            else
                dfAll = vcat(dfAll, calculate_car_single(df, timeframe, data=data, date=date, idcol=idcol, market_data=market_data))
            end
        end
        return dfAll
    else
        df = calculate_car_single(df, timeframes, data=data, date=date, idcol=idcol, market_data=market_data)
        return df
    end
end

function calculateCAR(
    dsn,
    df::DataFrame,
    timeframe::retTimeframe,
    method::ffMethod
    )

    df = df[:, :]
    for col in ["date"]
        if col ∉ names(df)
            println("$col must be in the DataFrame")
            return 0
        end
    end
    if "permno" ∉ names(df) && "cusip" ∉ names(df)
        println("DataFrame must include cusip or permno")
        return 0
    end
    if "cusip" in names(df) && "permno" ∉ names(df)
        dfNames = crspStocknames(dsn, cusip=unique(df[:, :cusip]), cols=["permno", "cusip"])
        df = leftjoin(df, unique(dfNames[:, [:permno, :cusip]]), on=:cusip)
    elseif "cusip" ∉ names(df) && "permno" in names(df)
        dfNames = crspStocknames(dsn, permno=unique(df[:, :permno]), cols=["permno", "cusip"])
        df = leftjoin(df, unique(dfNames[:, [:permno, :cusip]]), on=:permno)
    end


    df[!, :dateStart] = calculateDays(df[:, :date], method.businessDays[1], method.subtraction, method.monthPeriod[1])
    if method.maxRelativeToMin
        df[!, :dateEnd] = calculateDays(df[:, :dateStart], method.businessDays[2], method.addition, method.monthPeriod[2], includeFirstBDay=true)
    else
        df[!, :dateEnd] = calculateDays(df[:, :date], method.businessDays[2], method.addition, method.monthPeriod[2])
    end

    dfCrsp = crspData(dsn, df, columns=["ret"])
    dfCrsp = leftjoin(dfCrsp, method.dfData, on=:date)
    dfCrsp[!, :retrf] = dfCrsp[:, :ret] .- dfCrsp[:, method.rf]

    tempSymbols = vcat([:retrf], method.funSymbols)

    crsp = ndsparse(
        (
            permno=dfCrsp[:, :permno],
            date=dfCrsp[:, :date]
        ),
        NamedTuple{tuple(tempSymbols...)}([dfCrsp[:, x] for x in tempSymbols])
    )
    BusinessDays.initcache(:USNYSE)

    abnRet = Union{Float64, Missing}[]
    obs = Int[]
    for i in 1:size(df, 1)
        f = term(:retrf) ~ sum(term.(method.funSymbols))
        x = crsp[df[i, :permno], collect(df[i, :dateStart]:Day(1):df[i, :dateEnd])]
        if length(x) < method.minObs
            push!(obs, length(x))
            push!(abnRet, missing)
            continue
        end
        rr = reg(x, f, save=true)
        push!(obs, rr.nobs)
        index1 = bdayscount(:USNYSE, df[i, :dateStart], df[i, :date]) + isbday(:USNYSE, df[i, :dateStart])
        if timeframe.businessDays[1]
            index1 = index1 + timeframe.subtraction
        else
            index1 = index1 + bdayscount(:USNYSE, df[i, :date], df[i, :date] .+ Dates.Day(timeframe.subtraction))
        end
        index2 = bdayscount(:USNYSE, df[i, :dateStart], df[i, :date]) + isbday(:USNYSE, df[i, :dateStart])
        if timeframe.businessDays[2]
            index2 = index2 + timeframe.addition
        else
            index2 = index2 = bdayscount(:USNYSE, df[i, :date], df[i, :date] .+ Dates.Day(timeframe.addition))
        end
        push!(abnRet, sum(rr.augmentdf[index1:index2, :residuals]))
    end
    df[!, :abnRet] = abnRet
    df[!, :obs] = obs
    return df
end