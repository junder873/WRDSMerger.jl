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

    crsp = join(crsp, crspM, on=:date, kind=:left)
    crsp[!, :car] = crsp[:, :ret] .- crsp[:, Symbol(timeframe.marketReturn)]
    crsp[!, :plus1] = crsp[:, :ret] .+ 1
    crsp[!, :plus1m] = crsp[:, Symbol(timeframe.marketReturn)] .+ 1
    rename!(crsp, :date => :retDate)
    rename!(crsp, Symbol(timeframe.marketReturn) => :retm)

    df = myjoin(df, crsp)

    #df = join(df, crsp, on=:permno, kind=:left)

    #df = df[.&(df[:, :dateStart] .<= df[:, :retDate], df[:, :dateEnd] .>= df[:, :retDate]), :]
    df[!, :businessDays] = bdayscount(:USNYSE, df[:, :dateStart], df[:, :dateEnd]) .+ 1
    select!(df, Not([:dateStart, :dateEnd, :retDate]))
    aggCols = [:permno, :cusip, :date, :businessDays]
    if :name in names(df)
        push!(aggCols, :name)
    end
    df2 = aggregate(df[:, vcat(aggCols, [:plus1, :plus1m])], aggCols, prod)
    df2[!, :bhar] = df2[:, :plus1_prod] .- df2[:, :plus1m_prod]
    select!(df2, Not([:plus1_prod, :plus1m_prod]))
    select!(df, Not([:plus1, :plus1m]))
    df = aggregate(df, aggCols, sum)
    df = join(df, df2, on=aggCols, kind=:left)
    return df
end

function calculateCAR(dsn,
    df::DataFrame,
    timeframes::Union{Array{retTimeframe},retTimeframe}
    )
    for col in [:date]
        if col ∉ names(df)
            println("$(String(col)) must be in the DataFrame")
            return 0
        end
    end
    if :permno ∉ names(df) && :cusip ∉ names(df)
        println("DataFrame must include cusip or permno")
        return 0
    end


    df = copy(df)

    if :cusip in names(df) && :permno ∉ names(df)
        dfNames = crspStocknames(dsn, cusip=unique(df[:, :cusip]), cols=["permno", "cusip"])
        df = join(df, unique(dfNames[:, [:permno, :cusip]]), on=:cusip, kind=:left)
    elseif :cusip ∉ names(df) && :permno in names(df)
        dfNames = crspStocknames(dsn, permno=unique(df[:, :permno]), cols=["permno", "cusip"])
        df = join(df, unique(dfNames[:, [:permno, :cusip]]), on=:permno, kind=:left)
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