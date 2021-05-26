
function calculate_car(
    df::AbstractDataFrame,
    timeframe::retTimeframe,
    data::AbstractDataFrame,
    market_data::AbstractDataFrame;
    date::String="date",
    idcol::String="permno",
    
)
    if date ∉ names(df)
        throw(ArgumentError("The $date column is not found in the dataframe"))
    end
    if idcol ∉ names(df)
        throw(ArgumentError("The $idcol column is not found in the dataframe"))
    end
    df[!, :dateStart] = calculateDays(df[:, date], timeframe.businessDays[1], timeframe.subtraction, timeframe.monthPeriod)
    df[!, :dateEnd] = calculateDays(df[:, date], timeframe.businessDays[2], timeframe.addition, timeframe.monthPeriod)

    crsp = data
    crspM = market_data


    BusinessDays.initcache(:USNYSE)

    crsp = leftjoin(crsp, crspM, on=:date)
    crsp[!, :car] = crsp[:, :ret] .- crsp[:, Symbol(timeframe.marketReturn)]
    crsp[!, :plus1] = crsp[:, :ret] .+ 1
    crsp[!, :plus1m] = crsp[:, Symbol(timeframe.marketReturn)] .+ 1
    rename!(crsp, :date => :retDate)
    rename!(crsp, Symbol(timeframe.marketReturn) => :retm)

    df = range_join(
        df,
        crsp,
        [idcol],
        [
            (<=, :dateStart, :retDate),
            (>=, :dateEnd, :retDate)
        ]
    )

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
    df = combine(gd, valuecols(gd) .=> sum, valuecols(gd) .=> std)
    df = leftjoin(df, df2, on=aggCols)
    return df
end

function calculate_car(
    df::AbstractDataFrame,
    timeframe::retTimeframe,
    data::ODBC.Connection;
    date::String="date",
    idcol::String="permno"
)
    df = copy(df)
    
    df[!, :dateStart] = calculateDays(df[:, date], timeframe.businessDays[1], timeframe.subtraction, timeframe.monthPeriod)
    df[!, :dateEnd] = calculateDays(df[:, date], timeframe.businessDays[2], timeframe.addition, timeframe.monthPeriod)
    if timeframe.monthPeriod
        stockFile1 = "msf"
        stockFile2 = "msi"
    else
        stockFile1 = "dsf"
        stockFile2 = "dsi"
    end

    crsp = crspData(data, df, stockFile = stockFile1)
    crspM = crspWholeMarket(data,
        stockFile = stockFile2,
        dateStart = minimum(df[:, :dateStart]),
        dateEnd = maximum(df[:, :dateEnd]),
        col = timeframe.marketReturn)
    return calculate_car(df, timeframe, crsp, crspM; date=date, idcol=idcol)
end



function calculate_car(
    df::AbstractDataFrame,
    timeframe::Array{retTimeframe},
    data::AbstractDataFrame,
    market_data::AbstractDataFrame;
    date::String="date",
    idcol::String="permno"
)


    df = df[:, :]

    dfAll = DataFrame()

    for x in timeframe
        df[!, :name] .= nameConvention(x)
        if size(dfAll, 1) == 0
            dfAll = calculate_car(df, x, data, market_data, date=date, idcol=idcol)
        else
            dfAll = vcat(dfAll, calculate_car(df, x, data, market_data, date=date, idcol=idcol))
        end
    end
    return dfAll
end

function calculate_car(
    df::AbstractDataFrame,
    timeframe::Array{retTimeframe},
    data::ODBC.Connection;
    date::String="date",
    idcol::String="permno"
)
    
    df = df[:, :]
    df_temp = DataFrame()
    stockFile1 = "msf"
    stockFile2 = "msi"
    market_ret = String[]
    for x in timeframe
        df[!, :dateStart] = calculateDays(df[:, date], timeframe.businessDays[1], timeframe.subtraction, timeframe.monthPeriod)
        df[!, :dateEnd] = calculateDays(df[:, date], timeframe.businessDays[2], timeframe.addition, timeframe.monthPeriod)
        if nrow(df_temp) == 0
            df_temp = df[:, [idcol, "dateStart", "dateEnd"]]
        else
            df_temp = vcat(df_temp, df[:, [idcol, "dateStart", "dateEnd"]])
        end
        if !x.monthPeriod # if any of the time periods are not monthly, download the daily file
            stockFile1 = "dsf"
            stockFile2 = "dsi"
        end
        push!(market_ret, x.marketReturn)
    end
    gdf = groupby(df_temp, idcol)
    df_temp = combine(gdf, "dateStart" => minimum => "dateStart", "dateEnd" => maximum => "dateEnd")

    crsp = crspData(data, df_temp, stockFile = stockFile1)
    crspM = crspWholeMarket(data,
        stockFile = stockFile2,
        dateStart = minimum(df_temp[:, :dateStart]),
        dateEnd = maximum(df_temp[:, :dateEnd]),
        col = unique(market_ret)
    )

    return calculate_car(df, timeframe, crsp, crspM; date=date, idcol=idcol)

end

