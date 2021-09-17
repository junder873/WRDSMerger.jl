
function calculate_car(
    df::AbstractDataFrame,
    data::AbstractDataFrame,
    market_data::AbstractDataFrame;
    date_start::String="dateStart",
    date_end::String="dateEnd",
    idcol::String="permno",
    market_return::String="vwretd",
    out_cols=[
        ["ret", "vol", "shrout", "retm", "car"] .=> sum,
        ["car"] .=> std
    ]
)
    df = df[:, :]
    BusinessDays.initcache(:USNYSE)
    df[!, :businessDays] = bdayscount(:USNYSE, df[:, date_start], df[:, date_end]) .+ 1

    aggCols = names(df)

    crsp = data
    crspM = market_data

    crsp = leftjoin(crsp, crspM, on=:date)
    crsp[!, :car] = crsp[:, :ret] .- crsp[:, market_return]
    crsp[!, :plus1] = crsp[:, :ret] .+ 1
    crsp[!, :plus1m] = crsp[:, market_return] .+ 1
    rename!(crsp, :date => :retDate)
    rename!(crsp, market_return => "retm")

    df = range_join(
        df,
        crsp,
        [idcol],
        [
            (<=, Symbol(date_start), :retDate),
            (>=, Symbol(date_end), :retDate)
        ]
    )

    
    # select!(df, Not([date_start, date_end, "retDate"]))
    # aggCols = [idcol, date, "businessDays"]
    # if "name" in names(df)
    #     push!(aggCols, "name")
    # end
    gd = groupby(df[:, vcat(aggCols, ["plus1", "plus1m"])], aggCols)
    df2 = combine(gd, valuecols(gd) .=> prod)
    df2[!, :bhar] = df2[:, :plus1_prod] .- df2[:, :plus1m_prod]
    select!(df2, Not([:plus1_prod, :plus1m_prod]))
    select!(df, Not([:plus1, :plus1m]))
    gd = groupby(df, aggCols)
    df = combine(gd, out_cols...)
    df = leftjoin(df, df2, on=aggCols)
    return df
end

function calculate_car(
    df::AbstractDataFrame,
    ret_period::Tuple{DatePeriod, DatePeriod},
    data::AbstractDataFrame,
    market_data::AbstractDataFrame;
    date::String="date",
    idcol::String="permno",
    out_cols=[
        ["ret", "vol", "shrout", "retm", "car"] .=> sum,
        ["car"] .=> std
    ],
    market_return::String="vwretd"
)
    if date ∉ names(df)
        throw(ArgumentError("The $date column is not found in the dataframe"))
    end
    if idcol ∉ names(df)
        throw(ArgumentError("The $idcol column is not found in the dataframe"))
    end
    df[!, :dateStart] = df[:, date] + ret_period[1]
    df[!, :dateEnd] = df[:, date] + ret_period[2]

    return calculate_car(df, data, market_data; idcol, out_cols)
end

function calculate_car(
    dsn::LibPQ.Connection,
    df::AbstractDataFrame,
    ret_period::Tuple{DatePeriod, DatePeriod};
    date::String="date",
    idcol::String="permno",
    stock_file_daily::Bool=true,
    market_return::String="vwretd"
)
    df = copy(df)
    
    df[!, :dateStart] = df[:, date] + ret_period[1]
    df[!, :dateEnd] = df[:, date] + ret_period[2]
    if !stock_file_daily
        stockFile1 = "msf"
        stockFile2 = "msi"
    else
        stockFile1 = "dsf"
        stockFile2 = "dsi"
    end

    crsp = crsp_data(dsn, df, stockFile = stockFile1)
    crspM = crsp_market(
        dsn,
        stockFile = stockFile2,
        dateStart = minimum(df[:, :dateStart]),
        dateEnd = maximum(df[:, :dateEnd]),
        col = market_return
    )
    return calculate_car(df, ret_start, ret_end, crsp, crspM; date=date, idcol=idcol, market_return=market_return)
end

function calculate_car(
    dsn::LibPQ.Connection,
    df::AbstractDataFrame;
    date_start::String="dateStart",
    date_end::String="dateEnd",
    idcol::String="permno",
    stock_file_daily::Bool=true,
    marketReturn::String = "vwretd"
)
    df = copy(df)

    if !stock_file_daily
        stockFile1 = "msf"
        stockFile2 = "msi"
    else
        stockFile1 = "dsf"
        stockFile2 = "dsi"
    end

    crsp = crsp_data(dsn, df; stockFile = stockFile1, date_start, date_end)
    crspM = crsp_market(
        dsn,
        stockFile = stockFile2,
        dateStart = minimum(df[:, date_start]),
        dateEnd = maximum(df[:, date_end]),
        col = marketReturn
    )
    return calculate_car(df, crsp, crspM; date_start, date_end, idcol)
end



function calculate_car(
    df::AbstractDataFrame,
    ret_periods::Array{Tuple{DatePeriod, DatePeriod}},
    data::AbstractDataFrame,
    market_data::AbstractDataFrame;
    date::String="date",
    idcol::String="permno",
    market_return::String="vwretd"
)


    df = df[:, :]

    dfAll = DataFrame()

    for ret_period in ret_periods
        df[!, :name] .= repeat([ret_period], nrow(df))
        if size(dfAll, 1) == 0
            dfAll = calculate_car(df, ret_period, data, market_data, date=date, idcol=idcol, market_return=market_return)
        else
            dfAll = vcat(dfAll, calculate_car(df, ret_period, data, market_data, date=date, idcol=idcol, market_return=market_return))
        end
    end
    return dfAll
end

function calculate_car(
    dsn::LibPQ.Connection,
    df::AbstractDataFrame,
    ret_periods::Array{Tuple{DatePeriod, DatePeriod}};
    date::String="date",
    idcol::String="permno",
    stock_file_daily::Bool=true,
    market_return::String="vwretd",
)
    
    df = df[:, :]
    df_temp = DataFrame()
    if !stock_file_daily
        stockFile1 = "msf"
        stockFile2 = "msi"
    else
        stockFile1 = "dsf"
        stockFile2 = "dsi"
    end
    for ret_period in ret_periods
        df[!, :dateStart] = df[:, date] + ret_period[1]
        df[!, :dateEnd] = df[:, date] + ret_period[2]
        if nrow(df_temp) == 0
            df_temp = df[:, [idcol, date, "dateStart", "dateEnd"]]
        else
            df_temp = vcat(df_temp, df[:, [idcol, date, "dateStart", "dateEnd"]])
        end
    end
    gdf = groupby(df_temp, [idcol, date])
    df_temp = combine(gdf, "dateStart" => minimum => "dateStart", "dateEnd" => maximum => "dateEnd")

    crsp = crsp_data(dsn, df_temp, stockFile = stockFile1)

    crspM = crsp_market(
        dsn,
        stockFile = stockFile2,
        dateStart = minimum(df_temp[:, :dateStart]),
        dateEnd = maximum(df_temp[:, :dateEnd]),
        col = market_return
    )

    return calculate_car(df, ret_periods, crsp, crspM; date=date, idcol=idcol, market_return=market_return)

end

