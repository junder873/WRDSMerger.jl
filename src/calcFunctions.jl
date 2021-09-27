
# function calculate_car(
#     df::AbstractDataFrame;
#     date_start::String="dateStart",
#     date_end::String="dateEnd",
#     idcol::String="permno",
#     market_return::String="vwretd",
#     out_cols=[
#         ["ret", "vol", "shrout", "retm", "car"] .=> sum,
#         ["car"] .=> std
#     ],
#     data::Union{LibPQ.Connection, Tuple{DataFrame, DataFrame}}=(DataFrame(), DataFrame())
# )

function calculate_car(
    data::Tuple{AbstractDataFrame, AbstractDataFrame},
    df::AbstractDataFrame;
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

    crsp = data[1]
    crspM = data[2]

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
    data::Tuple{AbstractDataFrame, AbstractDataFrame},
    df::AbstractDataFrame,
    ret_period::Tuple{<:DatePeriod, <:DatePeriod};
    date::String="date",
    idcol::String="permno",
    out_cols=[
        ["ret", "vol", "shrout", "retm", "car"] .=> sum,
        ["car"] .=> std
    ],
    market_return::String="vwretd"
)
    calculate_car(
        data,
        df,
        EventWindow(ret_period);
        date,
        idcol,
        out_cols,
        market_return
    )
end

function calculate_car(
    data::Tuple{AbstractDataFrame, AbstractDataFrame},
    df::AbstractDataFrame,
    ret_period::EventWindow;
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
    df[!, :dateStart] = df[:, date] .+ ret_period.s
    df[!, :dateEnd] = df[:, date] .+ ret_period.e

    return calculate_car(data, df; idcol, out_cols)
end

function calculate_car(
    dsn::Union{LibPQ.Connection, DBInterface.Connection},
    df::AbstractDataFrame,
    ret_period::Tuple{<:DatePeriod, <:DatePeriod};
    date::String="date",
    idcol::String="permno",
    stock_file_daily::Bool=true,
    market_return::String="vwretd"
)
    calculate_car(
        dsn,
        df,
        EventWindow(ret_period);
        date,
        idcol,
        stock_file_daily,
        market_return
    )
end

function calculate_car(
    dsn::Union{LibPQ.Connection, DBInterface.Connection},
    df::AbstractDataFrame,
    ret_period::EventWindow;
    date::String="date",
    idcol::String="permno",
    stock_file_daily::Bool=true,
    market_return::String="vwretd"
)
    df = copy(df)
    
    df[!, :dateStart] = df[:, date] .+ ret_period.s
    df[!, :dateEnd] = df[:, date] .+ ret_period.e
    if !stock_file_daily
        stockFile1 = "msf"
        stockFile2 = "msi"
    else
        stockFile1 = "dsf"
        stockFile2 = "dsi"
    end

    crsp = crsp_data(dsn, df, stock_file = stockFile1)
    crspM = crsp_market(
        dsn,
        stock_file = stockFile2,
        dateStart = minimum(df[:, :dateStart]),
        dateEnd = maximum(df[:, :dateEnd]),
        col = market_return
    )
    return calculate_car((crsp, crspM), df, ret_period; date=date, idcol=idcol, market_return=market_return)
end

function calculate_car(
    dsn::Union{LibPQ.Connection, DBInterface.Connection},
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

    crsp = crsp_data(dsn, df; stock_file = stockFile1, date_start, date_end)
    crspM = crsp_market(
        dsn,
        stock_file = stockFile2,
        dateStart = minimum(df[:, date_start]),
        dateEnd = maximum(df[:, date_end]),
        col = marketReturn
    )
    return calculate_car((crsp, crspM), df; date_start, date_end, idcol)
end



function calculate_car(
    data::Tuple{AbstractDataFrame, AbstractDataFrame},
    df::AbstractDataFrame,
    ret_periods::Vector{EventWindow};
    date::String="date",
    idcol::String="permno",
    market_return::String="vwretd"
)


    df = df[:, :]

    dfAll = DataFrame()

    for ret_period in ret_periods
        df[!, :name] .= repeat([ret_period], nrow(df))
        if size(dfAll, 1) == 0
            dfAll = calculate_car(data, df, ret_period; date=date, idcol=idcol, market_return=market_return)
        else
            dfAll = vcat(dfAll, calculate_car(data, df, ret_period; date=date, idcol=idcol, market_return=market_return))
        end
    end
    return dfAll
end

function calculate_car(
    dsn::Union{LibPQ.Connection, DBInterface.Connection},
    df::AbstractDataFrame,
    ret_periods::Vector{EventWindow};
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
        df[!, :dateStart] = df[:, date] .+ ret_period.s
        df[!, :dateEnd] = df[:, date] .+ ret_period.e
        if nrow(df_temp) == 0
            df_temp = df[:, [idcol, date, "dateStart", "dateEnd"]]
        else
            df_temp = vcat(df_temp, df[:, [idcol, date, "dateStart", "dateEnd"]])
        end
    end
    gdf = groupby(df_temp, [idcol, date])
    df_temp = combine(gdf, "dateStart" => minimum => "dateStart", "dateEnd" => maximum => "dateEnd")

    crsp = crsp_data(dsn, df_temp, stock_file = stockFile1)

    crspM = crsp_market(
        dsn,
        stock_file = stockFile2,
        dateStart = minimum(df_temp[:, :dateStart]),
        dateEnd = maximum(df_temp[:, :dateEnd]),
        col = market_return
    )

    return calculate_car((crsp, crpsM), df, ret_periods; date=date, idcol=idcol, market_return=market_return)

end

function calculate_car(
    data::Tuple{DataFrame, DataFrame},
    df::DataFrame,
    ff_est::FFEstMethod;
    date_start::String="dateStart",
    date_end::String="dateEnd",
    est_window_start::String="est_window_start",
    est_window_end::String="est_window_end",
    idcol::String="permno",
    suppress_warning::Bool=false
)
    crsp_raw = data[1]
    mkt_data = data[2]
    df = copy(df)
    make_ff_est_windows!(df,
        ff_est;
        date_start,
        date_end,
        est_window_start,
        est_window_end,
        suppress_warning
    )
    crsp_raw = leftjoin(
        crsp_raw,
        mkt_data,
        on=:date,
        validate=(false, true)
    )
    ff_est_windows = range_join(
        df,
        crsp_raw,
        [idcol],
        [
            (<=, :_ff_date_start, :date),
            (>=, :_ff_date_end, :date)
        ]
    )

    event_windows = range_join(
        df,
        crsp_raw,
        [idcol],
        [
            (<=, Symbol(date_start), :date),
            (>=, Symbol(date_end), :date)
        ]
    )
    # My understanding is the original Fama French subtracted risk free
    # rate, but it does not appear WRDS does this, so I follow that
    # event_windows[!, :ret_rf] = event_windows.ret .- event_windows[:, :rf]
    # ff_est_windows[!, :ret_rf] = ff_est_windows.ret .- ff_est_windows[:, :rf]

    # I need to dropmissing here since not doing so creates huge problems
    # in the prediction component, where it thinks all of the data
    # is actually categorical in nature
    dropmissing!(event_windows, ff_est.ff_sym)
    gdf_ff = groupby(ff_est_windows, [idcol, est_window_start, est_window_end])
    gdf_event = groupby(event_windows, [idcol, date_start, date_end])

    f = term(:ret) ~ sum(term.(ff_est.ff_sym))
    
    df[!, :car_ff] = Vector{Union{Missing, Float64}}(missing, nrow(df))
    df[!, :bhar_ff] = Vector{Union{Missing, Float64}}(missing, nrow(df))
    df[!, :std_ff] = Vector{Union{Missing, Float64}}(missing, nrow(df))
    df[!, :obs_event] = Vector{Union{Missing, Int}}(missing, nrow(df))
    df[!, :obs_ff] = Vector{Union{Missing, Int}}(missing, nrow(df))

    for i in 1:nrow(df)
        temp_ff = get(
            gdf_ff,
            (df[i, idcol], df[i, est_window_start], df[i, est_window_end]),
            DataFrame()
        )
        temp_event = get(
            gdf_event,
            NamedTuple(df[i, [idcol, date_start, date_end]]),
            DataFrame()
        )
        nrow(temp_event) == 0 && continue
        df[i, :obs_ff] = nrow(temp_ff)
        nrow(temp_ff) < ff_est_first.min_est && continue
        
        rr = reg(temp_ff, f, save=true)
        expected_ret = predict(rr, temp_event)
        df[i, :car_ff] = sum(temp_event.ret .- expected_ret)
        df[i, :std_ff] = sqrt(rr.rss / rr.dof_residual) # similar to std(rr.residuals), corrects for the number of parameters
        df[i, :bhar_ff] = (prod(1 .+ temp_event.ret) .- 1) - (prod(1 .+ expected_ret) .- 1)
        df[i, :obs_event] = nrow(temp_event)
    end
    return df

end

function calculate_car(
    dsn::Union{LibPQ.Connection, DBInterface.Connection},
    df::AbstractDataFrame,
    ff_est::FFEstMethod;
    date_start::String="dateStart",
    date_end::String="dateEnd",
    est_window_start::String="est_window_start",
    est_window_end::String="est_window_end",
    idcol::String="permno",
    suppress_warning::Bool=false
)
    df = copy(df)

    make_ff_est_windows!(df,
        ff_est;
        date_start,
        date_end,
        est_window_start,
        est_window_end,
        suppress_warning
    )
    
    temp = df[:, [idcol, est_window_start, est_window_end]]
    rename!(temp, est_window_start => date_start, est_window_end => date_end)
    temp = vcat(temp, df[:, [idcol, date_start, date_end]]) |> unique

    crsp_raw = crsp_data(dsn, temp; date_start, date_end)
    ff_download = ff_data(
        dsn;
        date_start=minimum(temp[:, date_start]),
        date_end=maximum(temp[:, date_end])
    )
    return car_ff(
        (crsp_raw, ff_download),
        df,
        ff_est;
        date_start,
        date_end,
        idcol,
        suppress_warning=true
    )

end

function calculate_car(
    data::Tuple{DataFrame, DataFrame},
    df::AbstractDataFrame,
    ff_ests::Vector{FFEstMethod};
    date_start::String="dateStart",
    date_end::String="dateEnd",
    est_window_start::String="est_window_start",
    est_window_end::String="est_window_end",
    idcol::String="permno",
    suppress_warning::Bool=false
)
    df = copy(df)
    out = DataFrame()
    for ff_est in ff_ests
        temp = car_ff(
            data,
            df,
            ff_est;
            date_start,
            date_end,
            est_window_start,
            est_window_end,
            idcol,
            suppress_warning
        )
        temp[!, :ff_method] .= ff_est
        if nrow(out) == 0
            out = temp[:, :]
        else
            out = vcat(out, temp)
        end
    end
    return out
end
