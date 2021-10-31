"""
    function calculate_car(
        data::Tuple{AbstractDataFrame, AbstractDataFrame},
        df::AbstractDataFrame;
    )

    function calculate_car(
        data::Tuple{AbstractDataFrame, AbstractDataFrame},
        df::AbstractDataFrame,
        ret_period::Tuple{<:DatePeriod, <:DatePeriod};
    )

    function calculate_car(
        data::Tuple{AbstractDataFrame, AbstractDataFrame},
        df::AbstractDataFrame,
        ret_period::EventWindow;
    )

    function calculate_car(
        conn::Union{LibPQ.Connection, DBInterface.Connection},
        df::AbstractDataFrame,
        ret_period::Tuple{<:DatePeriod, <:DatePeriod};
    )

    function calculate_car(
        conn::Union{LibPQ.Connection, DBInterface.Connection},
        df::AbstractDataFrame,
        ret_period::EventWindow;
    )

    function calculate_car(
        conn::Union{LibPQ.Connection, DBInterface.Connection},
        df::AbstractDataFrame;
    )

    function calculate_car(
        data::Tuple{AbstractDataFrame, AbstractDataFrame},
        df::AbstractDataFrame,
        ret_periods::Vector{EventWindow};
    )

    function calculate_car(
        conn::Union{LibPQ.Connection, DBInterface.Connection},
        df::AbstractDataFrame,
        ret_periods::Vector{EventWindow};
    )

    function calculate_car(
        data::Tuple{DataFrame, DataFrame},
        df::DataFrame,
        ff_est::FFEstMethod;
    )

    function calculate_car(
        conn::Union{LibPQ.Connection, DBInterface.Connection},
        df::AbstractDataFrame,
        ff_est::FFEstMethod;
    )

    function calculate_car(
        data::Tuple{DataFrame, DataFrame},
        df::AbstractDataFrame,
        ff_ests::Vector{FFEstMethod};
    )

Calculates abnormal returns over a period, currently the Fama French and
other methods are very different, with different options.

# Arguments

## Main Arguments

- data: Provides the data source, one of the following:
    - `conn::Union{LibPQ.Connection, DBInterface.Connection}`: A connection to a database
    - `data::Tuple{DataFrame, DataFrame}`: A pair of DataFrames, the first is the stockfiles
      for each individual stock, the second is for the market data
- `df::AbstractDataFrame`: The second argument is a DataFrame, with an `idcol` and either a
  `date` or `date_start` and `date_end` (customizable by kwargs).
    - If `date` is passed, then there needs to be a `ret_periods` needs to also
      be passed, which adjusts the `date` to `date_start` and `date_end`
    - If no `ret_period` or `ff_est` is passed, then `date_start` and `date_end` must
      be in the DataFrame. These are then used as the range over which abnormal returns
      are summed
- `ret_period`: Either an `EventWindow` or `FFEstMethod` type (or a vector of those types)
  which specifies the primary event window and the estimation method for a Fama French abnormal
  return

## Keyword Arguments

- Event Date or Window: 
    - `date::String="date"`: The event date, only in functions where `ret_period` exists
    - `date_start::String="dateStart"` and `date_end::String="dateEnd"` specify the event
      period start and end dates, provides flexibility when the period might not be fixed
- `idcol::String="permno"`: The primary identifying column, only change if downloading
  data not from WRDS


## Keyword Arguments for Non-Fama French Methods

- `market_return::String="vwretd"`: The market return that a simple abnormal return is calculated
  against

```julia
out_cols=[
    ["ret", "vol", "shrout", "retm", "car"] .=> sum,
    ["car", "ret"] .=> std,
    ["ret", "retm"] => ((ret, retm) -> bhar=bhar_calc(ret, retm)) => "bhar",
    ["ret"] .=> buy_hold_return .=> ["bh_return"]
]
```
This flexible calculation is used in a `combine` function to calculate various statistics
over the event window.

## Keyword Arguments for Fama french Methods

The following (along with `date_start`, `date_end`, and an FFEstMethod) are passed to the
function `make_ff_est_windows!` in order to properly format the DataFrame for use:
- est_window_start::String="est_window_start"
- est_window_end::String="est_window_end"
- suppress_warning::Bool=false

"""
function calculate_car(
    data::Tuple{AbstractDataFrame, AbstractDataFrame},
    df::AbstractDataFrame;
    date_start::String="dateStart",
    date_end::String="dateEnd",
    idcol::String="permno",
    market_return::String="vwretd",
    out_cols=[
        ["ret", "vol", "shrout", "retm", "car"] .=> sum,
        ["car", "ret"] .=> std,
        ["ret", "retm"] => ((ret, retm) -> bhar=bhar_calc(ret, retm)) => "bhar",
        ["ret"] .=> buy_hold_return .=> ["bh_return"]
    ]
)
    df = copy(df)
    BusinessDays.initcache(:USNYSE)
    df[!, :businessDays] = bdayscount(:USNYSE, df[:, date_start], df[:, date_end]) .+ 1

    aggCols = names(df)

    crsp = data[1]
    crspM = data[2]

    crsp = leftjoin(crsp, crspM, on=:date)
    crsp[!, :car] = crsp[:, :ret] .- crsp[:, market_return]
    # crsp[!, :plus1] = crsp[:, :ret] .+ 1
    # crsp[!, :plus1m] = crsp[:, market_return] .+ 1
    rename!(crsp, :date => :retDate)
    rename!(crsp, market_return => "retm")

    df = range_join(
        df,
        crsp,
        [idcol],
        [
            Condition(<=, date_start, :retDate),
            Condition(>=, date_end, :retDate)
        ]
    )

    
    gd = groupby(df, aggCols)
    df = combine(gd, out_cols...)

    return df
end

function calculate_car(
    data::Tuple{AbstractDataFrame, AbstractDataFrame},
    df::AbstractDataFrame,
    ret_period::Tuple{<:DatePeriod, <:DatePeriod};
    date::String="date",
    idcol::String="permno",
    market_return::String="vwretd",
    out_cols=[
        ["ret", "vol", "shrout", "retm", "car"] .=> sum,
        ["car", "ret"] .=> std,
        ["ret", "retm"] => ((ret, retm) -> bhar=bhar_calc(ret, retm)) => "bhar",
        ["ret"] .=> buy_hold_return .=> ["bh_return"]
    ]
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
    market_return::String="vwretd",
    out_cols=[
        ["ret", "vol", "shrout", "retm", "car"] .=> sum,
        ["car", "ret"] .=> std,
        ["ret", "retm"] => ((ret, retm) -> bhar=bhar_calc(ret, retm)) => "bhar",
        ["ret"] .=> buy_hold_return .=> ["bh_return"]
    ]
)
    df = copy(df)
    if date ∉ names(df)
        throw(ArgumentError("The $date column is not found in the dataframe"))
    end
    if idcol ∉ names(df)
        throw(ArgumentError("The $idcol column is not found in the dataframe"))
    end
    df[!, :dateStart] = df[:, date] .+ ret_period.s
    df[!, :dateEnd] = df[:, date] .+ ret_period.e

    return calculate_car(data, df; idcol, out_cols, market_return)
end

function calculate_car(
    conn::Union{LibPQ.Connection, DBInterface.Connection},
    df::AbstractDataFrame,
    ret_period::Tuple{<:DatePeriod, <:DatePeriod};
    date::String="date",
    idcol::String="permno",
    market_return::String="vwretd",
    out_cols=[
        ["ret", "vol", "shrout", "retm", "car"] .=> sum,
        ["car", "ret"] .=> std,
        ["ret", "retm"] => ((ret, retm) -> bhar=bhar_calc(ret, retm)) => "bhar",
        ["ret"] .=> buy_hold_return .=> ["bh_return"]
    ]
)
    calculate_car(
        conn,
        df,
        EventWindow(ret_period);
        date,
        idcol,
        market_return,
        out_cols
    )
end

function calculate_car(
    conn::Union{LibPQ.Connection, DBInterface.Connection},
    df::AbstractDataFrame,
    ret_period::EventWindow;
    date::String="date",
    idcol::String="permno",
    market_return::String="vwretd",
    out_cols=[
        ["ret", "vol", "shrout", "retm", "car"] .=> sum,
        ["car", "ret"] .=> std,
        ["ret", "retm"] => ((ret, retm) -> bhar=bhar_calc(ret, retm)) => "bhar",
        ["ret"] .=> buy_hold_return .=> ["bh_return"]
    ]
)
    df = copy(df)
    
    df[!, :dateStart] = df[:, date] .+ ret_period.s
    df[!, :dateEnd] = df[:, date] .+ ret_period.e

    return calculate_car(
        conn,
        df;
        date_start="dateStart",
        date_end="dateEnd",
        idcol,
        market_return,
        out_cols
    )
end

function calculate_car(
    conn::Union{LibPQ.Connection, DBInterface.Connection},
    df::AbstractDataFrame;
    date_start::String="dateStart",
    date_end::String="dateEnd",
    idcol::String="permno",
    market_return::String="vwretd",
    out_cols=[
        ["ret", "vol", "shrout", "retm", "car"] .=> sum,
        ["car", "ret"] .=> std,
        ["ret", "retm"] => ((ret, retm) -> bhar=bhar_calc(ret, retm)) => "bhar",
        ["ret"] .=> buy_hold_return .=> ["bh_return"]
    ]
)
    df = copy(df)


    crsp = crsp_data(conn, df; date_start, date_end)
    crspM = crsp_market(
        conn,
        minimum(df[:, date_start]),
        maximum(df[:, date_end]);
        cols=market_return
    )
    return calculate_car(
        (crsp, crspM),
        df;
        date_start,
        date_end,
        idcol,
        market_return,
        out_cols
    )
end



function calculate_car(
    data::Tuple{AbstractDataFrame, AbstractDataFrame},
    df::AbstractDataFrame,
    ret_periods::Vector{EventWindow};
    date::String="date",
    idcol::String="permno",
    market_return::String="vwretd",
    out_cols=[
        ["ret", "vol", "shrout", "retm", "car"] .=> sum,
        ["car", "ret"] .=> std,
        ["ret", "retm"] => ((ret, retm) -> bhar=bhar_calc(ret, retm)) => "bhar",
        ["ret"] .=> buy_hold_return .=> ["bh_return"]
    ]
)


    df = copy(df)

    dfAll = DataFrame()

    for ret_period in ret_periods
        df[!, :name] .= repeat([ret_period], nrow(df))
        if size(dfAll, 1) == 0
            dfAll = calculate_car(data, df, ret_period; date, idcol, market_return, out_cols)
        else
            dfAll = vcat(dfAll, calculate_car(data, df, ret_period; date, idcol, market_return, out_cols))
        end
    end
    return dfAll
end

function calculate_car(
    conn::Union{LibPQ.Connection, DBInterface.Connection},
    df::AbstractDataFrame,
    ret_periods::Vector{EventWindow};
    date::String="date",
    idcol::String="permno",
    market_return::String="vwretd",
    out_cols=[
        ["ret", "vol", "shrout", "retm", "car"] .=> sum,
        ["car", "ret"] .=> std,
        ["ret", "retm"] => ((ret, retm) -> bhar=bhar_calc(ret, retm)) => "bhar",
        ["ret"] .=> buy_hold_return .=> ["bh_return"]
    ]
)
    
    df = copy(df)
    df_temp = DataFrame()

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

    crsp = crsp_data(conn, df_temp)

    crspM = crsp_market(
        conn,
        minimum(df_temp[:, :dateStart]),
        maximum(df_temp[:, :dateEnd]);
        cols=market_return,
    )

    return calculate_car(
        (crsp, crspM),
        df,
        ret_periods;
        date,
        idcol,
        market_return,
        out_cols
    )

end

function calculate_car(
    data::Tuple{DataFrame, DataFrame},
    df::DataFrame,
    ff_est::FFEstMethod;
    date_start::String="dateStart",
    date_end::String="dateEnd",
    date::String="date",
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
        suppress_warning,
        date
    )
    crsp_raw = leftjoin(
        crsp_raw,
        mkt_data,
        on=:date,
        validate=(false, true)
    )

    # My understanding is the original Fama French subtracted risk free
    # rate, but it does not appear WRDS does this, so I follow that
    # event_windows[!, :ret_rf] = event_windows.ret .- event_windows[:, :rf]
    # ff_est_windows[!, :ret_rf] = ff_est_windows.ret .- ff_est_windows[:, :rf]

    # I need to dropmissing here since not doing so creates huge problems
    # in the prediction component, where it thinks all of the data
    # is actually categorical in nature
    rename!(crsp_raw, "date" => "return_date")
    dropmissing!(crsp_raw, vcat([:ret], ff_est.ff_sym))
    gdf_crsp = groupby(crsp_raw, idcol)

    f = term(:ret) ~ sum(term.(ff_est.ff_sym))
    
    df[!, :car_ff] = Vector{Union{Missing, Float64}}(missing, nrow(df))
    df[!, :bhar_ff] = Vector{Union{Missing, Float64}}(missing, nrow(df))
    df[!, :std_ff] = Vector{Union{Missing, Float64}}(missing, nrow(df))
    df[!, :obs_event] = Vector{Union{Missing, Int}}(missing, nrow(df))
    df[!, :obs_ff] = Vector{Union{Missing, Int}}(missing, nrow(df))
    # for some reason, threading here drastically slows things down
    # on bigger datasets, for small sets it is sometimes faster
    # the difference primarily is in garbage collection, with large numbers
    # it alwasy ends up spending a ton of time garbage collecting
    for i in 1:nrow(df)
        temp = get(
            gdf_crsp,
            Tuple(df[i, idcol]),
            DataFrame()
        )
        nrow(temp) == 0 && continue
        fil_ff = filter_data(
            df[i, :],
            temp,
            [
                Condition(<=, est_window_start, "return_date"),
                Condition(>=, est_window_end, "return_date")
            ]
        )
        df[i, :obs_ff] = sum(fil_ff)
        sum(fil_ff) < ff_est.min_est && continue
        #temp_ff = temp[temp_ff, :]
        fil_event = filter_data(
            df[i, :],
            temp,
            [
                Condition(<=, date_start, "return_date"),
                Condition(>=, date_end, "return_date")
            ]
        )
        temp_event = temp[fil_event, :]
 
        nrow(temp_event) == 0 && continue

        rr = reg(temp[fil_ff, :], f)
        expected_ret = predict(rr, temp_event)
        df[i, :car_ff] = sum(temp_event.ret .- expected_ret)
        df[i, :std_ff] = sqrt(rr.rss / rr.dof_residual) # similar to std(rr.residuals), corrects for the number of parameters
        df[i, :bhar_ff] = bhar_calc(temp_event.ret, expected_ret)
        df[i, :obs_event] = nrow(temp_event)

    end
    return df

end

function calculate_car(
    conn::Union{LibPQ.Connection, DBInterface.Connection},
    df::AbstractDataFrame,
    ff_est::FFEstMethod;
    date_start::String="dateStart",
    date_end::String="dateEnd",
    date::String="date",
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
        suppress_warning,
        date
    )
    
    temp = df[:, [idcol, est_window_start, est_window_end]]
    rename!(temp, est_window_start => date_start, est_window_end => date_end)
    temp = vcat(temp, df[:, [idcol, date_start, date_end]]) |> unique

    crsp_raw = crsp_data(conn, temp; date_start, date_end)
    ff_download = ff_data(
        conn;
        date_start=minimum(temp[:, date_start]),
        date_end=maximum(temp[:, date_end])
    )
    return calculate_car(
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
    date::String="date",
    est_window_start::String="est_window_start",
    est_window_end::String="est_window_end",
    idcol::String="permno",
    suppress_warning::Bool=false
)
    df = copy(df)
    out = DataFrame()
    for ff_est in ff_ests
        temp = calculate_car(
            data,
            df,
            ff_est;
            date_start,
            date_end,
            est_window_start,
            est_window_end,
            idcol,
            date,
            suppress_warning
        )
        temp[!, :ff_method] .= repeat([ff_est], nrow(temp))
        if nrow(out) == 0
            out = temp[:, :]
        else
            out = vcat(out, temp)
        end
    end
    return out
end
