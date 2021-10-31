
struct EventWindow
    s::DatePeriod
    e::DatePeriod
end


EventWindow(x::Tuple{DatePeriod, DatePeriod}) = EventWindow(x[1], x[2])

struct FFEstMethod
    estimation_window::Union{Missing, EventWindow}
    gap_to_event::Union{String, DatePeriod}
    min_est::Int
    ff_sym::Vector{Symbol}
    event_window::Union{Missing, EventWindow}
end

function FFEstMethod(
    ;
    estimation_window::Union{Missing, EventWindow}=EventWindow(BDay(-150, :USNYSE), Day(0)),
    gap_to_event::Union{String, DatePeriod}=BDay(-15, :USNYSE),
    min_est::Int=120,
    ff_sym::Vector{Symbol}=[:mktrf, :smb, :hml],
    event_window::Union{Missing, EventWindow}=missing
)
    FFEstMethod(
        estimation_window,
        gap_to_event,
        min_est,
        ff_sym,
        event_window
    )
end

struct FFEstimation
    est_start::Date
    est_end::Date
    event_start::Date
    event_end::Date
    min_est::Int
    ff_sym::Vector{Symbol}
    function FFEstimation(
        est_start,
        est_end,
        event_start,
        event_end,
        min_est,
        ff_sym
    )
        if est_start >= est_end
            error("`est_start` must be before `est_end`")
        end
        if event_start >= event_end
            error("`event_start` must be before `event_end`")
        end
        if countbdays(:USNYSE, est_start, est_end) <= min_est
            x = "The number of business days in the estimation"
            x *= " period is less than or equal to the `min_est` "
            x *= "variable, this will make it hard to estimate "
            x *= "any cases."
            @warn(x)
        end
        new(est_start, est_end, event_start, event_end, min_est, ff_sym)
    end
end


function ff_data(
    conn::Union{LibPQ.Connection, DBInterface.Connection};
    date_start::Date=Date(1926, 7, 1),
    date_end::Date=today(),
    cols::Array{String}=[
        "date",
        "mktrf",
        "smb",
        "hml",
        "rf",
        "umd"
    ]
)
    col_str = join(cols, ", ")
    query = """
        SELECT $col_str FROM $(default_tables["ff_factors"])
        WHERE date BETWEEN '$date_start' AND '$date_end'
    """
    return run_sql_query(conn, query)
end


parse_date(d) = ismissing(d) ? missing : Date(d)

run_sql_query(conn::LibPQ.Connection, q::AbstractString) = LibPQ.execute(conn, q) |> DataFrame
function run_sql_query(
    conn::DBInterface.Connection,
    q::AbstractString;
    date_cols=[
        "date",
        "datadate",
        "namedt",
        "nameenddt",
        "sdate",
        "edate",
        "linkdt",
        "linkenddt"
    ]
)
    temp = DBInterface.execute(conn, q) |> DataFrame
    for col in date_cols
        if col ∈ names(temp)
            temp[!, col] = parse_date.(temp[:, col])
        end
    end
    return temp
end

struct Condition
    fun::Function
    l::Union{Symbol, String}
    r::Union{Symbol, String}
end

function Condition(
    l::Union{Symbol, String},
    fun::Function,
    r::Union{Symbol, String}
)
    Condition(
        fun,
        l,
        r
    )
end

function Condition(
    x::Tuple{Function, Symbol, Symbol}
)
    Condition(
        x[1],
        x[2],
        x[3]
    )
end

function range_join(
    df1::DataFrame,
    df2::DataFrame,
    on,
    conditions::Array{Tuple{Function, Symbol, Symbol}};
    minimize=nothing,
    join_conditions::Union{Array{Symbol}, Symbol}=:and,
    validate::Tuple{Bool, Bool}=(false, false),
    joinfun::Function=leftjoin
)
    jointype = if joinfun == leftjoin
        :left
    elseif joinfun == rightjoin
        :right
    elseif joinfun == innerjoin
        :inner
    else
        :outer
    end
        
    range_join(
        df1,
        df2,
        on,
        Condition.(conditions);
        minimize,
        join_conditions,
        validate=(validate[2], validate[1]),
        jointype
    )
end

function change_function(fun)
    if fun == <=
        return >=
    elseif fun == >=
        return <=
    elseif fun == >
        return <
    elseif fun == <
        return >
    else
        return !fun
    end
end

function filter_data(
    df_row::DataFrameRow,
    df_partial::AbstractDataFrame,
    conditions::Array{Condition};
    join_conditions::Union{Array{Symbol}, Symbol}=:and,
)


    fil = ones(Bool, nrow(df_partial))


    for (j, condition) in enumerate(conditions)
        temp_join = if typeof(join_conditions) <: Symbol
            join_conditions
        elseif j == 1 # if it is the first time through, all the current values are
            :and
        else
            join_conditions[j-1]
        end
        if temp_join == :and
            fil = fil .& broadcast(
                condition.fun,
                df_row[condition.l],
                df_partial[:, condition.r]
            )
        else
            fil = fil .| broadcast(
                condition.fun,
                df_row[condition.l],
                df_partial[:, condition.r]
            )
        end
    end
    return fil
end

"""
I need the keymap returned instead of the full
SubDataFrame
"""
function special_get(gdf, key)
    if haskey(gdf.keymap, key)
        x = gdf.keymap[key]
        return (gdf.starts[x], gdf.ends[x])
    else
        return (0, 0)
    end
end

r_col_names(x::Condition) = x.r
l_col_names(x::Condition) = x.l

function validate_error(df)
    if nrow(df) > 0
        if nrow(df) == 1
            error_message = "df1 contains 1 duplicate key: " *
                            "$(NamedTuple(df[1, :])). "
        elseif nrow(df) == 2
            error_message = "df1 contains 2 duplicate keys: " *
                            "$(NamedTuple(df[1, :])) and " *
                            "$(NamedTuple(df[2, :])). "
        else
            error_message = "df1 contains $(nrow(df)) duplicate keys: " *
                            "$(NamedTuple(df[1, :])), ..., " *
                            "$(NamedTuple(df[end, :])). "
        end
        error(error_message)
    end
end

"""

    function range_join(
        df1::DataFrame,
        df2::DataFrame,
        on,
        conditions::Array{Condition};
        minimize=nothing,
        join_conditions::Union{Array{Symbol}, Symbol}=:and,
        validate::Tuple{Bool, Bool}=(false, false),
        jointype::Symbol=:inner
    )
Joins the dataframes based on a series of conditions, designed
to work with ranges

### Arguments
- `df1::DataFrame`: left DataFrame
- `df2::DataFrame`: right DataFrame
- `on`: either array of column names or matched pairs
- `conditions::Array{Condition}`: array of `Condition`, which specifies the function (<=, >, etc.), left and right columns
- `jointype::Symbol=:inner`: type of join, options are :inner, :outer, :left, and :right
- `minimize`: either `nothing` or an array of column names or matched pairs, minimization will take place in order
- `join_conditions::Union{Array{Symbol}, Symbol}`: defaults to `:and`, otherwise an array of symbols that is 1 less than the length of conditions that the joins will happen in (:or or :and)
- `validate::Tuple{Bool, Bool}`: Whether to validate, this works differently than the equivalent in DataFrames joins,
  here, validate insures that a single row from the dataframe is not duplicated. So validate=(true, false) means that
  there are no duplicated rows from the left dataframe in the result.

### Example

```
df1 = DataFrame(
    firm=1:10,
    date=Date.(2013, 1:10, 1:10)
)

df2 = df1[:, :]
df2[!, :date_low] = df2.date .- Day(2)
df2[!, :date_high] = df2.date .+ Day(2)
select!(df2, Not(:date))

range_join(
    df1,
    df2,
    [:firm],
    [
        (<, :date, :date_high),
        (>, :date, :date_low)
    ],
    join_conditions=[:and]
)
```


"""
function range_join(
    df1::DataFrame,
    df2::DataFrame,
    on,
    conditions::Array{Condition};
    minimize=nothing,
    join_conditions::Union{Array{Symbol}, Symbol}=:and,
    validate::Tuple{Bool, Bool}=(false, false),
    jointype::Symbol=:inner,
)

    if nrow(df1) > nrow(df2) # the fewer main loops this goes through
        # the faster it is overall, if they are about equal
        # this likely slows things down, but it is
        # significantly faster if it is flipped for large sets
        new_cond = Condition[]
        for con in conditions
            push!(new_cond, Condition(change_function(con.fun), con.r, con.l))
        end
        on1, on2 = parse_ons(on)
        if minimize !== nothing
            min1, min2 = parse_ons(minimize)
            new_min = min2 .=> min1
        else
            new_min=nothing
        end
        new_join = if jointype == :left
            new_join = :right
        elseif jointype == :right
            new_join = :left
        else
            new_join = jointype
        end
        return range_join(
            df2,
            df1,
            on2 .=> on1,
            new_cond;
            minimize=new_min,
            join_conditions,
            validate=(validate[2], validate[1]),
            jointype=new_join,
        )
    end

    on1, on2 = WRDSMerger.parse_ons(on)

    res_left = Int[]
    res_right = Int[]

    sizehint!(res_left, max(nrow(df1), nrow(df2)))
    sizehint!(res_right, max(nrow(df1), nrow(df2)))


    gdf = groupby(df2, on2)

    idx = gdf.idx


    # under the current setup, I do not think threading will work here
    for (i, key) in enumerate(Tuple.(copy.(eachrow(df1[:, on1]))))

        s, e = special_get(gdf, key)

        e == 0 && continue


        fil = if join_conditions == :and
            broadcast(
                &,
                [
                    broadcast(
                        condition.fun,
                        df1[i, condition.l],
                        df2[idx[s:e], condition.r]
                    )
                    for condition in conditions
                ]...
            )
        elseif join_conditions == :or
            broadcast(
                |,
                [
                    broadcast(
                        condition.fun,
                        df1[i, condition.l],
                        df2[idx[s:e], condition.r]
                    )
                    for condition in conditions
                ]...
            )
        else
            WRDSMerger.filter_data(
                df1[i, :],
                df2[idx[s:e], :],
                conditions;
                join_conditions
            )
        end

        sum(fil) == 0 && continue
        cur = length(res_left)
        to_grow = sum(fil) + cur
        resize!(res_left, to_grow)
        resize!(res_right, to_grow)
        res_left[cur+1:end] .= i
        @inbounds res_right[cur+1:end] = idx[s:e][fil]


    end
    if minimize !== nothing
        df_temp = hcat(
            df1[res_left, string.(on1) |> unique],
            df2[res_right, string.(on2) |> unique]
        )
        df_temp[!, :_idx_left] = res_left
        df_temp[!, :_idx_right] = res_right
        for (on1, on2) in zip(on1, on2)
            #group_col = x.group_col_left ? :_idx_left : :_idx_right
            group_col = :_idx_left
            df_temp = subset(
                groupby(df_temp, group_col),
                [on1, on2] =>
                (l, r) -> abs.(l .- r) .== minimum(abs.(l .- r))
            )
        end
        res_left = df_temp[:, :_idx_left]
        res_right = df_temp[:, :_idx_right]
    end
    df = if jointype == :right
        hcat(df2[res_right, :], select(df1[res_left, :], Not(on1)))
    else
        hcat(df1[res_left, :], select(df2[res_right, :], Not(on2)))
    end

    if any(validate)
        df[!, :_idx_left] = res_left
        df[!, :_idx_right] = res_right
        if validate[1]
            cols = string.(vcat(on1, l_col_names.(conditions))) |> unique
            temp = df[nonunique(df[:, [:_idx_left]]), cols]
            validate_error(temp)
        end
    
        if validate[2]
            cols = string.(vcat(on1, r_col_names.(conditions))) |> unique
            temp = df[nonunique(df[:, [:_idx_right]]), cols]
            validate_error(temp)
        end
        select!(df, Not([:_idx_left, :_idx_right]))
    end

    if jointype == :left || jointype == :outer
        idx_l_add = DataFrames.find_missing_idxs(res_left, nrow(df1))
        temp = df1[idx_l_add, :]
        insertcols!(temp, [col => missing for col in names(df) if col ∉ names(temp)]...)
        df = vcat(df, temp)
    end
    if jointype == :right || jointype == :outer
        idx_r_add = DataFrames.find_missing_idxs(res_right, nrow(df2))
        temp = df2[idx_r_add, :]
        insertcols!(temp, [col => missing for col in names(df) if col ∉ names(temp)]...)
        df = vcat(df, temp)
    end

    return df
end

function parse_ons(on)
    on1 = String[]
    on2 = String[]
    for x in on
        if typeof(x) <: Pair
            push!(on1, String(x[1]))
            push!(on2, String(x[2]))
        else
            push!(on1, String(x))
            push!(on2, String(x))
        end
    end
    return on1, on2
end

# I wrote this macro a while ago, do not know if it currently works
# function parse_expression(
#     expression::Expr
# )
#     out = Expr[]
#     if expression.head == :call || expression.head == :comparison
#         push!(out, expression)
#         return out
#     end
#     for a in expression.args
#         if a.head == :&& || a.head == :||
#             out = vcat(out, parse_expression(a))
#         else a.head == :call
#             push!(out, a)
#         end
#     end
#     return out
# end

# function return_function(
#     val::Symbol
# )
#     if val == :<
#         return <
#     elseif val == :>
#         return >
#     elseif val == :<=
#         return <=
#     elseif val == :>=
#         return >=
#     else
#         error("Function Symbol must be a comparison")
#     end
# end

# function reverse_return_function(
#     val::Symbol
# )
#     if val == :<
#         return return_function(:>)
#     elseif val == :>
#         return return_function(:<)
#     elseif val == :<=
#         return return_function(:>=)
#     elseif val == :>=
#         return return_function(:<=)
#     else
#         error("Function Symbol must be a comparison")
#     end
# end

# function push_condition!(
#     conditions::Array{Tuple{Function, Symbol, Symbol}},
#     f::Symbol,
#     first::Expr,
#     second::Expr
# )
#     if first.args[1] == :left && second.args[1] == :right
#         push!(
#             conditions,
#             (
#                 return_function(f),
#                 eval(first.args[2]),
#                 eval(second.args[2])
#             )
#         )
#     elseif first.args[1] == :right && second.args[1] == :left
#         push!(
#             conditions,
#             (
#                 reverse_return_function(f),
#                 eval(second.args[2]),
#                 eval(first.args[2])
#             )
#         )
#     else
#         error("Comparison must have right and left as labels")
#     end
# end

# function expressions_to_conditions(
#     expressions::Array{Expr}
# )
#     out = Tuple{Function, Symbol, Symbol}[]
#     for x in expressions
#         if x.head == :call
#             push_condition!(
#                 out,
#                 x.args[1],
#                 x.args[2],
#                 x.args[3]
#             )
#         elseif x.head == :comparison
#             for i in 1:2:length(x.args)-1
#                 push_condition!(
#                     out,
#                     x.args[i+1],
#                     x.args[i],
#                     x.args[i+2]
#                 )
#             end
#         end
#     end
#     return out
# end
            





# function parse_expr(fil)
#     fil = string(fil)
#     for (s, r) in [("||", ") .| ("), ("&&", ") .& ("), ("<", ".<"), (">", ".>"), (r"left\.([^\s]*)", s"df1[i, :\1]"), (r"right\.([^\s]*)", s"temp[:, :\1]")]
#         fil = replace(fil, s => r)
#     end
#     fil = "($fil)"
#     Meta.parse(fil)
# end




# function join_helper(
#     df1,
#     df2,
#     on,
#     conditions,
#     args...
# )
#     #new_conditions = conditions |> parse_expression |> expressions_to_conditions
#     quote
#         $range_join(
#             $df1,
#             $df2,
#             $on,
#             $conditions;
#             $(args...)
#         )
#     end
# end

# macro join(
#     df1,
#     df2,
#     on,
#     conditions,
#     args...
# )
#     local new_conditions = conditions |> parse_expression |> expressions_to_conditions
#     #local aakws = [esc(a) for a in args]
#     esc(join_helper(df1, df2, on, new_conditions, args...))
# end

function make_ff_est_windows!(
    df,
    ff_est;
    date_start::String="dateStart",
    date_end::String="dateEnd",
    est_window_start::String="est_window_start",
    est_window_end::String="est_window_end",
    date::String="date",
    suppress_warning::Bool=false
)
    if ismissing(ff_est.event_window)
        if date_start ∉ names(df) || date_end ∉ names(df)
            x = "If `event_window` is `missing` in FFEstMethod, "
            x *= "$date_start and $date_end must be included instead."
            @error x
        end
    else
        if !suppress_warning && (date_start ∈ names(df) || date_end ∈ names(df))
            x = "$date_start or $date_end are already in the "
            x *= "dataframe, passing a nonmissing value to "
            x *= "`FFEstMethod` `event_window` will overwrite "
            x *= "the preexisting values in $date_start and $date_end."
            @warn x
        end
        # if the estimation window has business days, adjust the event
        # date to a business day
        to_bday = typeof(ff_est.event_window.s) == BDay ? BDay(0, ff_est.event_window.s.calendar) : Day(0)
        df[!, date_start] = df[:, date] .+ to_bday .+ ff_est.event_window.s
        df[!, date_end] = df[:, date] .+ to_bday .+ ff_est.event_window.e
    end

    if ismissing(ff_est.estimation_window)
        if est_window_end ∉ names(df) || est_window_start ∉ names(df)
            x = "if `estimation_window` is `missing` in FFEstMethod,"
            x *= " $est_window_start and $est_window_end must be included"
            x *= " instead."
            @error x
        end
    else
        if !suppress_warning && (est_window_end ∈ names(df) || est_window_start ∈ names(df))
            x = "$est_window_start or $est_window_end are already in the "
            x *= "dataframe, passing a nonmissing value to "
            x *= "`FFEstMethod` `estimation_window` will overwrite "
            x *= "the preexisting values in $est_window_start and $est_window_end."
            @warn x
        end
        if typeof(ff_est.gap_to_event) <: String && ff_est.gap_to_event ∉ names(df)
            x = "If `gap_to_event` in FFEstMethod is a `String`"
            x *= " that `String` must be a column name in the `DataFrame`."
            @error x
        end
        if typeof(ff_est.gap_to_event) <: String
            to_bday = typeof(ff_est.estimation_window.e) == BDay ? BDay(0, ff_est.estimation_window.e.calendar) : Day(0)
            df[!, est_window_end] = df[:, ff_est.gap_to_event] .+ to_bday .+  ff_est.estimation_window.e
            df[!, est_window_start] = df[:, ff_est.gap_to_event] .+ to_bday .+  ff_est.estimation_window.s
        else
            # I subtract an extra day since not doing so makes the trading
            # window longer and the between gap a day shorter than it should be
            extra_day = typeof(ff_est.gap_to_event) == BDay ? BDay(1, ff_est.gap_to_event.calendar) : Day(1)

            df[!, est_window_end] = df[:, date_start] .+ ff_est.gap_to_event .- extra_day .+ ff_est.estimation_window.e
            # I don't subtract an extra day here since doing so adds an extra day
            # to the estimation period
            df[!, est_window_start] = df[:, date_start] .+ ff_est.gap_to_event .+ ff_est.estimation_window.s

        end
    end
end