
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

struct Conditions
    fun::Function
    l::Union{Symbol, String}
    r::Union{Symbol, String}
end

function Conditions(
    l::Union{Symbol, String},
    fun::Function,
    r::Union{Symbol, String}
)
    Conditions(
        fun,
        l,
        r
    )
end

function Conditions(
    x::Tuple{Function, Symbol, Symbol}
)
    Conditions(
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
        Conditions.(conditions);
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
    conditions::Array{Conditions};
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

r_col_names(x::Conditions) = x.r
l_col_names(x::Conditions) = x.l

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
        conditions::Array{Conditions};
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
- `conditions::Array{Conditions}`: array of `Conditions`, which specifies the function (<=, >, etc.), left and right columns
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
        Conditions(<, :date, :date_high),
        Conditions(>, :date, :date_low)
    ],
    join_conditions=[:and]
)
```


"""
function range_join(
    df1::DataFrame,
    df2::DataFrame,
    on,
    conditions::Array{Conditions};
    minimize=nothing,
    join_conditions::Union{Array{Symbol}, Symbol}=:and,
    validate::Tuple{Bool, Bool}=(false, false),
    jointype::Symbol=:inner,
)

    if nrow(df1) > nrow(df2) && minimize === nothing
        # the fewer main loops this goes through
        # the faster it is overall, if they are about equal
        # this likely slows things down, but it is
        # significantly faster if it is flipped for large sets
        # I added minimize === nothing since, under the current setup
        # the minimization is done as minimize assumes a groupby on
        # the left, so it does not work properly if this goes through
        # one thought on how to fix that is to create a minimize object
        # that would allow for setting the condition under which
        # minimizaiton makes sense
        new_cond = Conditions[]
        for con in conditions
            push!(new_cond, Conditions(change_function(con.fun), con.r, con.l))
        end
        on1, on2 = parse_ons(on)
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
        min1, min2 = parse_ons(minimize)
        df_temp = hcat(
            df1[res_left, string.(min1) |> unique],
            df2[res_right, string.(min2) |> unique]
        )
        df_temp[!, :_idx_left] = res_left
        df_temp[!, :_idx_right] = res_right
        for (l, r) in zip(min1, min2)
            #group_col = x.group_col_left ? :_idx_left : :_idx_right
            group_col = :_idx_left
            df_temp = subset(
                groupby(df_temp, group_col),
                [l, r] =>
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
