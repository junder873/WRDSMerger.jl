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

function getCrspNames(dsn, df, col, ignore; datecol="date", identifying_col::String="permno")
    permno::Array{<:Number} = Int[]
    ncusip::Array{String} = String[]
    cusip::Array{String} = String[]
    df = dropmissing(df, col)
    if identifying_col == "permno"
        permno = df[:, col]
    elseif identifying_col == "cusip"
        cusip = df[:, col]
    else
        ncusip = df[:, col]
    end
    crsp = unique(crspStocknames(dsn, permno=permno, cusip=cusip, ncusip=ncusip, cols=["permno", "ncusip", "cusip", "namedt", "nameenddt"]))
    for x in ignore
        if x in names(crsp) # Removes the column if it would create a duplicate
            select!(crsp, Not(x))
        end
    end
    crsp[!, :namedt] = coalesce.(crsp[:, :namedt], minimum(df[:, datecol]))
    crsp[!, :nameenddt] = coalesce.(crsp[:, :nameenddt], maximum(df[:, datecol]))
    df = range_join(
        df,
        crsp,
        [col => identifying_col],
        [(<=, Symbol(datecol), :nameenddt), (>=, Symbol(datecol), :namedt)],
        validate=(false, true)
    )
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

"""
Joins the left dataframe with a daterange (though this can be any range) and
the right dataframe with a date.

### Arguments
- `df1`: DataFrame with the range
- `df2`: DataFrame with a specific value that fits between the range
- `on`: Vector of column names that match is on (not including the range variables)
- `validate::Tuple{Bool, Bool} = (false, false)`: whether to make sure matches are unique
- `dateColMin::Union{String, Symbol} = "datemin"`: column name in left dataframe that is the minimum value
- `dateColMax::Union{String, Symbol} = "datemax"`: column name in left dataframe that is the max value
- `dateColTest::Union{String, Symbol} = "date"`: column name in right dataframe that fits between the min and max
- `joinfun::Function = leftjoin`: the function being performed, mainly leftjoin or rightjoin

"""
function dateRangeJoin(
    df1::AbstractDataFrame,
    df2::AbstractDataFrame;
    on::Union{Array{Symbol}, Array{String}} = Symbol[],
    validate::Tuple{Bool, Bool} = (false, false),
    dateColMin::Union{String, Symbol} = "datemin",
    dateColMax::Union{String, Symbol} = "datemax",
    dateColTest::Union{String, Symbol} = "date",
    joinfun::Function = leftjoin
)
    return range_join(
        df1,
        df2,
        on,
        [(<=, Symbol(dateColMin), Symbol(dateColTest)), (>=, Symbol(dateColMax), Symbol(dateColTest))];
        validate,
        joinfun

    )
end



"""
Joins the dataframes based on a series of conditions, designed
to work with ranges

### Arguments
- `df1::DataFrame`: left DataFrame
- `df2::DataFrame`: right DataFrame
- `on`: either array of column names or matched pairs
- `conditions::Array{Tuple{Function, Symbol, Symbol}}`: array of tuples where the tuple is (Function, left dataframe column symbol, right dataframe column symbol)
- `joinfun::Function=leftjoin`: function being performed
- `minimize`: either `nothing` or an array of column names or matched pairs, will only minimize 1 column
- `join_conditions::Union{Array{Symbol}, Symbol}`: defaults to `:and`, otherwise an array of symbols that is 1 less than the length of conditions that the joins will happen in (:or or :and)
- `validate::Tuple{Bool, Bool}`: Whether to validate a 1:1, many:1, or 1:many match
"""
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

    df1 = df1[:, :]
    df2 = df2[:, :]
    df2[!, :_index2] = 1:nrow(df2)

    on1, on2 = parse_ons(on)

    if minimize !== nothing
        min1, min2 = parse_ons(minimize)
        min1 = min1[1]
        min2 = min2[1]
    end

    gdf = groupby(df2, on2)

    df1[!, :_index2] = repeat([Int[]], nrow(df1))

    Threads.@threads for i in 1:nrow(df1)
        temp = get(
            gdf,
            Tuple(df1[i, on1]),
            0
        )
        temp == 0 && continue

        fil = Array{Bool}(undef, nrow(temp))
        fil .= true

        for (j, (fun, lcol, rcol)) in enumerate(conditions)
            temp_join = typeof(join_conditions) <: Symbol ? join_conditions : join_conditions[j-1]
            if j == 1 || temp_join == :and
                fil = fil .& broadcast(fun, df1[i, lcol], temp[:, rcol])
            else
                fil = fil .| broadcast(fun, df1[i, lcol], temp[:, rcol])
            end
        end

        temp = temp[fil, :]

        if nrow(temp) > 1 && minimize !== nothing
            x = argmin(abs.(df1[i, min1] .- temp[:, min2]))

            temp = temp[temp[:, min2] .== temp[x, min2], :]
        end

        if nrow(temp) > 0
            df1[i, :_index2] = temp._index2
            if validate[2] && nrow(temp) > 1
                error("More than one match at row $(temp._index2) in df2")
            end
        end


    end

    df1 = flatten(df1, :_index2)

    select!(df2, Not(on2))
    df1 = joinfun(df1, df2, on=:_index2, validate=(validate[1], true))
    select!(df1, Not([:_index2]))


    return df1
end

function parse_expression(
    expression::Expr
)
    out = Expr[]
    if expression.head == :call || expression.head == :comparison
        push!(out, expression)
        return out
    end
    for a in expression.args
        if a.head == :&& || a.head == :||
            out = vcat(out, parse_expression(a))
        else a.head == :call
            push!(out, a)
        end
    end
    return out
end

function return_function(
    val::Symbol
)
    if val == :<
        return <
    elseif val == :>
        return >
    elseif val == :<=
        return <=
    elseif val == :>=
        return >=
    else
        error("Function Symbol must be a comparison")
    end
end

function reverse_return_function(
    val::Symbol
)
    if val == :<
        return return_function(:>)
    elseif val == :>
        return return_function(:<)
    elseif val == :<=
        return return_function(:>=)
    elseif val == :>=
        return return_function(:<=)
    else
        error("Function Symbol must be a comparison")
    end
end

function push_condition!(
    conditions::Array{Tuple{Function, Symbol, Symbol}},
    f::Symbol,
    first::Expr,
    second::Expr
)
    if first.args[1] == :left && second.args[1] == :right
        push!(
            conditions,
            (
                return_function(f),
                eval(first.args[2]),
                eval(second.args[2])
            )
        )
    elseif first.args[1] == :right && second.args[1] == :left
        push!(
            conditions,
            (
                reverse_return_function(f),
                eval(second.args[2]),
                eval(first.args[2])
            )
        )
    else
        error("Comparison must have right and left as labels")
    end
end

function expressions_to_conditions(
    expressions::Array{Expr}
)
    out = Tuple{Function, Symbol, Symbol}[]
    for x in expressions
        if x.head == :call
            println(x.args)
            push_condition!(
                out,
                x.args[1],
                x.args[2],
                x.args[3]
            )
        elseif x.head == :comparison
            for i in 1:2:length(x.args)-1
                push_condition!(
                    out,
                    x.args[i+1],
                    x.args[i],
                    x.args[i+2]
                )
            end
        end
    end
    return out
end
            





function parse_expr(fil)
    fil = string(fil)
    for (s, r) in [("||", ") .| ("), ("&&", ") .& ("), ("<", ".<"), (">", ".>"), (r"left\.([^\s]*)", s"df1[i, :\1]"), (r"right\.([^\s]*)", s"temp[:, :\1]")]
        fil = replace(fil, s => r)
    end
    fil = "($fil)"
    println(fil)
    Meta.parse(fil)
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

function join_helper(
    df1,
    df2,
    on,
    conditions,
    args...
)
    #new_conditions = conditions |> parse_expression |> expressions_to_conditions
    quote
        $range_join(
            $df1,
            $df2,
            $on,
            $conditions;
            $(args...)
        )
    end
end

# macro join(
#     df1,
#     df2,
#     on,
#     conditions,
#     args...
# )
#     local new_conditions = conditions |> parse_expression |> expressions_to_conditions
#     local aakws = [esc(a) for a in args]
#     quote
#         $range_join(
#             $(df1),
#             $(df2),
#             $on,
#             $new_conditions;
#             $(aakws...)
#         )
#     end
# end

macro join(
    df1,
    df2,
    on,
    conditions,
    args...
)
    local new_conditions = conditions |> parse_expression |> expressions_to_conditions
    #local aakws = [esc(a) for a in args]
    esc(join_helper(df1, df2, on, new_conditions, args...))
end
