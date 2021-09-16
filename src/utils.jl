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

function calculateDays(
    dates::Array{Date},
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

function getCrspNames(
    dsn::LibPQ.Connection,
    df,
    col,
    ignore;
    datecol="date",
    identifying_col::String="permno")
    df = dropmissing(df, col)
    crsp = if identifying_col == "permno"
        crsp_stocknames(dsn, df[:, col]; warn_on_long=false) |> unique
    elseif identifying_col == "cusip"
        crsp_stocknames(dsn, df[:, col]; warn_on_long=false) |> unique
    else
        crsp_stocknames(dsn, df[:, col]; warn_on_long=false, cusip_col="ncusip") |> unique
    end
    select!(crsp, Not(:ticker))
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
Joins the dataframes based on a series of conditions, designed
to work with ranges

### Arguments
- `df1::DataFrame`: left DataFrame
- `df2::DataFrame`: right DataFrame
- `on`: either array of column names or matched pairs
- `conditions::Array{Tuple{Function, Symbol, Symbol}}`: array of tuples where the tuple is (Function, left dataframe column symbol, right dataframe column symbol)
- `joinfun::Function=leftjoin`: function being performed
- `minimize`: either `nothing` or an array of column names or matched pairs, minimization will take place in order
- `join_conditions::Union{Array{Symbol}, Symbol}`: defaults to `:and`, otherwise an array of symbols that is 1 less than the length of conditions that the joins will happen in (:or or :and)
- `validate::Tuple{Bool, Bool}`: Whether to validate a 1:1, many:1, or 1:many match

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
        # min1 = min1[1]
        # min2 = min2[1]
    end

    gdf = groupby(df2, on2)

    df1[!, :_index2] = if joinfun == leftjoin || joinfun == outerjoin
        repeat([[0]], nrow(df1))
    else
        repeat([Int[]], nrow(df1))
    end

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
            temp_join = if typeof(join_conditions) <: Symbol
                join_conditions
            elseif j == 1 # if it is the first time through, all the current values are
                :and
            else
                join_conditions[j-1]
            end
            if temp_join == :and
                fil = fil .& broadcast(fun, df1[i, lcol], temp[:, rcol])
            else
                fil = fil .| broadcast(fun, df1[i, lcol], temp[:, rcol])
            end
        end

        temp = temp[fil, :]

        if nrow(temp) > 1 && minimize !== nothing
            for j in 1:length(min1)

                x = argmin(abs.(df1[i, min1[j]] .- temp[:, min2[j]]))

                temp = temp[temp[:, min2[j]] .== temp[x, min2[j]], :]
            end
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
