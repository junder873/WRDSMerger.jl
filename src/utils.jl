

parse_date(::Missing) = missing
parse_time(::Missing) = missing
parse_datetime(::Missing) = missing
parse_int(::Missing) = missing
parse_date(d) = Date(d)
parse_time(d) = Time(d)
parse_datetime(d) = DateTime(d)
parse_int(x) = Int(x)

check_if_date(d::Missing) = true
check_if_time(d::Missing) = true
check_if_datetime(d::Missing) = true
function check_if_date(d::AbstractString)
    x = match(r"\d{4}-\d{1,2}-\d{1,2}", d)
    x === nothing && return false
    x.match == d && tryparse(Date, d) !== nothing
end
function check_if_time(d::AbstractString)
    x = match(r"\d{2}:\d{2}:\d{2}\.\d{1,4}", d)
    x === nothing && return false
    x.match == d && tryparse(Time, d) !== nothing
end
function check_if_datetime(d::AbstractString)
    x = match(r"\d{4}-\d{1,2}-\d{1,2}T\d{2}:\d{2}:\d{2}\.\d{1,4}", d)
    x === nothing && return false
    x.match == d && tryparse(DateTime, d) !== nothing
end
function check_if_date(d::AbstractVector)
    for x in d
        check_if_date(x) && continue
        return false
    end
    return true
end
function check_if_time(d::AbstractVector)
    for x in d
        check_if_time(x) && continue
        return false
    end
    return true
end
function check_if_datetime(d::AbstractVector)
    for x in d
        check_if_date(x) && continue
        return false
    end
    return true
end

function integer_or_missing(x::AbstractVector)
    for i in x
        ismissing(i) && continue
        isinteger(i) && continue
        return false
    end
    return true
end

"""
`modify_col!` tries to identify the real type of a column, especially for strings
that are actually dates or floats that are actually integers. Almost all data downloaded
from WRDS correctly specifies dates, but all numbers are stored as float8, even items like
`year` which should be an integer. This uses multiple dispatch to check if all elements
in a given column are compatible with changing type and then changes the type of the
column. Note that for strings this function does not by default try to convert integer
like strings to integer (such as GVKey), it only converts strings that look like a date,
datetime, or time.
"""
function modify_col!(df::AbstractDataFrame, col::String, ::Type{<:AbstractString})
    if check_if_date(df[:, col])
        df[!, col] = parse_date.(df[:, col])
    elseif check_if_time(df[:, col])
        df[!, col] = parse_time.(df[:, col])
    elseif check_if_datetime(df[:, col])
        df[!, col] = parse_datetime.(df[:, col])
    end
end

function modify_col!(df::AbstractDataFrame, col::String, ::Type{<:Real})
    if integer_or_missing(df[:, col])
        df[!, col] = parse_int.(df[:, col])
    end
end

function modify_col!(df::AbstractDataFrame, col::String, ::Type{Union{Missing, A}}) where {A <: Any}
    all_missing = all(ismissing.(df[:, col]))
    if !all_missing # if all items are missing, do not modify the column since it is uncertain what
        # it should be modified to
        modify_col!(df, col, A)
        if !any(ismissing.(df[:, col]))
            disallowmissing!(df, col)
        end
    end
end

function modify_col!(df::AbstractDataFrame, col::String, ::Type{<:Any})
end


function run_sql_query(
    conn,
    q::AbstractString
)
    df = DBInterface.execute(conn, q) |> DataFrame
    for col in names(df)
        modify_col!(df, col, eltype(df[:, col]))
    end
    df
end

"""
    Conditions(fun::Function, l::Union{Symbol,String}, r::Union{Symbol,String})
    Conditions(l, fun, r)

A condition for use with [`range_join`](@ref). Specifies a comparison function `fun`
(e.g., `<=`, `>=`) applied between column `l` from the left DataFrame and column `r`
from the right DataFrame.
"""
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

# I need the keymap returned instead of the full
# SubDataFrame
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


create_filter(x::AbstractArray{<:Real}) = "IN ($(join(x, ", ")))"
create_filter(x::AbstractArray) = "IN ('$(join(x, "', '"))')"
create_filter(x::Missing) = "IS NOT NULL"
create_filter(x::Real) = "= $x"
create_filter(x::AbstractString) = "= '$x'"

function create_filter(
    filters::Dict{String, <:Any},
    fil = ""
)
    for (key, data) in filters
        if length(fil) > 0
            fil *= " AND "
        else
            fil *= " WHERE "
        end
        fil *= "$key $(create_filter(data))"
    end
    return fil
end
