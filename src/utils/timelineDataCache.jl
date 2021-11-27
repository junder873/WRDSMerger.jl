# The idea for this is heavily inspired by BusinessDays.jl
# Thank you to that package for making this possible

###################################################################
##
# Another idea to make this section better is to rewrite it relying
# on a single master calendar from BusienssDays (likely a custom
# calendar). This would have the advantage of being a more common
# interface and considerably reducing storage since the individual
# firms would just need date_start, date_end, and a vector of
# days that are missing relative to the busienssday calendar.
##
# The difficulty of ths is quickly getting the underlying data.
# There would be no loss of efficiency for firms that have no
# missing data, but for firms with missing dates then bdayscount
# would provide an innacurate position in the data and would need
# to be adjusted based on the missing dates, which could be slow.
##
###################################################################


abstract type TimelineData end


struct FirmData <: TimelineData
    date_start::Date
    date_end::Date
    is_day::Vector{Bool}
    index::Vector{UInt32}
    data::Dict{String, Vector{<:Real}}
    function FirmData(d1, d2, is_day, index, data)
        @assert d2 >= d1 "End Date must be greater than Start Date"
        @assert length(is_day) == length(index) == Dates.value(d2 - d1) + 1 "Vector must equal total days."
        @assert maximum(index) == sum(is_day) "Largest value in vector is more than the length of data"
        @assert issorted(index) "Vector of days must be sorted"
        @assert all(sum(is_day) .== [length(v) for (i, v) in data]) "Length of data must be same as number of days"
        new(d1, d2, is_day, index, data)
    end
end

mutable struct MarketData <: TimelineData
    date_start::Date
    date_end::Date
    is_day::Vector{Bool}
    index::Vector{UInt32}
    cols::Vector{String}
    data::Matrix{<:Real}
    MarketData() = new()
end

const FIRM_DATA_CACHE = Dict{Int, FirmData}()
const MARKET_DATA_CACHE = MarketData()

function update_market_data_cache!(d1, d2, is_day, index, cols, data)
    @assert d2 >= d1 "End Date must be greater than Start Date"
    @assert length(is_day) == length(index) == Dates.value(d2 - d1) + 1 "Vector must equal total days."
    @assert maximum(index) == sum(is_day) "Largest value in vector is more than the length of data"
    @assert issorted(index) "Vector of days must be sorted"
    @assert sum(is_day) == size(data)[1] "Length of data must be same as number of days"
    @assert length(cols) == size(data)[2] "Number of column names must be same as columns of matrix"
    @assert unique(cols) == cols "Column names must be unique"
    
    MARKET_DATA_CACHE.date_start = d1
    MARKET_DATA_CACHE.date_end = d2
    MARKET_DATA_CACHE.is_day = is_day
    MARKET_DATA_CACHE.index = index
    MARKET_DATA_CACHE.cols = cols
    MARKET_DATA_CACHE.data = data
end


"""
Takes an AbstractDataFrame (typically either full DataFrame or SubDataFrame)
and specifies the set up index and business days that exist. Inspiration is largely
from BusinessDays.jl. Returns 2 vectors that are every day from the start to end
date, the vectors are of type bool for if that day is included in the dataframe and
integers for the position of the day of in the data.
"""
function create_timeline(
    df::AbstractDataFrame;
    perform_checks=true,
    date_col="date",
    valuecols=["ret"],
    check_market_data=true
)
    d1 = minimum(df[:, date_col])
    d2 = maximum(df[:, date_col])
    if perform_checks
        if !issorted(df, date_col)
            df = sort(df, date_col)
        end
        df = dropmissing(df, valuecols)
    end
    is_day = zeros(Bool, Dates.value(d2 - d1)+1)
    index = zeros(UInt32, length(is_day))
    df_counter = 1
    for index_counter in 1:length(index)
        index[index_counter] = df_counter
        if d1 + Day(index_counter) > df[df_counter, date_col]
            df_counter += 1
        end
    end
    for i in 1:nrow(df)
        is_day[Dates.value(df[i, date_col] - d1)+1]=true
    end
    if check_market_data && isdefined(MARKET_DATA_CACHE, :data)
        test_days = MARKET_DATA_CACHE.is_day[raw_range(MARKET_DATA_CACHE.date_start, d1, d2)]
        @assert sum(test_days) >= sum(test_days .| is_day) "There are days in the FirmData that are not in the MarketData"
    end

    return (
        d1,
        d2,
        is_day,
        index
    )
end


"""
    FirmData(
        df::DataFrame;
        date_col="date",
        id_col="permno",
        valuecols=nothing
    )

Creates the cached data for firms. Does some initial cleaning (dropmissing and sort)
and then assigns the values to a Dictionary (typically with Permno as a key) with the values
as vectors in a second Dictionary.
"""
function FirmData(
    df::DataFrame;
    date_col="date",
    id_col="permno",
    valuecols=nothing
)
    if valuecols === nothing
        valuecols = [n for n in names(df) if n ∉ [date_col, id_col]]
    end
    df = select(df, vcat([id_col, date_col], valuecols))
    dropmissing!(df)
    sort!(df)
    gdf = groupby(df, id_col)
    for (i, g) in enumerate(gdf)
        FIRM_DATA_CACHE[keys(gdf)[i][1]] = FirmData(create_timeline(
            g;
            perform_checks=false,
            date_col,
            )...,
            Dict([n => g[:, n] for n in valuecols]...)
        )
    end
end

"""
    MarketData(
        df::DataFrame;
        date_col="date",
        valuecols=nothing,
        add_intercept=true,
        force_update=false
    )

Creates the cached data for the market. Does some initial cleaning and creates a timeline
of the existing data.
"""
function MarketData(
    df::DataFrame;
    date_col="date",
    valuecols=nothing,
    add_intercept=true,
    force_update=false
)
    if (
        !isdefined(MARKET_DATA_CACHE, :data) ||
        force_update ||
        (
            MARKET_DATA_CACHE.date_start <= minimum(df[:, date_col]) &&
            MARKET_DATA_CACHE.date_end >= maximum(df[:, date_col])
        )
    )
        
        
        if valuecols === nothing
            valuecols = [n for n in names(df) if n ≠ date_col]
        end
        df = dropmissing(df, vcat([date_col], valuecols))
        sort!(df, [date_col])
        if add_intercept
            df[!, :intercept] = ones(Int, nrow(df))
            valuecols=vcat(["intercept"], valuecols)
        end
        select!(df, vcat([date_col], valuecols))
        (d1, d2, is_day, index) = create_timeline(df; perform_checks=false, date_col, check_market_data=false)
        cols = string.(valuecols)
        data = Matrix(df[:, valuecols])

        # perform checks
        update_market_data_cache!(d1, d2, is_day, index, cols, data)

    end
end

##########################################################
# Underlying accessor functions
##########################################################

# if the end of a range and that is on the weekend (or other non business day),
# end before the index
range_end(index, is_day) = is_day ? index : index-1

# The raw position is the position in the index to go to
raw_pos(date_start::Date, dt::Date) = Dates.value(dt - date_start) + 1

# Gets the range over the index or is_day vectors, used for getting data (especially market data)
# where the data needs to be the same length
raw_range(date_start::Date, d1::Date, d2::Date) = raw_pos(date_start, d1):raw_pos(date_start, d2)



"""
    data_range(timed_data::TimelineData, d1::Date, d2::Date)

    data_range(firm_data::FirmData, mkt_data::MarketData, d1::Date, d2::Date)

Provides the data range based on the dates provided. If only one TimelineData type is provided,
returns a range (i.e., 15:60) that corresponds to the data stored in the type. If firm and market
data are provided, then returns a vector of integers to make sure the number of rows provided
in the market matrix matches the length of the data vector for the firm.
"""
function data_range(timed_data::TimelineData, d1::Date, d2::Date)
    @assert timed_data.date_start <= d1 <= timed_data.date_end "Date Value out of bounds"
    @assert timed_data.date_start <= d2 <= timed_data.date_end "Date Value out of bounds"
    timed_data.index[raw_pos(timed_data.date_start, d1)]:range_end(
        timed_data.index[raw_pos(timed_data.date_start, d2)],
        timed_data.is_day[raw_pos(timed_data.date_start, d2)]
        )
end

function data_range(
    firm_data::FirmData,
    mkt_data::MarketData,
    d1::Date,
    d2::Date
)
    #date_range(mkt_data, d1, d2)
    mkt_data.index[raw_range(mkt_data.date_start, d1, d2)[firm_data.is_day[raw_range(firm_data.date_start, d1, d2)]]]
end


"""
    get_firm_data(id::Int, d1::Date, d2::Date, col::String; warn_dates::Bool=false)

Fetches a vector from the FIRM_DATA_CACHE for a specific firm over a date range.
"""
function get_firm_data(id::Int, d1::Date, d2::Date, col::String; warn_dates::Bool=false)
    if !haskey(FIRM_DATA_CACHE, id)
        return missing
    end
    firm_data = FIRM_DATA_CACHE[id]
    if warn_dates
        d1 < firm_data.date_start && @warn "Minimum Date is less than Cached Firm Date Start, this will be adjusted."
        d2 > firm_data.date_end && @warn "Maximum Date is greater than Cached Firm Date End, this will be adjusted."
    end
    d1 = max(d1, firm_data.date_start)
    d2 = min(d2, firm_data.date_end)

    firm_data.data[col][data_range(firm_data, d1, d2)]
end

col_pos(x::String, cols::Vector{String}) = findfirst(isequal(x), cols)

# only market data
"""
    get_market_data(d1::Date, d2::Date, cols_market::String...; warn_dates::Bool=false)

    get_market_data(id::Int, d1::Date, d2::Date, cols_market::Union{Nothing, Vector{String}}=nothing; warn_dates::Bool=false)

Fetches a Matrix of market data between two dates, if an id (Integer) is provided, then the rows of the matrix will be the same
length as the length of the vector for the firm betwee those two dates.
"""
function get_market_data(d1::Date, d2::Date, cols_market::String...; warn_dates::Bool=false)
    if warn_dates
        d1 < MARKET_DATA_CACHE.date_start && @warn "Minimum Date is less than Cached Market Date Start, this will be adjusted."
        d2 > MARKET_DATA_CACHE.date_end && @warn "Maximum Date is greater than Cached Market Date End, this will be adjusted."
    end
    d1 = max(d1, MARKET_DATA_CACHE.date_start)
    d2 = min(d2, MARKET_DATA_CACHE.date_end)

    if length(cols_market) == 0
        pos = 1:length(MARKET_DATA_CACHE.cols)
    else
        @assert all([c in MARKET_DATA_CACHE.cols for c in cols_market]) "Not all columns are in the data"
        pos = [col_pos(c, MARKET_DATA_CACHE.cols) for c in cols_market]
    end
    MARKET_DATA_CACHE.data[data_range(MARKET_DATA_CACHE, d1, d2), pos]
end

# market data with same length as a firm data

function get_market_data(id::Int, d1::Date, d2::Date, cols_market::String...; warn_dates::Bool=false)
    if !haskey(FIRM_DATA_CACHE, id)
        return missing
    end
    firm_data = FIRM_DATA_CACHE[id]
    if warn_dates
        d1 < firm_data.date_start && @warn "Minimum Date is less than Cached Firm Date Start, this will be adjusted."
        d2 > firm_data.date_end && @warn "Maximum Date is greater than Cached Firm Date End, this will be adjusted."
        d1 < MARKET_DATA_CACHE.date_start && @warn "Minimum Date is less than Cached Market Date Start, this will be adjusted."
        d2 > MARKET_DATA_CACHE.date_end && @warn "Maximum Date is greater than Cached Market Date End, this will be adjusted."
    end
    d1 = max(d1, firm_data.date_start, MARKET_DATA_CACHE.date_start)
    d2 = min(d2, firm_data.date_end, MARKET_DATA_CACHE.date_end)

    if length(cols_market) == 0
        pos = 1:length(MARKET_DATA_CACHE.cols)
    else
        @assert all([c in MARKET_DATA_CACHE.cols for c in cols_market]) "Not all columns are in the data"
        pos = [col_pos(c, MARKET_DATA_CACHE.cols) for c in cols_market]
    end
    MARKET_DATA_CACHE.data[data_range(MARKET_DATA_CACHE, firm_data, d1, d2), pos]
end

# market data and firm data with same length
"""
    get_firm_market_data(
        id::Int,
        d1::Date,
        d2::Date,
        col_firm::String,
        cols_market::Union{Nothing, Vector{String}}=nothing;
        warn_dates::Bool=false
    )

Returns a Tuple of a vector of firm data and a matrix of market data (matrix has same number of rows as vector length).
"""
function get_firm_market_data(
    id::Int,
    d1::Date,
    d2::Date,
    cols_market::Union{Nothing, Vector{String}}=nothing,
    col_firm::String="ret";
    warn_dates::Bool=false
)
    if !haskey(FIRM_DATA_CACHE, id)
        return missing
    end
    firm_data = FIRM_DATA_CACHE[id]
    if warn_dates
        d1 < firm_data.date_start && @warn "Minimum Date is less than Cached Firm Date Start, this will be adjusted."
        d2 > firm_data.date_end && @warn "Maximum Date is greater than Cached Firm Date End, this will be adjusted."
        d1 < MARKET_DATA_CACHE.date_start && @warn "Minimum Date is less than Cached Market Date Start, this will be adjusted."
        d2 > MARKET_DATA_CACHE.date_end && @warn "Maximum Date is greater than Cached Market Date End, this will be adjusted."
    end
    d1 = max(d1, firm_data.date_start, MARKET_DATA_CACHE.date_start)
    d2 = min(d2, firm_data.date_end, MARKET_DATA_CACHE.date_end)

    if cols_market === nothing
        pos = 1:length(MARKET_DATA_CACHE.cols)
    else
        @assert all([c in MARKET_DATA_CACHE.cols for c in cols_market]) "Not all columns are in the data"
        pos = [col_pos(c, MARKET_DATA_CACHE.cols) for c in cols_market]
    end
    (
        firm_data.data[col_firm][data_range(firm_data, d1, d2)],
        MARKET_DATA_CACHE.data[data_range(firm_data, MARKET_DATA_CACHE, d1, d2), pos]
    )
end

function get_firm_market_data(
    id::Int,
    d1::Date,
    d2::Date,
    col_market::String="vwretd",
    col_firm::String="ret";
    warn_dates::Bool=false
)
    if !haskey(FIRM_DATA_CACHE, id)
        return missing
    end
    firm_data = FIRM_DATA_CACHE[id]
    if warn_dates
        d1 < firm_data.date_start && @warn "Minimum Date is less than Cached Firm Date Start, this will be adjusted."
        d2 > firm_data.date_end && @warn "Maximum Date is greater than Cached Firm Date End, this will be adjusted."
        d1 < MARKET_DATA_CACHE.date_start && @warn "Minimum Date is less than Cached Market Date Start, this will be adjusted."
        d2 > MARKET_DATA_CACHE.date_end && @warn "Maximum Date is greater than Cached Market Date End, this will be adjusted."
    end
    d1 = max(d1, firm_data.date_start, MARKET_DATA_CACHE.date_start)
    d2 = min(d2, firm_data.date_end, MARKET_DATA_CACHE.date_end)

    @assert col_market ∈ MARKET_DATA_CACHE.cols "$col_market is not in the MARKET_DATA_CACHE"
    pos = col_pos(col_market, MARKET_DATA_CACHE.cols)

    (
        firm_data.data[col_firm][data_range(firm_data, d1, d2)],
        MARKET_DATA_CACHE.data[data_range(firm_data, MARKET_DATA_CACHE, d1, d2), pos]
    )
end