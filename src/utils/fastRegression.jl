struct BasicReg <: RegressionModel
    coef::Vector{Float64}
    coefnames::Vector{String}
    yname::String
    nobs::Int
    tss::Float64
    rss::Float64
    function BasicReg(coef, coefnames, yname, nobs, tss, rss)
        @assert rss >= 0 "Residual sum of squares must be greater than 0"
        @assert tss >= 0 "Total sum of squares must be greater than 0"
        @assert nobs >= 0 "Observations must be greater than 0"
        @assert length(coef) == length(coefnames) "Number of coefficients must be same as number of coefficient names"
        new(coef, coefnames, yname, nobs, tss, rss)
    end
end


function quick_reg(
    y::Vector{<:Real},
    x::Matrix{<:Real},
    coefnames::Union{Nothing, Vector{String}}=nothing,
    yname::Union{Nothing, String}=nothing;
    minobs::Real=1
)
    size(x)[1] < size(x)[2] && return missing
    size(x)[1] < minobs && return missing
    if coefnames !== nothing
        @assert size(x)[2] == length(coefnames) "Coefficient names must be the same as number of matrix columns"
    else
        coefnames = ["x$i" for i in 1:size(x)[2]]
    end
    if yname === nothing
        yname = "y"
    end
    coef = cholesky!(Symmetric( x' * x )) \ (x' * y)
    rss = sum(abs2, (y .- x * coef))
    tss = sum(abs2, (y .- mean(y)))
    BasicReg(
        coef,
        coefnames,
        yname,
        length(y),
        tss,
        rss
    )
end

"""

    cache_reg(
        id::Int,
        est_min::Date,
        est_max::Date,
        cols_market::Union{Nothing, Vector{String}}=nothing,
        col_firm::String="ret";
        warn_dates::Bool=false,
        minobs::Real=.8,
        calendar="USNYSE"
    )

An intentionally minamilistic linear regression of a vector of firm data on a matrix
of market data, where the firm data is provided by an Integer id and the range
of dates. Designed to quickly estimate Fama French predicted returns.

## Arguments

- `id::Int`: The firm identifier
- `est_min::Date`: The start of the estimation period
- `est_max::Date`: The end of the estimation period
- `cols_market::Union{Nothing, Vector{String}}=nothing`: A vector of columns that are stored
    in the MARKET_DATA_CACHE, should not be repeated and it is recommended to include `"intercept"`
    as the intercept (typically first). If `nothing` (default) then all columns saved will be used.
    Note that if all columns are used there could be errors in the regression (if the risk free rate
    is stored) since over short periods the risk free rate is often constant and conflicts with
    the intercept
- `col_firm::String="ret"`: The column for the firm vector, typically this is the return, it must be
    in the FIRM_DATA_CACHE data
- `warn_dates::Bool=false`: Warns if the dates provided are outside the dates of the firm or market
    data. The warning can mean that more data needs to be downloaded and cached, but it often means
    the data was missing and that restricted the cached data.
- `minobs::Real=.8`: Minimum number of observations to run the regression, if the number provided
    is less than 1, then it is assumed to be a ratio (i.e., minimum observations is number of
    businessdays times minobs)
- `calendar="USNYSE"`: calendar to use if minobs is less than 1, should be initialized cache if
    used.
"""
function cache_reg(
    id::Int,
    est_min::Date,
    est_max::Date,
    cols_market::Union{Nothing, Vector{String}}=nothing,
    col_firm::String="ret";
    warn_dates::Bool=false,
    minobs::Real=.8,
    calendar="USNYSE"
)
    if !haskey(FIRM_DATA_CACHE, id)
        return missing
    end
    if minobs < 1
        minobs = bdayscount(calendar, est_min, est_max) * minobs
    end
    if cols_market === nothing
        cols_market = MARKET_DATA_CACHE.cols
    end
    quick_reg(
        get_firm_market_data(id, est_min, est_max, cols_market, col_firm; warn_dates)...,
        cols_market,
        col_firm;
        minobs
    )
end

function cache_reg(
    id::Int,
    est_min::Date,
    est_max::Date;
    cols_market::Union{Nothing, Vector{String}}=nothing,
    col_firm::String="ret",
    warn_dates::Bool=false,
    minobs::Real=.8,
    calendar="USNYSE"
)
    cache_reg(id, est_min, est_max, cols_market, col_firm; warn_dates, minobs, calendar)
end

StatsBase.predict(mod::BasicReg, x) = x * mod.coef

StatsBase.coef(x::BasicReg) = x.coef
StatsBase.coefnames(x::BasicReg) = x.coefnames
StatsBase.responsename(x::BasicReg) = x.yname
StatsBase.nobs(x::BasicReg) = x.nobs
StatsBase.dof_residual(x::BasicReg) = nobs(x) - length(coef(x))
StatsBase.r2(x::BasicReg) = 1 - (rss(x) / deviance(x))
StatsBase.adjr2(x::BasicReg) = 1 - rss(x) / deviance(x) * (nobs(x) - 1) / dof_residual(x)
StatsBase.islinear(x::BasicReg) = true
StatsBase.deviance(x::BasicReg) = x.tss
StatsBase.rss(x::BasicReg) = x.rss

"""
    predict(rr::BasicReg, date_start::Date, date_end::Date; warn_dates::Bool=true)

Uses a provided BasicReg model and the cached saved market data to predict returns
between the two dates provided
"""
function StatsBase.predict(rr::BasicReg, date_start::Date, date_end::Date; warn_dates::Bool=true)
    predict(rr, get_market_data(date_start, date_end, coefnames(rr)...; warn_dates))
end