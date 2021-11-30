struct BasicReg <: RegressionModel
    coef::Vector{Float64}
    coefnames::Vector{String}
    yname::String
    nobs::Int
    tss::Float64
    rss::Float64
    residuals::Union{Vector{Float64}, Nothing}
    function BasicReg(coef, coefnames, yname, nobs, tss, rss, residuals)
        @assert rss >= 0 "Residual sum of squares must be greater than 0"
        @assert tss >= 0 "Total sum of squares must be greater than 0"
        @assert nobs >= 0 "Observations must be greater than 0"
        @assert length(coef) == length(coefnames) "Number of coefficients must be same as number of coefficient names"
        new(coef, coefnames, yname, nobs, tss, rss, residuals)
    end
end


function quick_reg(
    y::Vector{<:Real},
    x::Matrix{<:Real},
    coefnames::Union{Nothing, Vector{String}}=nothing,
    yname::Union{Nothing, String}=nothing;
    minobs::Real=1,
    save_residuals::Bool=false
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
    resid = y .- x * coef
    rss = sum(abs2, resid)
    tss = sum(abs2, (y .- mean(y)))
    BasicReg(
        coef,
        coefnames,
        yname,
        length(y),
        tss,
        rss,
        save_residuals ? resid : nothing
    )
end

"""

    cache_reg(
        id::Int,
        est_min::Date,
        est_max::Date;
        cols_market::Union{Nothing, Vector{String}}=nothing,
        col_firm::String="ret",
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
- `minobs::Real=.8`: Minimum number of observations to run the regression, if the number provided
    is less than 1, then it is assumed to be a ratio (i.e., minimum observations is number of
    businessdays times minobs)
- `calendar="USNYSE"`: calendar to use if minobs is less than 1, should be initialized cache if
    used.
- `save_residuals::Bool=false`: Whether or not to save the residuals in the regression
"""
function cache_reg(
    id::Int,
    est_min::Date,
    est_max::Date;
    cols_market::Union{Nothing, Vector{String}}=nothing,
    col_firm::String="ret",
    minobs::Real=.8,
    calendar="USNYSE",
    save_residuals::Bool=false
)
    if minobs < 1
        minobs = bdayscount(calendar, est_min, est_max) * minobs
    end
    if cols_market === nothing
        cols_market = MARKET_DATA_CACHE.cols
    end
    y, x = get_firm_market_data(id, est_min, est_max; cols_market, col_firm)
    if any(ismissing.(y))
        return missing
    end
    quick_reg(
        y,
        x,
        cols_market,
        col_firm;
        minobs,
        save_residuals
    )
end

StatsBase.predict(mod::BasicReg, x) = x * coef(mod)

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
function StatsBase.residuals(x::BasicReg)
    if x.residuals === nothing
        @error("To access residuals, run `cache_reg` with the option `save_residuals=true`")
    else
        x.residuals
    end
end

"""
    predict(rr::RegressionModel, date_start::Date, date_end::Date)

Uses a provided RegressionModel model and the cached saved market data to predict returns
between the two dates provided.

This will work with any RegressionModel provided as long as it provides methods for `coefnames`
that correspond to names stored in the MARKET_DATA_CACHE and a `coef` method. The model should be linear.
"""
function StatsBase.predict(rr::RegressionModel, date_start::Date, date_end::Date)
    get_market_data(date_start, date_end, coefnames(rr)...) * coef(rr)
end