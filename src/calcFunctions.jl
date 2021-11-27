
Statistics.var(rr::BasicReg) = rss(rr) / dof_residual(rr)
Statistics.std(rr::BasicReg) = sqrt(var(rr))

function Statistics.var(
    id::Int,
    date_start::Date,
    date_end::Date,
    col_market::String="vwretd",
    col_firm::String="ret";
    warn_dates=false
)
    var((-).(get_firm_market_data(id, date_start, date_end, col_market, col_firm; warn_dates)...))
end

function Statistics.std(
    id::Int,
    date_start::Date,
    date_end::Date,
    col_market::String="vwretd",
    col_firm::String="ret";
    warn_dates=false
)
    sqrt(var(id, date_start, date_end, col_market, col_firm; warn_dates))
end

Statistics.var(rr::Missing) = missing
Statistics.std(rr::Missing) = missing

bh_return(x) = prod(1 .+ x) - 1
bhar(x, y) = bh_return(x) - bh_return(y)

# for firm data
"""
    bh_return(id::Int, d1::Date, d2::Date, col_firm::String="ret"; warn_dates::Bool=false)
    bh_return(d1::Date, d2::Date, col_market::String="vwretd"; warn_dates::Bool=true)

Calculates the buy and hold returns (also called geometric return) for TimelineData. If an Integer
is passed, then it is calculated based on the FIRM_DATA_CACHE (for the integer provided), otherwise
is calculated for the MARKET_DATA_CACHE.
"""
function bh_return(id::Int, d1::Date, d2::Date, col_firm::String="ret"; warn_dates::Bool=false)
    if !haskey(FIRM_DATA_CACHE, id)
        return missing
    end
    bh_return(get_firm_data(id, d1, d2, col_firm; warn_dates))
end

# for market data
function bh_return(d1::Date, d2::Date, col_market::String="vwretd"; warn_dates::Bool=true)
    bh_return(get_market_data(d1, d2, col_market; warn_dates))
end


"""
    bhar(id::Int, d1::Date, d2::Date, col_market::String="vwretd", col_firm::String="ret"; warn_dates::Bool=false)
    bhar(id::Int, d1::Date, d2::Date, rr::Union{Missing, BasicReg}; warn_dates::Bool=false)

Calculates the difference between buy and hold returns relative to the market. If a BasicReg type is passed, then
the expected return is estimated based on the regression (Fama French abnormal returns). Otherwise, the value is
based off of the value provided (typically a market wide return).
"""
function bhar(
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
    bh_return(id, d1, d2, col_firm; warn_dates) - bh_return(d1, d2, col_market; warn_dates)
end

function bhar(
    id::Int,
    d1::Date,
    d2::Date,
    rr::Union{Missing, BasicReg};
    warn_dates::Bool=false
)
    ismissing(rr) && return missing
    if !haskey(FIRM_DATA_CACHE, id)
        return missing
    end
    bh_return(id, d1, d2, responsename(rr); warn_dates) - bh_return(predict(rr, d1, d2; warn_dates))
end

"""
    car(id::Int, d1::Date, d2::Date, col_market::String="vwretd", col_firm::String="ret"; warn_dates::Bool=false)
    car(id::Int, d1::Date, d2::Date, rr::Union{Missing, BasicReg}; warn_dates::Bool=false)

Calculates the difference between cumulative returns relative to the market. If a BasicReg type is passed, then
the expected return is estimated based on the regression (Fama French abnormal returns). Otherwise, the value is
based off of the value provided (typically a market wide return).

Cumulative returns are the simple sum of returns, they are often used due to their ease to calculate but
undervalue extreme returns compared to buy and hold returns (bh_return or bhar).
"""
function car(
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
    sum(get_firm_data(id, d1, d2, col_firm; warn_dates)) - sum(get_market_data(d1, d2, col_market; warn_dates))
end

function car(
    id::Int,
    d1::Date,
    d2::Date,
    rr::Union{Missing, BasicReg};
    warn_dates::Bool=false
)
    ismissing(rr) && return missing
    if !haskey(FIRM_DATA_CACHE, id)
        return missing
    end
    sum(get_firm_data(id, d1, d2, responsename(rr); warn_dates)) - sum(predict(rr, d1, d2; warn_dates))
end


function get_coefficient_val(rr::BasicReg, coefname::String...)
    for x in coefname
        if x ∈ coefnames(rr)
            return coef(rr)[col_pos(x, coefnames(rr))]
        end
    end
    @error("None of $(coefname) is in the BasicReg model.")
end
"""
    alpha(rr::BasicReg, coefname::String...="intercept")

"alpha" in respect to the the CAPM model, i.e., the intercept in the model.
This is the alpha from the estimation period.

This function finds the position of the coefficient name provided, defaults to "intercept".
If the coefname is not in the regression, then this function errors.
"""
alpha(rr::BasicReg, coefname::String...="intercept") = get_coefficient_val(rr, coefname...)


"""
    beta(rr::BasicReg, coefname::String...=["mkt", "mktrf", "vwretd", "ewretd"])

"beta" in respect to the CAPM model, i.e., the coefficient on the market return minus the risk free rate.
This is the beta from the estimation period.

This function finds the position of the coefficient name provided, defaults to several common market returns.
If the coefname is not in the regression, then this function errors.
"""
beta(rr::BasicReg, coefname::String...=["mkt", "mktrf", "vwretd", "ewretd"]...) = get_coefficient_val(rr, coefname...)

alpha(rr::Missing, coefname::String...="intercept") = missing
beta(rr::Missing, coefname::String...="error") = missing