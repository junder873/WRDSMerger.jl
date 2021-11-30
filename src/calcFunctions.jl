
Statistics.var(rr::RegressionModel) = rss(rr) / dof_residual(rr)
Statistics.std(rr::RegressionModel) = sqrt(var(rr))

function Statistics.var(
    id::Int,
    date_start::Date,
    date_end::Date;
    cols_market::String="vwretd",
    col_firm::String="ret"
)
    var((-).(get_firm_market_data(id, date_start, date_end; cols_market, col_firm)...))
end

function Statistics.std(
    id::Int,
    date_start::Date,
    date_end::Date;
    cols_market::String="vwretd",
    col_firm::String="ret"
)
    sqrt(var(id, date_start, date_end; cols_market, col_firm))
end

Statistics.var(rr::Missing) = missing
Statistics.std(rr::Missing) = missing

bh_return(x) = prod(1 .+ x) - 1
bhar(x, y) = bh_return(x) - bh_return(y)

# for firm data
"""
    bh_return(id::Int, date_start::Date, date_end::Date, col_firm::String="ret")
    bh_return(date_start::Date, date_end::Date, cols_market::String="vwretd")

Calculates the buy and hold returns (also called geometric return) for TimelineData. If an Integer
is passed, then it is calculated based on the FIRM_DATA_CACHE (for the integer provided), otherwise
is calculated for the MARKET_DATA_CACHE.

These functions treat missing returns in the period implicitly as a zero return.
"""
function bh_return(id::Int, date_start::Date, date_end::Date, col_firm::String="ret")
    bh_return(get_firm_data(id, date_start, date_end, col_firm))
end

# for market data
function bh_return(date_start::Date, date_end::Date, cols_market::String="vwretd")
    bh_return(get_market_data(date_start, date_end, cols_market))
end


"""
    bhar(id::Int, date_start::Date, date_end::Date, cols_market::String="vwretd", col_firm::String="ret")
    bhar(id::Int, date_start::Date, date_end::Date, rr::Union{Missing, RegressionModel})

Calculates the difference between buy and hold returns relative to the market. If a RegressionModel type is passed, then
the expected return is estimated based on the regression (Fama French abnormal returns). Otherwise, the value is
based off of the value provided (typically a market wide return).

These functions treat missing returns in the period implicitly as a zero return.
"""
function bhar(
    id::Int,
    date_start::Date,
    date_end::Date;
    cols_market::String="vwretd",
    col_firm::String="ret",
)
    bhar(get_firm_market_data(id, date_start, date_end; cols_market, col_firm)...)
end

function bhar(
    id::Int,
    date_start::Date,
    date_end::Date,
    rr::Union{Missing, RegressionModel}
)
    ismissing(rr) && return missing
    ret, mkt = get_firm_market_data(id, date_start, date_end; cols_market=coefnames(rr), col_firm=responsename(rr))
    bhar(ret, predict(rr, mkt))
end

"""
    car(id::Int, date_start::Date, date_end::Date, cols_market::String="vwretd", col_firm::String="ret")
    car(id::Int, date_start::Date, date_end::Date, rr::Union{Missing, RegressionModel})

Calculates the difference between cumulative returns relative to the market. If a RegressionModel type is passed, then
the expected return is estimated based on the regression (Fama French abnormal returns). Otherwise, the value is
based off of the value provided (typically a market wide return).

Cumulative returns are the simple sum of returns, they are often used due to their ease to calculate but
undervalue extreme returns compared to buy and hold returns (bh_return or bhar).

These functions treat missing returns in the period implicitly as a zero return.
"""
function car(
    id::Int,
    date_start::Date,
    date_end::Date;
    cols_market::String="vwretd",
    col_firm::String="ret"
)
    sum((-).(get_firm_market_data(id, date_start, date_end; cols_market, col_firm)...))
end

function car(
    id::Int,
    date_start::Date,
    date_end::Date,
    rr::Union{Missing, RegressionModel}
)
    ismissing(rr) && return missing
    ret, mkt = get_firm_market_data(id, date_start, date_end; cols_market=coefnames(rr), col_firm=responsename(rr))
    sum(ret .- predict(rr, mkt))
end


function get_coefficient_val(rr::RegressionModel, coefname::String...)
    for x in coefname
        if x ∈ coefnames(rr)
            return coef(rr)[col_pos(x, coefnames(rr))]
        end
    end
    @error("None of $(coefname) is in the RegressionModel model.")
end
"""
    alpha(rr::RegressionModel, coefname::String...="intercept")

"alpha" in respect to the the CAPM model, i.e., the intercept in the model.
This is the alpha from the estimation period.

This function finds the position of the coefficient name provided, defaults to "intercept".
If the coefname is not in the regression, then this function returns an error.
"""
alpha(rr::RegressionModel, coefname::String...="intercept") = get_coefficient_val(rr, coefname...)


"""
    beta(rr::RegressionModel, coefname::String...=["mkt", "mktrf", "vwretd", "ewretd"])

"beta" in respect to the CAPM model, i.e., the coefficient on the market return minus the risk free rate.
This is the beta from the estimation period.

This function finds the position of the coefficient name provided, defaults to several common market returns.
If the coefname is not in the regression, then this function returns an error.
"""
beta(rr::RegressionModel, coefname::String...=["mkt", "mktrf", "vwretd", "ewretd"]...) = get_coefficient_val(rr, coefname...)

alpha(rr::Missing, coefname::String...="intercept") = missing
beta(rr::Missing, coefname::String...="error") = missing