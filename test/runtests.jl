using WRDSMerger, Test, CSV, DataFrames, Dates

ret1 = setRetTimeframe(-2, 2, businessDays=true, monthPeriod=false)
ret2 = setRetTimeframe(-2, 2, businessDays=(false, true))
ret3 = setRetTimeframe(-1, 2, businessDays=false, monthPeriod=true)
ret4 = setRetTimeframe(0, 2, businessDays=false, monthPeriod=true)

permnos = [87055, 77702, 86525, 12448, 92655]
date = [Dates.Date(2012, 1, 15), Dates.Date(2015, 6, 30)]
df = DataFrame(permno=repeat(permnos, 2), date=repeat(date, 5))

data = CSV.File("data\\crsp.csv") |> DataFrame
market_data = CSV.File("data\\crspm.csv") |> DataFrame
resh = CSV.File("data\\dailycalc.csv") |> DataFrame
res = calculate_car(df, [ret1, ret2], data, market_data)

sort!(res)
sort!(resh)
select!(res, Not(["ret_std", "vol_std", "shrout_std", "retm_std", "car_std", "businessDays"]))
dropmissing!(resh)
dropmissing!(res)

for col in names(res)
    if typeof(res[:, col]) <: Array{Float64}
        @test isapprox(res[:, col], resh[:, col])
    else
        @test isequal(res[:, col], resh[:, col])
    end
end

data = CSV.File("data\\crspMonth.csv") |> DataFrame
market_data = CSV.File("data\\crspmMonth.csv") |> DataFrame
resh = CSV.File("data\\monthcalc.csv") |> DataFrame
res = calculate_car(df, [ret3, ret4], data, market_data)
sort!(res)
sort!(resh)
select!(res, Not(["ret_std", "vol_std", "shrout_std", "retm_std", "car_std", "businessDays"]))
dropmissing!(resh)
dropmissing!(res)

for col in names(res)
    if typeof(res[:, col]) <: Array{Float64}
        @test isapprox(res[:, col], resh[:, col])
    else
        @test isequal(res[:, col], resh[:, col])
    end
end