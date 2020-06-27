using WRDSMerger, Test, CSV, DataFrames, Dates

ret1 = setRetTimeframe(-2, 2, businessDays=true, monthPeriod=false)
ret2 = setRetTimeframe(-2, 2, businessDays=(false, true))
ret3 = setRetTimeframe(-1, 2, businessDays=false, monthPeriod=true)
ret4 = setRetTimeframe(0, 2, businessDays=false, monthPeriod=true)

permnos = [87055, 77702, 86525, 12448, 92655]
date = [Dates.Date(2012, 1, 15), Dates.Date(2015, 6, 30)]
df = DataFrame(permno=repeat(permnos, 2), date=repeat(date, 5))

data = CSV.read("data\\crsp.csv")
market_data = CSV.read("data\\crspm.csv")
resh = CSV.read("data\\dailycalc.csv", copycols=true)
res = calculate_car(df, [ret1, ret2], data=data, market_data=market_data)
sort!(res)
sort!(resh)

@test isequal(res, resh)

data = CSV.read("data\\crspMonth.csv")
market_data = CSV.read("data\\crspmMonth.csv")
resh = CSV.read("data\\monthcalc.csv", copycols=true)
res = calculate_car(df, [ret3, ret4], data=data, market_data=market_data)
sort!(res)
sort!(resh)

@test isequal(res, resh)