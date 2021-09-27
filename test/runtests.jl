using WRDSMerger, Test, CSV, DataFrames, Dates

permnos = [87055, 77702, 86525, 12448, 92655]
date = [Dates.Date(2012, 1, 15), Dates.Date(2015, 6, 30)]
df = DataFrame(permno=repeat(permnos, 2), date=repeat(date, 5))

data = CSV.File("test\\data\\crsp.csv") |> DataFrame
market_data = CSV.File("test\\data\\crspm.csv") |> DataFrame
resh = CSV.File("test\\data\\dailycalc.csv") |> DataFrame

##

temp = EventWindow.([
    (BDay(-2, :USNYSE), BDay(2, :USNYSE)),
    (Day(-2), BDay(2, :USNYSE))
])
##

res = calculate_car(
    (data, market_data),
    df,
    temp
)

sort!(res, [:permno, :date, :dateStart, :dateEnd])
sort!(resh)
select!(res, Not(["car_std", "businessDays", "name", "dateStart", "dateEnd"]))
dropmissing!(resh)
dropmissing!(res)

for col in names(res)
    if typeof(res[:, col]) <: Array{Float64}
        @test isapprox(sort(res[:, col]), sort(resh[:, col]))
    else
        @test isequal(res[:, col], resh[:, col])
    end
end

##

data = CSV.File("test\\data\\crspMonth.csv") |> DataFrame
market_data = CSV.File("test\\data\\crspmMonth.csv") |> DataFrame
resh = CSV.File("test\\data\\monthcalc.csv") |> DataFrame
res = calculate_car(
    (data, market_data),
    df,
    EventWindow.([
        (Month(-1), Month(2)),
        (Month(0), Month(2))
    ])
)
sort!(res, [:permno, :date, :dateStart, :dateEnd])
sort!(resh)
select!(res, Not(["car_std", "businessDays", "name", "dateStart", "dateEnd"]))
dropmissing!(resh)
dropmissing!(res)

for col in names(res)
    if typeof(res[:, col]) <: Array{Float64}
        @test isapprox(sort(res[:, col]), sort(resh[:, col]))
    else
        @test isequal(res[:, col], resh[:, col])
    end
end