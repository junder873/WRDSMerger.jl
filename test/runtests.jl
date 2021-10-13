using SQLite, DataFrames, Dates, Test, CSV
using WRDSMerger

##


db = SQLite.DB(joinpath("data", "sql_data.sqlite"))

##
WRDSMerger.default_tables.comp_funda = "compa_funda"
WRDSMerger.default_tables.comp_fundq = "compa_fundq"
WRDSMerger.default_tables.crsp_stocknames = "crsp_stocknames"
WRDSMerger.default_tables.crsp_index = "crsp_dsi"
WRDSMerger.default_tables.crsp_stock_data = "crsp_dsf"
WRDSMerger.default_tables.crsp_delist = "crsp_dsedelist"
WRDSMerger.default_tables.crsp_a_ccm_ccmxpf_lnkhist = "crsp_a_ccm_ccmxpf_lnkhist"
WRDSMerger.default_tables.ibes_crsp = "wrdsapps_ibcrsphist"
WRDSMerger.default_tables.comp_company = "comp_company"
WRDSMerger.default_tables.ff_factors = "ff_factors_daily"

##

df = comp_data(db) |> dropmissing
println(size(df))
@test nrow(df) > 0

df = comp_data(db, 2020) |> dropmissing
println(size(df))
@test nrow(df) > 0

df = comp_data(db, 2020, 2020) |> dropmissing
println(size(df))
@test nrow(df) > 0

df = comp_data(db, Date(2020, 6, 30), 2020) |> dropmissing
println(size(df))
@test nrow(df) > 0

df = comp_data(db, Date(2020, 6, 30), Date(2021)) |> dropmissing
println(size(df))
@test nrow(df) > 0

df = comp_data(db; filters=Dict{String, String}()) |> dropmissing
println(size(df))
@test nrow(df) > 0

df = comp_data(db, annual=false, cols=["gvkey", "fyearq", "datadate", "fqtr", "saleq"]) |> dropmissing
println(size(df))
@test nrow(df) > 0

##

df = comp_data(db, ["001380", "002269"]) |> dropmissing
println(size(df))
@test nrow(df) > 0

df = comp_data(db, ["001380", "002269"], 2020) |> dropmissing
println(size(df))
@test nrow(df) > 0

df = comp_data(db, ["001380", "002269"], 2020, 2020) |> dropmissing
println(size(df))
@test nrow(df) > 0

df = comp_data(db, ["001380", "002269"], Date(2020, 6, 30), 2020) |> dropmissing
println(size(df))
@test nrow(df) > 0

df = comp_data(db, ["001380", "002269"], Date(2020, 6, 30), Date(2021)) |> dropmissing
println(size(df))
@test nrow(df) > 0

df = comp_data(db, ["001380", "002269"]; filters=Dict{String, String}()) |> dropmissing
println(size(df))
@test nrow(df) > 0

df = comp_data(db, ["001380", "002269"], annual=false, cols=["gvkey", "fyearq", "datadate", "fqtr", "saleq"]) |> dropmissing
println(size(df))
@test nrow(df) > 0


##

df = crsp_stocknames(db) |> dropmissing
println(size(df))
@test nrow(df) > 0

df = crsp_stocknames(db; cols=["permno", "cusip", "ticker"]) |> dropmissing
println(size(df))
@test nrow(df) > 0

df = crsp_stocknames(db, ["68389X10", "G2918310"]) |> dropmissing
println(size(df))
@test nrow(df) > 0

df = crsp_stocknames(db, ["68389X10", "27828110"], cusip_col="ncusip") |> dropmissing
println(size(df))
@test nrow(df) > 0

df = crsp_stocknames(db, ["68389X10", "27828110"], cols=["permno", "cusip", "ticker"], cusip_col="ncusip") |> dropmissing
println(size(df))
@test nrow(df) > 0

df = crsp_stocknames(db, [10104, 11762]) |> dropmissing
println(size(df))
@test nrow(df) > 0

df = crsp_stocknames(db, [10104, 11762], cols=["permno", "cusip", "ticker"]) |> dropmissing
println(size(df))
@test nrow(df) > 0

df = crsp_market(db) |> dropmissing
println(size(df))
@test nrow(df) > 0

df = crsp_market(db; dateStart=Date(2020)) |> dropmissing
println(size(df))
@test nrow(df) > 0

df = crsp_market(db; dateEnd=Date(2020)) |> dropmissing
println(size(df))
@test nrow(df) > 0

df = crsp_market(db; col="ewretd") |> dropmissing
println(size(df))
@test nrow(df) > 0

df_pull = DataFrame(
    permno=[10104, 11762],
    dateStart=[Date(2019), Date(2020)],
    dateEnd=[Date(2021), Date(2021)]
)
df = crsp_data(
    db,
    df_pull;
    pull_method=:optimize,
) |> dropmissing
println(size(df))
@test nrow(df) > 100

df = crsp_data(
    db,
    df_pull;
    pull_method=:minimize,
) |> dropmissing
println(size(df))
@test nrow(df) > 100

df = crsp_data(
    db,
    df_pull;
    pull_method=:stockonly,
) |> dropmissing
println(size(df))
@test nrow(df) > 100

df = crsp_data(
    db,
    df_pull;
    pull_method=:alldata,
) |> dropmissing
println(size(df))
@test nrow(df) > 100

df = crsp_data(
    db,
    df_pull;
    pull_method=:minimize,
    adjust_crsp_data=false
) |> dropmissing
println(size(df))
@test nrow(df) > 100

df = crsp_data(
    db,
    df_pull;
    pull_method=:minimize,
    cols=["ret", "askhi", "prc"]
) |> dropmissing
println(size(df))
@test nrow(df) > 100

df = crsp_data(
    db,
    Date(2015),
    Date(2021)
)
println(size(df))
@test nrow(df) > 100

df = crsp_data(
    db,
    Date(2015),
    Date(2021);
    cols=["ret", "askhi", "prc"]
)
println(size(df))
@test nrow(df) > 100

##

temp = DataFrame(
    permno=[10104],
    date=[Date(2020)]
)
df = link_identifiers(db, temp; permno=true, cusip=true, ncusip=true, gvkey=true, ticker=true, cik=true, ibes_ticker=true)
println(size(df))
println(df)
@test nrow(df) > 0

temp = DataFrame(
    gvkey=["012142"],
    date=[Date(2020)]
)
df = link_identifiers(db, temp; permno=true, cusip=true, ncusip=true, gvkey=true, ticker=true, cik=true, ibes_ticker=true)
println(size(df))
println(df)
@test nrow(df) > 0

temp = DataFrame(
    gvkey=[12142],
    date=[Date(2020)]
)
df = link_identifiers(db, temp; permno=true, cusip=true, ncusip=true, gvkey=true, ticker=true, cik=true, ibes_ticker=true)
println(size(df))
println(df)
@test nrow(df) > 0

temp = DataFrame(
    cik=[1341439],
    date=[Date(2020)]
)
df = link_identifiers(db, temp; permno=true, cusip=true, ncusip=true, gvkey=true, ticker=true, cik=true, ibes_ticker=true)
println(size(df))
println(df)
@test nrow(df) > 0

temp = DataFrame(
    cik=["0001341439"],
    date=[Date(2020)]
)
df = link_identifiers(db, temp; permno=true, cusip=true, ncusip=true, gvkey=true, ticker=true, cik=true, ibes_ticker=true)
println(size(df))
println(df)
@test nrow(df) > 0

temp = DataFrame(
    ticker=["ORCL"],
    date=[Date(2020)]
)
df = link_identifiers(db, temp; permno=true, cusip=true, ncusip=true, gvkey=true, ticker=true, cik=true, ibes_ticker=true)
println(size(df))
println(df)
@test nrow(df) > 0

temp = DataFrame(
    ticker=["ORCL"],
    date=[Date(2020)]
)
df = link_identifiers(db, temp; permno=true, cusip=true, ncusip=true, gvkey=true, ticker=true, cik=true, ibes_ticker=true, ibes_ticker_name="ticker", ticker_name="other")
println(size(df))
println(df)
@test nrow(df) > 0

##

temp = DataFrame(
    permno=[10104, 71563, 79637, 89002, 90993],
    date=[Date(2020, 12, 1), Date(2020, 12, 20), Date(2020, 7, 3), Date(2020, 9, 30), Date(2020, 10, 15)]
)

df = calculate_car(db, temp, EventWindow(BDay(-3, :USNYSE), BDay(3, :USNYSE)))
println(size(df))
@test nrow(df) > 0


df = calculate_car(db, temp, EventWindow(BDay(-3, :USNYSE), Month(1)))
println(size(df))
@test nrow(df) > 0


df = calculate_car(db, temp, (BDay(-3, :USNYSE), Day(3)))
println(size(df))
@test nrow(df) > 0


df = calculate_car(
    db,
    temp,
    [EventWindow(BDay(-3, :USNYSE), BDay(3, :USNYSE)), EventWindow(BDay(-3, :USNYSE), Month(1))]
)
println(size(df))
@test nrow(df) > 0

crsp_firms = crsp_data(db)
crsp_market_data = crsp_market(db)

df = calculate_car(
    (crsp_firms, crsp_market_data),
    temp,
    [EventWindow(BDay(-3, :USNYSE), BDay(3, :USNYSE)), EventWindow(BDay(-3, :USNYSE), Month(1))]
)
println(size(df))
@test nrow(df) > 0

df = calculate_car(
    (crsp_firms, crsp_market_data),
    temp,
    (Month(-1), Day(3))
)
println(size(df))
@test nrow(df) > 0

##

ff = FFEstMethod(event_window=EventWindow(BDay(-3, :USNYSE), BDay(3, :USNYSE)))
ff2 = FFEstMethod(event_window=EventWindow(Day(-5), BDay(3, :USNYSE)))

df = calculate_car(db, temp, ff)
println(size(df))
@test nrow(df) > 0

ff_market_data = WRDSMerger.ff_data(db)

df = calculate_car((crsp_firms, ff_market_data), temp, [ff, ff2])
println(size(df))
@test nrow(df) > 0

##

temp = DataFrame(
    permno=[82515, 14763, 15291, 51369, 61516, 76185, 87445],
    date=[Date(2020, 10, 7), Date(2020, 9, 21), Date(2020, 9, 21), Date(2020, 9, 21), Date(2020, 6, 22), Date(2020, 6, 22), Date(2020, 6, 22)]
)

sort!(temp, :permno)
df_res = CSV.File(joinpath("data", "car_results.csv")) |> DataFrame
sort!(df_res, :permno)

# the SAS code that I tested this against appears to round results to 3 significant digits

ff = FFEstMethod(event_window=EventWindow(BDay(-10, :USNYSE), BDay(10, :USNYSE)))
df = calculate_car(db, temp, ff)
sort!(df, :permno)
@test isapprox.(round.(df.car_ff, sigdigits=3), df_res.car_ff) |> all
@test isapprox.(round.(df.bhar_ff, sigdigits=3), df_res.bhar_ff) |> all
@test isapprox.(df.std_ff .^ 2, df_res.estimation_period_variance_ff_model_, rtol=.000001) |> all


ff = FFEstMethod(event_window=EventWindow(BDay(-10, :USNYSE), BDay(10, :USNYSE)), ff_sym=[:mktrf, :smb, :hml, :umd])
df = calculate_car(db, temp, ff)
sort!(df, :permno)
@test isapprox.(round.(df.car_ff, sigdigits=3), df_res.car_ffm) |> all
@test isapprox.(round.(df.bhar_ff, sigdigits=3), df_res.bhar_ffm) |> all
@test isapprox.(df.std_ff .^ 2, df_res.estimation_period_variance_carhart_model_, rtol=.000001) |> all


ff = FFEstMethod(event_window=EventWindow(BDay(-10, :USNYSE), BDay(10, :USNYSE)), ff_sym=[:mktrf])
df = calculate_car(db, temp, ff)
sort!(df, :permno)

@test isapprox.(round.(df.car_ff, sigdigits=3), df_res.car_mm) |> all
@test isapprox.(round.(df.bhar_ff, sigdigits=3), df_res.bhar_mm) |> all
@test isapprox.(df.std_ff .^ 2, df_res.estimation_period_variance_market_model_, rtol=.000001) |> all


# there are subtle differences between crsp vwretd and the returns listed in fama french
# to be able to accurately test this, I use the fama french data, but the results are very similar

df_crsp_firms = crsp_data(db)
df_crsp_market = ff_data(db)
df_crsp_market[!, :mkt] = df_crsp_market.mktrf .+ df_crsp_market.rf


df = calculate_car(
    (df_crsp_firms, df_crsp_market),
    temp,
    EventWindow(BDay(-10, :USNYSE), BDay(10, :USNYSE)),
    market_return="mkt"
)
sort!(df, :permno)
@test isapprox.(round.(df.car_sum, sigdigits=3), df_res.car_ma) |> all
@test isapprox.(round.(df.bhar, sigdigits=3), df_res.bhar_ma) |> all
