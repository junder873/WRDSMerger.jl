module WRDSMerger

##############################################################################
##
## Dependencies
##
##############################################################################
using DataFrames
using Dates
using BusinessDays
using Statistics
using LibPQ
using DBInterface
using AbstractTrees
using ShiftedArrays: lead
using InteractiveUtils
using StatsBase
using Statistics
using LinearAlgebra

##############################################################################
##
## Exported methods and types
##
##############################################################################

# identifiers and linking items
export link_identifiers, Permno, Cusip, NCusip,
    GVKey, CIK, Ticker, IbesTicker, LinkTable,
    link_table

# downloads and WRDS exploration functions
export comp_data, crsp_data, crsp_market, crsp_stocknames,
    crsp_adjust, crsp_delist, list_libraries, list_tables,
    describe_table, get_table, raw_sql, ff_data
    
# types and functions for fast CAR calculations
export TimelineData, FirmData, car, alpha, beta,
    MarketData, get_firm_data, get_market_data,
    get_firm_market_data, BasicReg, cache_reg,
    bh_return, bhar

# extra utilities
export range_join, BDay, Conditions

# From Statistics
export var, std

# From StatsBase
export coef, coefnnames, responsename, nobs, dof_residual,
    r2, adjr2, islinear, deviance, rss, predict

##############################################################################
##
## Load files
##
##############################################################################

include("utils.jl")
include(joinpath("utils", "dateFunctions.jl"))
include(joinpath("utils", "identifierTypes.jl"))
include(joinpath("utils", "linkTree.jl"))
include(joinpath("utils", "utils.jl"))
include(joinpath("utils", "timelineDataCache.jl"))
include(joinpath("utils", "fastRegression.jl"))

include("crspFunctions.jl")
include("calcFunctions.jl")
include("compFunctions.jl")
include("mergeFunctions.jl")
include("exploreDB.jl")
include("ffData.jl")

global default_tables = Dict{String, String}(
    "comp_funda" => "compa.funda",
    "comp_fundq" => "compa.fundq",
    "crsp_stocknames" => "crsp.stocknames",
    "crsp_index" => "crsp.dsi",
    "crsp_stock_data" => "crsp.dsf",
    "crsp_delist" => "crsp.dsedelist",
    "crsp_a_ccm_ccmxpf_lnkhist" => "crsp_a_ccm.ccmxpf_lnkhist",
    "wrdsapps_ibcrsphist" => "wrdsapps.ibcrsphist",
    "comp_company" => "comp.company",
    "ff_factors" => "ff.factors_daily",
)

end 
