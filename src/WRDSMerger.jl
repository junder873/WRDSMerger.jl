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
using FixedEffectModels
using LibPQ
using DBInterface
using ParallelKMeans

##############################################################################
##
## Exported methods and types
##
##############################################################################

export link_identifiers, comp_data,
    crsp_data, crsp_market, crsp_stocknames,
    crsp_adjust, crsp_delist,
    ibes_crsp_link, calculate_car, 
    range_join, @join, BDay, FFEstMethod,
    EventWindow, Conditions, ff_data,
    DataFrame

##############################################################################
##
## Load files
##
##############################################################################

include("utils.jl")
include("crspFunctions.jl")
include("calcFunctions.jl")
include("compFunctions.jl")
include("mergeFunctions.jl")
include("dateFunctions.jl")
include("exploreDB.jl")


global default_tables = TableDefaults(
    "compa.funda",
    "compa.fundq",
    "comp.company",
    "crsp.dsf",
    "crsp.dsi",
    "crsp.dsedelist",
    "crsp.stocknames",
    "crsp_a_ccm.ccmxpf_lnkhist",
    "wrdsapps.ibcrsphist",
    "ff.factors_daily"
)

end 
