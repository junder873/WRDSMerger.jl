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
using AbstractTrees
using ShiftedArrays: lead
using InteractiveUtils

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
    EventWindow, Condition, ff_data,
    Permno, Cusip, NCusip, GVKey, CIK,
    Ticker, IbesTicker, LinkTable, link_table

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

include("crspFunctions.jl")
include("calcFunctions.jl")
include("compFunctions.jl")
include("mergeFunctions.jl")
include("exploreDB.jl")


global default_tables = Dict{String, String}(
    "comp_funda" => "compa_funda",
    "comp_fundq" => "compa_fundq",
    "crsp_stocknames" => "crsp_stocknames",
    "crsp_index" => "crsp_dsi",
    "crsp_stock_data" => "crsp_dsf",
    "crsp_delist" => "crsp_dsedelist",
    "crsp_a_ccm_ccmxpf_lnkhist" => "crsp_a_ccm_ccmxpf_lnkhist",
    "wrdsapps_ibcrsphist" => "wrdsapps_ibcrsphist",
    "comp_company" => "comp_company",
    "ff_factors" => "ff_factors_daily",
)

end 
