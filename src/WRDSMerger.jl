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

# extra utilities
export range_join, BDay, Conditions


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
