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
using StringDistances
using CSV
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
    ibesCrspLink, calculate_car, 
    range_join, @join, BDay, FFEstMethod,
    EventWindow

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
include("date_functions.jl")


end # module
