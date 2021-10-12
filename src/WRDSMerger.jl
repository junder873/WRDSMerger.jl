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
    EventWindow, Condition, ff_data

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
