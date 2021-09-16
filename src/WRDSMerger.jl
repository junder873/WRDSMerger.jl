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
#using IndexedTables: ndsparse, rows
using FixedEffectModels
using StringDistances
using CSV
using LibPQ
using ParallelKMeans

##############################################################################
##
## Exported methods and types
##
##############################################################################

export link_identifiers, comp_data,
    crsp_data, crsp_market, crsp_stocknames,
    setRetTimeframe, retTimeframe, setFFMethod,
    ibesCrspLink, calculate_car,
    range_join, @join

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


end # module
