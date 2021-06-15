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

##############################################################################
##
## Exported methods and types
##
##############################################################################

export addIdentifiers, compustatCrspLink,
    getCompData,
    crspData, crspWholeMarket, crspStocknames,
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
