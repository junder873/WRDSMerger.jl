module WRDSMerger

##############################################################################
##
## Dependencies
##
##############################################################################
using ODBC
using DataFrames
using Dates
using BusinessDays
using Statistics
using IndexedTables: ndsparse, rows
using FixedEffectModels
using StringDistances
using CSV

##############################################################################
##
## Exported methods and types
##
##############################################################################

export addIdentifiers, compustatCrspLink,
    getCompData,
    crspData, crspWholeMarket, crspStocknames,
    calculateCAR,
    setRetTimeframe, retTimeframe, setFFMethod, dateRangeJoin,
    ibesCrspLink

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
