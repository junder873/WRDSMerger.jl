module WRDSMerger

##############################################################################
##
## Dependencies
##
##############################################################################
using DataFrames
using Dates
using Statistics
using LibPQ
using DBInterface
using InteractiveUtils

##############################################################################
##
## Exported methods and types
##
##############################################################################

# identifiers and linking items
export Permno, Permco, Cusip, NCusip, Cusip6, NCusip6,
    GVKey, CIK, Ticker, IbesTicker, RPEntity, SecID,
    FirmIdentifier, SecurityIdentifier, AbstractIdentifier,
    LinkPair, AbstractLinkPair, create_all_links,
    generate_ibes_links, generate_crsp_links,
    generate_comp_cik_links, generate_option_crsp_links,
    generate_comp_crsp_links, generate_ravenpack_links

# downloads and WRDS exploration functions
export comp_data, crsp_data, crsp_market, crsp_stocknames,
    crsp_adjust, crsp_delist, list_libraries, list_tables,
    describe_table, get_table, raw_sql, ff_data

# extra utilities
export range_join, Conditions


##############################################################################
##
## Load files
##
##############################################################################

include(joinpath("links", "identifierTypes.jl"))
include(joinpath("links", "linkPairs.jl"))
include(joinpath("links", "linkMethods.jl"))
include(joinpath("links", "creatingLinks.jl"))
include(joinpath("links", "downloadLinks.jl"))
include(joinpath("links", "specialCases.jl"))

include("utils.jl")

include("crspFunctions.jl")
include("compFunctions.jl")
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
    "optionm_all_secnmd" => "optionm_all.secnmd",
    "ravenpack_common_rp_entity_mapping" => "ravenpack_common.rp_entity_mapping"
)

end 
