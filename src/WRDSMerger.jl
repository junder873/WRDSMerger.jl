module WRDSMerger

##############################################################################
##
## Dependencies
##
##############################################################################
using DataFrames
using Dates
using Statistics
using DBInterface
using InteractiveUtils

##############################################################################
##
## Exported methods and types
##
##############################################################################

# identifiers and linking items
export Permno, Permco, Cusip, NCusip, Cusip6, NCusip6, HdrCusip, HdrCusip6,
    GVKey, CIK, Ticker, IbesTicker, RPEntity, SecID,
    FirmIdentifier, SecurityIdentifier, AbstractIdentifier,
    LinkPair, AbstractLinkPair, create_all_links, identifier_data,
    generate_ibes_links, generate_crsp_links, generate_crsp_links_v2,
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
    "comp_funda" => "comp.funda",
    "comp_fundq" => "comp.fundq",
    "crsp_stocknames" => "crsp.stocknames",
    "crsp_index" => "crsp.dsi",
    "crsp_stock_data" => "crsp.dsf",
    "crsp_delist" => "crsp.dsedelist",
    "crsp_stocknames_v2" => "crsp.stocknames_v2",
    "crsp_stock_data_v2" => "crsp.dsf_v2",
    "crsp_a_ccm_ccmxpf_lnkhist" => "crsp_a_ccm.ccmxpf_lnkhist",
    "wrdsapps_ibcrsphist" => "wrdsapps.ibcrsphist",
    "comp_company" => "comp.company",
    "ff_factors" => "ff.factors_daily",
    "optionm_all_secnmd" => "optionm_all.secnmd",
    "ravenpack_common_rp_entity_mapping" => "ravenpack_common.rp_entity_mapping"
)

using PrecompileTools: @setup_workload, @compile_workload

@setup_workload begin
    # 4-element vectors with two patterns so that cross-pattern pairs
    # produce multi-child groups (exercising the full overlap/priority
    # sweep in check_priority_errors), while same-pattern pairs produce
    # same-child groups (exercising the all_same fast path).
    #
    # AABB: repeated parents pair with alternating children from ABAB vectors
    # ABAB: alternating values pair with repeated parents from AABB vectors
    #
    # GVKey and CIK share a pattern so their special is_higher_priority
    # override (always false) never creates multi-child groups that warn.

    # AABB pattern
    permnos     = [Permno(12345), Permno(12345), Permno(54321), Permno(54321)]
    ncusips     = [NCusip("12345678"), NCusip("12345678"), NCusip("98765432"), NCusip("98765432")]
    hdrcusips   = [HdrCusip("12345678"), HdrCusip("12345678"), HdrCusip("98765432"), HdrCusip("98765432")]
    gvkeys      = [GVKey("123456"), GVKey("123456"), GVKey("654321"), GVKey("654321")]
    ciks        = [CIK("1234567890"), CIK("1234567890"), CIK("9876543210"), CIK("9876543210")]
    tickers     = [Ticker("AAPL"), Ticker("AAPL"), Ticker("MSFT"), Ticker("MSFT")]

    # ABAB pattern
    permcos     = [Permco(1234), Permco(4321), Permco(1234), Permco(4321)]
    ncusips6    = [NCusip6("123456"), NCusip6("987654"), NCusip6("123456"), NCusip6("987654")]
    hdrcusips6  = [HdrCusip6("123456"), HdrCusip6("987654"), HdrCusip6("123456"), HdrCusip6("987654")]
    ibes_tickers = [IbesTicker("AAPL"), IbesTicker("MSFT"), IbesTicker("AAPL"), IbesTicker("MSFT")]
    rp_entities = [RPEntity("123456"), RPEntity("654321"), RPEntity("123456"), RPEntity("654321")]
    secids      = [SecID(123456), SecID(654321), SecID(123456), SecID(654321)]

    # Overlapping date ranges with all-distinct priorities so that
    # ties are always resolved (no warnings emitted). Positions
    # sharing a parent in any pattern always have different priorities.
    dt1s = [Date(2020, 1, 1), Date(2020, 3, 1), Date(2020, 1, 1), Date(2020, 3, 1)]
    dt2s = [Date(2020, 9, 30), Date(2020, 12, 31), Date(2020, 9, 30), Date(2020, 12, 31)]
    priorities = [4.0, 1.0, 3.0, 2.0]

    @compile_workload begin
        for v1 in [ncusips, ncusips6, hdrcusips, hdrcusips6, gvkeys, ciks, tickers, ibes_tickers, rp_entities, secids, permnos, permcos]
            for v2 in [ncusips, ncusips6, hdrcusips, hdrcusips6, gvkeys, ciks, tickers, ibes_tickers, rp_entities, secids, permnos, permcos]
                if v1 != v2
                    data1 = LinkPair.(v1, v2, dt1s, dt2s, priorities)
                    data2 = LinkPair.(v2, v1, dt1s, dt2s, priorities)
                    Dict(data1)
                    Dict(data2)
                end
            end
        end
    end
end

end 
