using WRDSMerger
using Documenter

DocMeta.setdocmeta!(
    WRDSMerger,
    :DocTestSetup,
    quote
        using DuckDB, DBInterface, DataFrames, WRDSMerger, Dates
        db = DBInterface.connect(DuckDB.DB, joinpath("..", "test", "data", "test_data_final.duckdb"))
        funs = [
            generate_crsp_links,
            generate_comp_crsp_links,
            generate_comp_cik_links,
            generate_ibes_links,
            generate_option_crsp_links,
            generate_ravenpack_links
        ]
        for fun in funs
            fun(db)
        end
        create_all_links()
    end;
    recursive=true
)

Documenter.makedocs(
    modules = [WRDSMerger],
    sitename = "WRDSMerger.jl",
    pages = [
        "Introduction" => "index.md",
        "Downloading WRDS Data" => "download_data.md",
        "Links Between WRDS Data" => [
            "Linking Basics" => "basic_linking.md",
            "Default Behavior" => "default_behavior.md",
            "Identifier Types" => "linking_identifiers.md",
            "Internals" => "linking_internals.md"
        ],
        "Miscellaneous Utilities" => "misc_utilities.md"
    ]
)

deploydocs(
    repo = "github.com/junder873/WRDSMerger.jl.git",
    target = "build",
)
