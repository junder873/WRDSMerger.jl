using WRDSMerger
using Documenter

DocMeta.setdocmeta!(
    WRDSMerger,
    :DocTestSetup,
    quote
        data_dir = joinpath("..", "test", "data")
        using CSV, DataFrames, WRDSMerger, Dates
        files = [
            "crsp_links",
            "crsp_comp_links",
            "gvkey_cik_links",
            "ibes_links",
            "option_links",
            "ravenpack_links"
        ]
        funs=[
            generate_crsp_links,
            generate_comp_crsp_links,
            generate_comp_cik_links,
            generate_ibes_links,
            generate_option_crsp_links,
            generate_ravenpack_links
        ]
        for (file, fun) in zip(files, funs)
            fun(
                DataFrame(
                    CSV.File(joinpath(data_dir, file * ".csv"))
                )
            )
        end
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
