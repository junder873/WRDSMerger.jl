
function convert_identifier(::Type{ID}, x::T1, dt::Date; vargs...) where {ID<:AbstractIdentifier, T1<:AbstractIdentifier}
    throw(
        MethodError,
        "No Method currently links $T1 -> $ID. Make sure the proper data\
        is loaded and run `create_all_links()` to create the necessary methods."
    )
end

function convert_identifier(::Type{ID}, ::Missing, dt::Date; vargs...) where {ID<:AbstractIdentifier}
    missing
end

function new_link_method(data::Vector{L}) where {L<:AbstractLinkPair}
    new_link_method(Dict(data))
end
function new_link_method(data::Dict{T1, Vector{L}}) where {T1, ID, L<:AbstractLinkPair{T1, ID}}
    if WRDSMerger.has_parent(T1)
        @eval begin
            function convert_identifier(
                ::Type{$ID},
                x::$T1,
                dt::Date,
                data::Dict{$T1, Vector{$L}}=$data;
                allow_parent_firm=false,
                vargs...
            )
                if haskey(data, x)
                    WRDSMerger.choose_best_match(data[x], dt; vargs...)
                elseif allow_parent_firm && has_parent($T1) && parent_type($T1) != $ID
                    convert_identifier($ID, convert_identifier(parent_type($T1), x, dt), dt)
                else
                    missing
                end
            end
        end
    else
        @eval begin
            function convert_identifier(
                ::Type{$ID},
                x::$T1,
                dt::Date,
                data::Dict{$T1, Vector{$L}}=$data;
                vargs...
            )
                if haskey(data, x)
                    WRDSMerger.choose_best_match(data[x], dt; vargs...)
                else
                    missing
                end
            end
        end
    end
end

function new_link_method(
    ::Type{T1},
    ::Type{ID};
    current_links = all_pairs(AbstractIdentifier, AbstractIdentifier)
) where {ID<:AbstractIdentifier, T1<:AbstractIdentifier}
    f = get_steps(T1, ID; current_links)
    if f === nothing # there is not the necessary link data
        return nothing
    end
    @assert length(f) > 2 "Error in number of steps"
    @assert f[end] == ID "Failed to find path"
    inter_step = f[end-1]
    @eval begin
        function convert_identifier(::Type{$ID}, x::$T1, dt::Date; vargs...)
            convert_identifier(
                $ID,
                convert_identifier($inter_step, x, dt; vargs...),
                dt
            )
        end
    end
    println("Created link for $T1 -> $ID")
end

function base_method_exists(x, y)
    !isempty(methods(convert_identifier, (Type{y}, x, Date, Dict)))
end
function method_is_missing(x, y)
    isempty(
        intersect(
            methodswith(Type{y}, convert_identifier),
            methodswith(x, convert_identifier)
        )
    )
end

function all_pairs(
    a::Type{<:AbstractIdentifier},
    b::Type{<:AbstractIdentifier};
    out = Vector{Tuple{DataType, DataType}}(),
    test_fun=base_method_exists
)
    for x in subtypes(a)
        if isabstracttype(x)
            out = all_pairs(x, b; out, test_fun)
        end
        for y in subtypes(b)
            if x == y || isabstracttype(x)
                continue
            end
            if isabstracttype(y)
                out = all_pairs(a, y; out, test_fun)
            elseif test_fun(x, y)
                push!(out, (x, y))
            end
        end
    end
    out |> unique
end

function new_links(link, current_links)
    out = Vector{Vector{DataType}}()
    for l in current_links
        if link[end] == l[1] && l[2] ∉ link
            push!(out, vcat(link, [l[2]]))
        end
    end
    out
end
function find_path(links, current_links, T)
    out = Vector{Vector{DataType}}()
    for link in links
        temp = new_links(link, current_links)
        out = vcat(out, temp)
    end
    if length(out) == 0
        return nothing
    elseif any(last.(out) .== T)
        for x in out
            if last(x) == T && Permno ∈ x # Permno tends to be a much better match, so prefer
                # that over other potential paths
                # (e.g., gvkey -> permno -> cusip instead of gvkey -> permco -> cusip)
                return x
            end
        end

        return out[findfirst(last.(out) .== T)]
    else
        return find_path(out, current_links, T)
    end
end


function get_steps(
    ::Type{T1},
    ::Type{T2};
    current_links = all_pairs(AbstractIdentifier, AbstractIdentifier)
) where {T1, T2}
    links = new_links([T1], current_links)
    find_path(links, current_links, T2)
end

has_parent(::Type{<:AbstractIdentifier}) = false
has_parent(::Type{Permno}) = true
has_parent(::Type{Cusip}) = true
has_parent(::Type{NCusip}) = true
parent_type(::Type{Permno}) = Permco
parent_type(::Type{Cusip}) = Cusip6
parent_type(::Type{NCusip}) = NCusip6


"""
Picks the best identifier based on the vector of links provided.

## Args

- `allow_inexact_date=true`: If true, and the length of the supplied vector is 1, then is will return that
  value even if the supplied date does not fit within the link.
- `allow_parent_firm=false`: If true, then the match will retry with a parent firm. For example, if matching
    Cusip -> Permno, but there is no exact match, then the function will try again with the Cusip6 -> Permno.
    In cases of a SecurityIdentifier -> SecurityIdentifier, this will lead to inexact matches, but if the goal
    is a SecurityIdentifier -> FirmIdentifier (e.g., Cusip -> GVKey), this will create more matches.
"""
function choose_best_match(
    data::AbstractVector{L},
    dt::Date;
    allow_inexact_date=true,
    args...
)::Union{T2, Missing} where {T1, T2, L<:AbstractLinkPair{T1, T2}}
    best = 0
    for (i, v) in enumerate(data)
        if dt in v
            # either first or the current one is higher priority
            if best == 0 || data[best] < data[i]
                best = i
            end
        end
    end
    if best != 0
        childID(data[best])
    elseif allow_inexact_date && length(data) == 1 # no matches with date, but there is only one link
        childID(data[1])
    else
        missing
    end
end


# in the most generic version, allow flexible dates but not flexible firms since this is either
# a firm -> firm, firm -> security, or security -> security
function (::Type{ID})(
    x::T1,
    dt::Date;
    allow_inexact_date=true,
    allow_parent_firm=false,
) where {ID<:AbstractIdentifier, T1<:AbstractIdentifier}
    out = convert_identifier(ID, x, dt; allow_inexact_date, allow_parent_firm)
    value(out)
end

# A special version where if trying to link a security -> a firm, then allow a link to
# a parent firm earlier. For example, a Cusip might have no direct link to a Permno
# (which would then connect to a GVKey), but the Cusip6 does link to a Permno
# so use that value instead
function (::Type{ID})(
    x::T1,
    dt::Date;
    allow_inexact_date=true,
    allow_parent_firm=true,
) where {ID<:FirmIdentifier, T1<:SecurityIdentifier}
    out = convert_identifier(ID, x, dt; allow_inexact_date, allow_parent_firm)
    value(out)
end


# the current design is pretty fast, so the extra benefit of 
# using threads is pretty small
# function (::Type{ID})(
#     x::AbstractVector{T},
#     dt::AbstractVector{Date};
#     vargs...
# ) where {ID <: AbstractIdentifier, T<:Union{Missing, AbstractIdentifier}}
#     out = Vector{Union{Missing, String}}(missing, length(x))
#     Threads.@threads for i in eachindex(x, dt)
#         out[i] = ID(x[i], dt[i]; vargs...)
#     end
#     out
# end


function (::Type{ID})(
    x::Missing,
    dt::Date;
    vargs...
) where {ID <: AbstractIdentifier}
    missing
end

