

struct LinkSet{T1<:AbstractIdentifier, T2<:AbstractIdentifier}
    data::Dict{T1, Vector{LinkPair{T1, T2}}}
end

function Base.getindex(data::LinkSet{T1}, val::T1) where {T1<:AbstractIdentifier}
    data.data[val]
end

Base.haskey(data::LinkSet{T1}, val::T1) where {T1 <: AbstractIdentifier} = haskey(data.data, val)

function Base.show(io::IO, data::LinkSet{T1, T2}) where {T1, T2}
    temp = check_priority_errors.(values(data.data))
    println(io, "Linking data for $T1 -> $T2")
    println(io, "Contains $(length(keys(data.data))) unique $T1 identifiers")
    if sum(temp) > 0
        println(io, "There are $(sum(temp)) cases of overlapping identifiers linking \
        $T1 -> $T2 that do not have a priority, links might be inconsistent")
    end
end


struct AllLinks
    # data stores all of the actual data
    data::Dict{Tuple{DataType, DataType}, LinkSet}
    # link_order communicates how to get from one identifier to another
    # even if the identifiers are not directly linked
    link_order::Dict{Tuple{DataType, DataType}, Vector{DataType}}
end

Base.haskey(data::AllLinks, x) = haskey(data.data, x)
function Base.getindex(data::AllLinks, ::Type{T1}, ::Type{T2})::LinkSet{T1, T2} where {T1<:AbstractIdentifier, T2<:AbstractIdentifier}
    data.data[(T1, T2)]
end

function update_links!(
    data::AllLinks,
    vals::LinkSet{T1, T2}
) where {T1, T2}
    data.data[(T1, T2)] = vals
end

function Base.show(io::IO, data::AllLinks)
    vals = keys(data.data)
    to_print_bi = Set{Tuple{DataType, DataType}}()
    to_print_single = Set{Tuple{DataType, DataType}}()
    for v in vals
        if (v[2], v[1]) ∈ vals && (v[2], v[1]) ∉ to_print_bi
            push!(to_print_bi, v)
        elseif (v[2], v[1]) ∉ vals
            push!(to_print_single, v)
        end
    end
    if length(to_print_bi) == 0 && length(to_print_single) == 0
        println(io, "Link Data currently has no links stored")
    end
    if length(to_print_bi) > 0
        println(io, "Link Data currently has stored the following bi-directional links:")
        for v in to_print_bi
            println(io, "  $(v[1]) <-> $(v[2])")
        end
    end
    if length(to_print_single) > 0
        println(io, "Link Data currently has stored the following single direction links:")
        for v in to_print_single
            println(io, "  $(v[1]) -> $(v[2])")
        end
    end
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
    if any(last.(out) .== T)
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


function get_steps(data::AllLinks, ::Type{T1}, ::Type{T2}) where {T1, T2}
    if haskey(data.link_order, (T1, T2))
        return data.link_order[(T1, T2)]
    end
    current_links = collect(keys(data.data))
    links = new_links([T1], current_links)
    new_path = find_path(links, current_links, T2)[2:end]
    data.link_order[(T1, T2)] = new_path
    new_path
end

has_parent(::Type{<:AbstractIdentifier}) = false
has_parent(::Type{Permno}) = true
has_parent(::Type{Cusip}) = true
has_parent(::Type{NCusip}) = true
parent_type(::Type{Permno}) = Permco
parent_type(::Type{Cusip}) = Cusip6
parent_type(::Type{NCusip}) = NCusip6
parent_firm(x::Permno, dt::Date) = Permco(x, dt; allow_parent_firm=false)
parent_firm(x::Cusip, dt::Date) = Cusip6(x)
parent_firm(x::NCusip, dt::Date) = NCusip6(x)


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
    data::AbstractVector{LinkPair{T1, T2}},
    dt::Date;
    allow_inexact_date=true,
    allow_parent_firm=false,
    available_links::AllLinks= GENERAL_LINK_DATA
)::Union{T2, Missing} where {T1, T2}
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
    # elseif allow_parent_firm && has_parent(T1) && T2 != parent_type(T1)
        # T2(parent_firm(parentID(data[1]), dt), dt; allow_inexact_date, data=available_links)
    elseif allow_inexact_date && length(data) == 1 # no matches with date, but there is only one link
        childID(data[1])
    else
        missing
    end
end

choose_best_match(data::Missing, dt; args...) = missing




function (::Type{ID})(
    x::T1,
    dt::Date,
    data::LinkSet{T1, ID};
    allow_inexact_date=true,
    allow_parent_firm=false,
    available_links::AllLinks= GENERAL_LINK_DATA
)::Union{ID, Missing} where {ID<:AbstractIdentifier, T1<:AbstractIdentifier}
    if haskey(data, x)
        choose_best_match(data[x], dt; allow_inexact_date, allow_parent_firm, available_links)
    else
        missing
    end
end

# in the most generic version, allow flexible dates but not flexible firms since this is either
# a firm -> firm, firm -> security, or security -> security
function (::Type{ID})(
    x::T1,
    dt::Date;
    data::AllLinks= GENERAL_LINK_DATA,
    allow_inexact_date=true,
    allow_parent_firm=false
)::Union{ID, Missing} where {ID <: AbstractIdentifier, T1 <: AbstractIdentifier}
    if haskey(data, (T1, ID))
        return ID(x, dt, data[T1, ID]; allow_inexact_date, allow_parent_firm, available_links=data)
    end
    steps = get_steps(data, T1, ID)
    for f in steps
        x = f(x, dt; allow_inexact_date, allow_parent_firm, data)
    end
    x
end

# A special version where if trying to link a security -> a firm, then allow a link to
# a parent firm earlier. For example, a Cusip might have no direct link to a Permno
# (which would then connect to a GVKey), but the Cusip6 does link to a Permno
# so use that value instead
function (::Type{ID})(
    x::T1,
    dt::Date;
    data::AllLinks= GENERAL_LINK_DATA,
    allow_inexact_date=true,
    allow_parent_firm=true
)::Union{ID, Missing} where {ID <: FirmIdentifier, T1 <: SecurityIdentifier}
    if haskey(data, (T1, ID))
        return ID(x, dt, data[T1, ID]; allow_inexact_date, allow_parent_firm, available_links=data)
    end
    steps = get_steps(data, T1, ID)
    for f in steps
        x = f(x, dt; allow_inexact_date, allow_parent_firm, data)
    end
    x
end

function (::Type{ID})(
    x::AbstractVector{T},
    dt::AbstractVector{Date};
    data::AllLinks= GENERAL_LINK_DATA,
    allow_inexact_date=true,
    allow_parent_firm=false
)::Vector{Union{ID, Missing}} where {ID <: AbstractIdentifier, T<:Union{Missing, AbstractIdentifier}}
    T1 = nonmissingtype(T)
    if haskey(data, (T1, ID))
        out = Vector{Union{Missing, ID}}(missing, length(x))
        to_use = data[T1, ID]
        Threads.@threads for i in eachindex(x)
            out[i] = ID(x[i], dt[i], to_use; allow_inexact_date, allow_parent_firm, available_links=data)
        end
        return out
    end
    steps = get_steps(data, T1, ID)
    for f in steps
        x = f(x, dt; allow_inexact_date, allow_parent_firm, data)
    end
    x
end

function (::Type{ID})(
    x::AbstractVector{T},
    dt::AbstractVector{Date};
    data::AllLinks= GENERAL_LINK_DATA,
    allow_inexact_date=true,
    allow_parent_firm=true
)::Vector{Union{ID, Missing}} where {ID <: FirmIdentifier, T<:Union{Missing, AbstractIdentifier}}
    T1 = nonmissingtype(T)
    if haskey(data, (T1, ID))
        out = Vector{Union{Missing, ID}}(missing, length(x))
        to_use = data[T1, ID]
        for i in eachindex(x)
            out[i] = ID(x[i], dt[i], to_use; allow_inexact_date, allow_parent_firm, available_links=data)
        end
        return out
    end
    steps = get_steps(data, T1, ID)
    for f in steps
        x = f(x, dt; allow_inexact_date, allow_parent_firm, data)
    end
    x
end


function (::Type{ID})(
    x::Missing,
    dt::Date;
    vargs...
) where {ID <: AbstractIdentifier}
    missing
end

function (::Type{ID})(
    x::Missing,
    dt::Date,
    data::LinkSet;
    vargs...
) where {ID <: AbstractIdentifier}
    missing
end
