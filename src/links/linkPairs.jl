
"""
    AbstractLinkPair{T1<:AbstractIdentifier, T2<:AbstractIdentifier}

Abstract supertype for all link pairs. A link pair represents a directional
relationship from identifier type `T1` to `T2` over a date range with an
associated priority. See [`LinkPair`](@ref) for the concrete default implementation
and [Defining New `AbstractLinkPair`s](@ref) for how to create custom subtypes.

Required interface for subtypes: `parentID`, `childID`, `min_date`, `max_date`,
`priority`.
"""
abstract type AbstractLinkPair{T1<:AbstractIdentifier, T2<:AbstractIdentifier} end

function Base.show(io::IO, x::AbstractLinkPair{T1, T2}) where {T1, T2}
    show(io, "$T1($(parentID(x))) -> $T2($(childID(x))) valid $(min_date(x)) - $(max_date(x)) with priority $(round(priority(x), digits=3))")
end

function Base.print(io::IO, x::AbstractLinkPair{T1, T2}) where {T1, T2}
    print(io, "$T1($(parentID(x))) -> $T2($(childID(x))) valid $(min_date(x)) - $(max_date(x)) with priority $(round(priority(x), digits=3))")
end



parentID(data::AbstractLinkPair) = data.parent
childID(data::AbstractLinkPair) = data.child
min_date(data::AbstractLinkPair) = data.dt1
max_date(data::AbstractLinkPair) = data.dt2
priority(data::AbstractLinkPair, args...) = data.priority

Base.in(dt::Date, link::AbstractLinkPair) = min_date(link) <= dt <= max_date(link)

"""
    is_higher_priority(
        data1::AbstractLinkPair{T1, T2},
        data2::AbstractLinkPair{T1, T2},
        args...
    ) where {T1, T2}

Determines whether data1 has higher priority than data2. `args...` are
automatically passed to the `priority`function, which can then deal
with special circumstances (currently passed as the date of the match).
However, none of the default settings use this.
"""
function is_higher_priority(
    data1::AbstractLinkPair{T1, T2},
    data2::AbstractLinkPair{T1, T2},
    args...
) where {T1, T2}
    priority(data1, args...) > priority(data2, args...)
end



"""
    function LinkPair(
        parent::T1,
        child::T2,
        dt1::Union{Missing, Date, String}=Date(0, 1, 1),
        dt2::Union{Missing, Date, String}=Date(9999, 12, 31),
        priority::Real=0.0
    ) where {T1<:AbstractIdentifier, T2<:AbstractIdentifier}

`LinkPair` is the basic structure that provides a link between two identifiers.
These are defined as a single direction link (T1 -> T2) that is valid between
a specific date range (inclusive) and has a given priority (higher is better).
Priority is useful if there are overlapping T1 -> T2 items. For example, a
[`FirmIdentifier`](@ref) likely has multiple [`SecurityIdentifier`](@ref)s
that relate to it. One common way to pick between different `SecurityIdentifier`s
is to pick the one with the larger market cap as the primary.

If the source data provides priority as something other than a number
(such as a String indicating priority), convert it to a numeric value before
constructing the `LinkPair`. See [`gvkey_crsp_priority`](@ref) and
[`crsp_gvkey_priority`](@ref) for examples of functions that convert
CRSP/Compustat link priority strings into numeric priorities.
"""
struct LinkPair{T1<:AbstractIdentifier, T2<:AbstractIdentifier} <: AbstractLinkPair{T1, T2}
    parent::T1
    child::T2
    dt1::Date# first date valid
    dt2::Date# last date valid
    priority::Float64# higher is better
    function LinkPair(
        t1::T1,
        t2::T2,
        dt1::Date=Date(0, 1, 1),
        dt2::Date=Date(9999, 12, 31),
        priority::Real=0.0
    ) where {T1<:AbstractIdentifier, T2<:AbstractIdentifier}
        return new{T1, T2}(t1, t2, dt1, dt2, priority)
    end
    function LinkPair(
        t1::T1,
        t2::T2,
        dt1::Date,
        dt2::Missing,
        priority::Real=0.0
    ) where {T1<:AbstractIdentifier, T2<:AbstractIdentifier}
        return new{T1, T2}(t1, t2, dt1, Date(9999, 12, 31), priority)
    end
    function LinkPair(
        t1::T1,
        t2::T2,
        dt1::Missing,
        dt2::Missing,
        priority::Real=0.0
    ) where {T1<:AbstractIdentifier, T2<:AbstractIdentifier}
        return new{T1, T2}(t1, t2, Date(0, 1, 1), Date(9999, 12, 31), priority)
    end

        
end

function LinkPair(
    t1::T1,
    t2::T2,
    dt1::String,
    dt2::String,
    priority::Real=0.0
)
    LinkPair(t1, t2, Date(dt1), Date(dt2), priority)
end
function LinkPair(
    t1::T1,
    t2::T2,
    dt1::String,
    dt2::Missing,
    priority::Real=0.0
)
    LinkPair(t1, t2, Date(dt1), missing, priority)
end

# GVKey is only ever linked to one CIK
is_higher_priority(data1::LinkPair{T1, T2}, data2::LinkPair{T1, T2}, args...) where {T1<:Union{GVKey, CIK}, T2<:Union{GVKey, CIK}} = false
Base.in(dt::Date, link::LinkPair{T1, T2}) where {T1<:Union{GVKey, CIK}, T2<:Union{GVKey, CIK}} = true

"""
    gvkey_crsp_priority(linkprim::AbstractString, linktype::AbstractString) -> Float64

Converts CRSP/Compustat link descriptor strings into a numeric priority for
links going from [`GVKey`](@ref) to a CRSP identifier ([`Permno`](@ref) or
[`Permco`](@ref)). Higher values indicate a stronger link.

The `linkprim` flag (P > C > J) receives the larger weight, reflecting
that the primary security match matters most when starting from a firm
identifier. `linktype` (LC > LU > LS > LX > LD > LN > NR > NU) serves
as a tiebreaker.

See also [`crsp_gvkey_priority`](@ref) for the reverse direction.
"""
function gvkey_crsp_priority(linkprim::AbstractString, linktype::AbstractString)
    priority = 0.0
    if linkprim == "P"
        priority += 3
    elseif linkprim == "C"
        priority += 2
    elseif linkprim == "J"
        priority += 1
    end
    if linktype == "LC"
        priority += 0.8
    elseif linktype == "LU"
        priority += 0.7
    elseif linktype == "LS"
        priority += 0.6
    elseif linktype == "LX"
        priority += 0.5
    elseif linktype == "LD"
        priority += 0.4
    elseif linktype == "LN"
        priority += 0.3
    elseif linktype == "NR"
        priority += 0.2
    elseif linktype == "NU"
        priority += 0.1
    end
    return priority
end

"""
    crsp_gvkey_priority(linkprim::AbstractString, linktype::AbstractString) -> Float64

Converts CRSP/Compustat link descriptor strings into a numeric priority for
links going from a CRSP identifier ([`Permno`](@ref) or [`Permco`](@ref)) to
[`GVKey`](@ref). Higher values indicate a stronger link.

The `linktype` flag (LC > LU > LS > LX > LD > LN > NR > NU) receives the
larger weight, reflecting that the link quality matters most when starting
from a security identifier. `linkprim` (P > C > J) serves as a tiebreaker.

See also [`gvkey_crsp_priority`](@ref) for the reverse direction.
"""
function crsp_gvkey_priority(linkprim::AbstractString, linktype::AbstractString)
    priority = 0.0
    if linkprim == "P"
        priority += 0.3
    elseif linkprim == "C"
        priority += 0.2
    elseif linkprim == "J"
        priority += 0.1
    end
    if linktype == "LC"
        priority += 8
    elseif linktype == "LU"
        priority += 7
    elseif linktype == "LS"
        priority += 6
    elseif linktype == "LX"
        priority += 5
    elseif linktype == "LD"
        priority += 4
    elseif linktype == "LN"
        priority += 3
    elseif linktype == "NR"
        priority += 2
    elseif linktype == "NU"
        priority += 1
    end
    return priority
end

