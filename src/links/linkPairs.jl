
abstract type AbstractLinkPair{T1<:AbstractIdentifier, T2<:AbstractIdentifier} end

function Base.show(io::IO, x::AbstractLinkPair{T1, T2}) where {T1, T2}
    show(io, "$T1($(x.parent)) -> $T2($(x.child)) valid $(x.dt1) - $(x.dt2) with priority $(round(x.priority, digits=3))")
end

function Base.print(io::IO, x::AbstractLinkPair{T1, T2}) where {T1, T2}
    print(io, "$T1($(x.parent)) -> $T2($(x.child)) valid $(x.dt1) - $(x.dt2) with priority $(round(x.priority, digits=3))")
end



parentID(data::AbstractLinkPair) = data.parent
childID(data::AbstractLinkPair) = data.child
min_date(data::AbstractLinkPair) = data.dt1
max_date(data::AbstractLinkPair) = data.dt2
priority(data::AbstractLinkPair) = data.priority

Base.in(dt::Date, link::AbstractLinkPair) = min_date(link) <= dt <= max_date(link)

function Base.isless(data1::AbstractLinkPair{T1, T2}, data2::AbstractLinkPair{T1, T2}) where {T1, T2}
    priority(data1) < priority(data2)
end



"""
    function LinkPair(
        parent::T1,
        child::T2,
        dt1::Union{Missing, Date, String}=Date(0, 1, 1),
        dt2::Union{Missing, Date, String}=Date(9999, 12, 31),
        priority::Real=0.0
    ) where {T1<:AbstractIdentifier, T2<:AbstractIdentifier}

    function LinkPair(
        parent::T1,
        child::T2,
        dt1::Union{Missing, Date, String},
        dt2::Union{Missing, Date, String},
        linkprim::String,
        linktype::String,
    ) where {T1<:Union{GVKey, Permno, Permco}, T2<:Union{GVKey, Permno, Permco}}

`LinkPair` is the basic structure that provides a link between two identifiers.
These are defined as a single direction link (T1 -> T2) that is valid between
a specific date range (inclusive) and has a given priority (higher is better).
Priority is useful if there are overlapping T1 -> T2 items. For example, a
[`FirmIdentifier`](@ref) likely has multiple [`SecurityIdentifier`](@ref)s
that relate to it. One common way to pick between different `SecurityIdentifier`s
is to pick the one with the large market cap as the primary.

If defining a new identifier that has other methods of choosing priorities
(such as a String indicating priority), it can help to define a function
that converts these strings into a number. An example of this exists for
linking GVKey -> Permno or Permco (and the reverse), which take in `linkprim`
and `linktype` and convert those to the appropriate priority.
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
        dt1::Union{Missing, Date, AbstractString}=Date(0, 1, 1),
        dt2::Union{Missing, Date, AbstractString}=Date(9999, 12, 31),
        priority::Real=0.0
    ) where {T1<:AbstractIdentifier, T2<:AbstractIdentifier}
        if ismissing(dt1)
            dt1 = Date(0, 1, 1)
        end
        if ismissing(dt2)
            dt2 = Date(9999, 12, 31)
        end
    
        if typeof(dt1) == String
            dt1 = Date(dt1)
        end
        if typeof(dt2) == String
            dt2 = Date(dt2)
        end
        return new{T1, T2}(t1, t2, dt1, dt2, priority)
    end
end



# GVKey is only ever linked to one CIK
Base.isless(data1::LinkPair{T1, T2}, data2::LinkPair{T1, T2}) where {T1<:Union{GVKey, CIK}, T2<:Union{GVKey, CIK}} = false
Base.in(dt::Date, link::LinkPair{T1, T2}) where {T1<:Union{GVKey, CIK}, T2<:Union{GVKey, CIK}} = true
#Base.in(dt::Date, link::LinkPair{T1, T2}) where {T1<:Union{NCusip, RPEntity}, T2<:Union{NCusip, RPEntity}} = min_date(link) <= dt <= max_date(link)


function LinkPair(
    t1::T1,
    t2::T2,
    dt1::Union{Missing, Date, AbstractString},
    dt2::Union{Missing, Date, AbstractString},
    linkprim::AbstractString,
    linktype::AbstractString,
) where {T1<:GVKey, T2<:Union{Permno, Permco}}
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
    return LinkPair(t1, t2, dt1, dt2, priority)
end

function LinkPair(
    t1::T1,
    t2::T2,
    dt1::Union{Missing, Date, AbstractString},
    dt2::Union{Missing, Date, AbstractString},
    linkprim::AbstractString,
    linktype::AbstractString,
) where {T1<:Union{Permno, Permco}, T2<:GVKey}
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
    return LinkPair(t1, t2, dt1, dt2, priority)
end

