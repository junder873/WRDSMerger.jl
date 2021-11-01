
# Supertype for all Firm Identifiers
abstract type FirmIdentifier end

# Some identifiers are essentially strings, this is a super class for all of those
abstract type FirmIdentifierString <: FirmIdentifier end

# Reamining are similar to integers, this deals with those cases
abstract type FirmIdentifierInt <: FirmIdentifier end

# Technically, there is only 1 Cusip, however, WRDS considers two types (NCusip and Cusip)
# which are basicaly just old and current versions, since other than the actual
# numbers, these are exactly the samne, this abstract type deals with all of those
# so I can have common functions
abstract type CusipAll <: FirmIdentifierString end

# checksum calculator for Cusip
function luhn_checksum(s)
    if length(s) != 8
        error("Length must be 8 digits")
    end
    tot = 0
    for (i, v) in enumerate(s)
        n = if '0' ≤ v ≤ '9'
            parse(Int, v)
        elseif 'A' ≤ v ≤ 'Z'
            Int(v) - 65 + 10
        elseif 'a' ≤ v ≤ 'z'
            Int(v) - 97 + 10
        elseif v == '*'
            36
        elseif v == '@'
            37
        elseif v == '#'
            38
        end
        if iseven(i)
            n *= 2
        end
        tot += floor(n / 10) + n % 10
    end
    (10 - tot % 10) % 10 |> Int
end


"""
GVKey is the primary identifier in the Compustat universe
It is a string, though only has numeric values
"""
struct GVKey <: FirmIdentifierString
    val::String
    function GVKey(s::String)
        if length(s) > 6
            error("GVKey must be 6 characters long")
        end
        if occursin(r"\D", s)
            error("GVKey can only contain numeric characters")
        end
        new(lpad(s, 6, "0"))
    end
end

GVKey(n::Real) = GVKey(string(Int(n)))

"""
CIK is a common identifier outside of WRDS
It is a string, though has numeric values
"""
struct CIK <: FirmIdentifierString
    val::String
    function CIK(s::String)
        if length(s) > 10
            error("CIK must be 10 characters long")
        end
        if occursin(r"\D", s)
            error("CIK can only contain numeric characters")
        end
        new(lpad(s, 10, "0"))
    end
end

CIK(n::Real) = CIK(string(Int(n)))

"""
Cusip is a common identifier within and outside of WRDS
WRDS tracks older Cusips as NCusip
all Cusips are made up of 3 parts, issuer (6 characters)
issue (2 characters), and a checksum
most databases in WRDS only use the 8 characters
"""
struct Cusip <: CusipAll
    issuer::String
    issue::String
    checksum::Int
    function Cusip(issuer::AbstractString, issue::AbstractString, checksum::Int)
        if length(issuer) != 6
            error("Issuer identification must be 6 characters")
        end
        if length(issue) != 2
            error("Issue must be 2 characters")
        end
        if (!)(0 ≤ checksum ≤ 9)
            error("Checksum must be between 0 and 9 (inclusive)")
        end
        if luhn_checksum(issuer * issue) != checksum
            error("Invalid Cusip or Checksum")
        end
        new(String(issuer), String(issue), checksum)
    end
end

function Cusip(s::AbstractString)
    if length(s) == 9
        Cusip(s[1:6], s[7:8], parse(Int, s[9]))
    elseif length(s) == 8
        Cusip(s[1:6], s[7:8], luhn_checksum(s))
    else
        error("Cusip must be 8 or 9 characters")
    end
end

Cusip(issuer::AbstractString, issue::AbstractString) = Cusip(issuer, issue, luhn_checksum(issuer * issue))


"""
See note on Cusip
"""
struct NCusip <: CusipAll
    issuer::String
    issue::String
    checksum::Int
    function NCusip(issuer::AbstractString, issue::AbstractString, checksum::Int)
        if length(issuer) != 6
            error("Issuer identification must be 6 characters")
        end
        if length(issue) != 2
            error("Issue must be 2 characters")
        end
        if (!)(0 ≤ checksum ≤ 9)
            error("Checksum must be between 0 and 9 (inclusive)")
        end
        if luhn_checksum(issuer * issue) != checksum
            error("Invalid NCusip or Checksum")
        end
        new(string(issuer), string(issue), checksum)
    end
end

function NCusip(s::AbstractString)
    if length(s) == 9
        NCusip(s[1:6], s[7:8], parse(Int, s[9]))
    elseif length(s) == 8
        NCusip(s[1:6], s[7:8], luhn_checksum(s))
    else
        error("NCusip must be 8 or 9 characters")
    end
end

NCusip(issuer::AbstractString, issue::AbstractString) = NCusip(issuer, issue, luhn_checksum(issuer * issue))

Cusip(x::NCusip) = Cusip(x.issuer, x.issue, x.checksum)
NCusip(x::Cusip) = NCusip(x.issuer, x.issue, x.checksum)

"""
Ticker here refers to the ticker that is on the NYSE (not to be confused with IbesTicker)
While a common identifier outside of WRDS, this is not recommended since they change often
"""
struct Ticker <: FirmIdentifierString
    val::String
end

Ticker(s::AbstractString) = Ticker(string(s))


"""
IbesTicker is the primary identifier in the IBES universe
"""
struct IbesTicker <: FirmIdentifierString
    val::String
end

IbesTicker(s::AbstractString) = IbesTicker(string(s))

"""
Permno is the primary identifier of securities in the CRSP universe, it is also
one of the most common methods of linking between databases since it is easy to
find links to IBES, Compustat (GVKey), and Cusip.
"""
struct Permno <: FirmIdentifierInt
    val::Int
end

Permno(x::Real) = Permno(Int(x))

Permno(x::Permno) = x
Cusip(x::Cusip) = x
CIK(x::CIK) = x
GVKey(x::GVKey) = x
NCusip(x::NCusip) = x
Ticker(x::Ticker) = x
IbesTicker(x::IbesTicker) = x

Permno(x::Missing) = x
Cusip(x::Missing) = x
CIK(x::Missing) = x
GVKey(x::Missing) = x
NCusip(x::Missing) = x
Ticker(x::Missing) = x
IbesTicker(x::Missing) = x

######################################################################################
# As long as convert is specified for an identifier, all of the other functions here
# should work automatically work (as long as the type fits into string or integer)
######################################################################################

# Used for Permno
Base.convert(::Type{T}, x::K) where {T<:Real,K<:FirmIdentifierInt} = convert(T, x.val)

# Used for Permno, GVKey, and CIK
Base.convert(::Type{K}, x::Real) where {K<:FirmIdentifier} = K(x)

# Used for GVKey, CIK, Ticker, and IbesTicker
Base.convert(::Type{T}, x::K) where {T<:AbstractString,K<:FirmIdentifierString} = convert(T, x.val)
Base.convert(::Type{T}, x::K) where {T<:Real,K<:FirmIdentifierString} = parse(T, x.val)

# Used for GVKey, CIK, Ticker, IbesTicker, Cusip, and NCusip
Base.convert(::Type{K}, x::AbstractString) where {K<:FirmIdentifierString} = K(x)

# Used for Cusip and NCusip
Base.convert(::Type{T}, x::K) where {T<:AbstractString,K<:CusipAll} = convert(T, "$(x.issuer)$(x.issue)")

# String based identifiers

Base.:(==)(id::FirmIdentifierString, s::T) where {T<:AbstractString} = convert(T, id) == s
Base.:(==)(s::T, id::FirmIdentifierString) where {T<:AbstractString} = s == convert(T, id)

Base.isless(id::FirmIdentifierString, s::T) where {T <: AbstractString} = isless(convert(T, id), s)
Base.isless(s::T, id::FirmIdentifierString) where {T <: AbstractString} = isless(s, convert(T, id))
Base.isless(id1::FirmIdentifierString, id2::FirmIdentifierString) = isless(convert(String, id1), convert(String, id2))

Base.show(io::IOContext, id::FirmIdentifierString) = show(io, convert(String, id))
# Base.print(io::IOContext, x::GVKey) = print(io, x.val)
Base.string(id::FirmIdentifierString) = convert(String, id)
Base.tryparse(::Type{T}, s::AbstractString) where {T<:FirmIdentifierString} = try T(s) catch nothing end
value(id::FirmIdentifierString) = convert(String, id)

# Special "value" parameter for CusipAll so it can return 9 characters
value(x::CusipAll, l::Int=8) = "$(x.issuer)$(x.issue)$(x.checksum)"[1:l]

Base.promote_rule(::Type{T}, ::Type{K}) where {T<:FirmIdentifierString,K<:AbstractString} = K

Base.hash(id::FirmIdentifierString) = hash(convert(String, id))

# Integer based identifiers

Base.:(==)(id::FirmIdentifierInt, s::T) where {T<:Real} = convert(T, id) == s
Base.:(==)(s::T, id::FirmIdentifierInt) where {T<:Real} = s == convert(T, id)

Base.isless(id::FirmIdentifierInt, s::T) where {T <: Real} = isless(convert(T, id), s)
Base.isless(s::T, id::FirmIdentifierInt) where {T <: Real} = isless(s, convert(T, id))
Base.isless(id1::FirmIdentifierInt, id2::FirmIdentifierInt) = isless(convert(Int, id1), convert(Int, id2))

Base.show(io::IOContext, id::FirmIdentifierInt) = show(io, convert(Int, id))
Base.string(id::FirmIdentifierInt) = string(convert(Int, id))
value(id::FirmIdentifierInt) = convert(Int, id)

Base.promote_rule(::Type{T}, ::Type{K}) where {T<:FirmIdentifierInt,K<:Real} = K
Base.tryparse(::Type{T}, s::AbstractString) where {T<:FirmIdentifierInt} = try T(parse(Int, s)) catch nothing end
Base.hash(id::FirmIdentifierInt) = hash(convert(Int, id))

# to prevent errors for missing data
value(x::Missing) = missing

######################################################################################
# Future notes: One thing that would be nice is if I could specify `isequal` for
# items that are "number like", for example, it would be nice if
# GVKey("000015") == 15 returned true (which is easy) and the has values were the same
# (which I cannot figure out how to do). As far as I can tell, I can specify the hash
# value as either a String or an Integer, but not both

# Another future task is to make the has value of a tuple to be the same as the base
# version, right now this works well for normal joins but not well for groupby,
# which of course is what my join function relies on
######################################################################################