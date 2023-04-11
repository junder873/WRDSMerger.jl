# Supertype for all Firm Identifiers
abstract type AbstractIdentifier end

"""
A `FirmIdentifier` specifies a specific firm over a given date range and
is opposed to a [`SecurityIdentifier`](@ref). In a standard database, a single
`FirmIdentifier` can have multiple `SecurityIdentifier`s but a
`SecurityIdentifier` should only have one `FirmIdentifier`.

Examples include [`GVKey`](@ref), [`Permco`](@ref) and [`Cusip6`](@ref).
"""
abstract type FirmIdentifier <: AbstractIdentifier end

"""
A `SecurityIdentifier` specifies a specific firm over a given date range and
is opposed to a [`FirmIdentifier`](@ref). In a standard database, a single
`FirmIdentifier` can have multiple `SecurityIdentifier`s but a
`SecurityIdentifier` should only have one `FirmIdentifier`.

Examples include [`Permno`](@ref), [`Cusip`](@ref) and [`Ticker`](@ref).
"""
abstract type SecurityIdentifier <: AbstractIdentifier end

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
    GVKey <: FirmIdentifier

    GVKey(s::Union{<:Real, <:AbstractString})

    GVKey(x::AbstractIdentifier, d::Date)


GVKey is the primary identifier in the Compustat universe
It only contains numeric values (and is stored as an integer),
though is often represented as a string with up to 6 digits.
"""
struct GVKey <: FirmIdentifier
    val::Int
    function GVKey(s::Real)
        if s > 999999
            error("GVKey can only have 6 digits")
        end
        new(s)
    end
end
GVKey(n::AbstractString) = GVKey(parse(Int, n))

GVKey(n::Symbol) = GVKey(String(n))

value(n::GVKey) = lpad(n.val, 6, "0")



"""
    CIK <: FirmIdentifier

    CIK(s::Union{<:Real, <:AbstractString})

    CIK(x::AbstractIdentifier, d::Date)


CIK is a common identifier outside of WRDS
It only contains numeric values (and is stored as an integer),
though is often represented as a string of 10 digits.
"""
struct CIK <: FirmIdentifier
    val::Int
    function CIK(s::Real)
        if s > 9_999_999_999
            error("CIK can only have 10 digits")
        end
        new(s)
    end
end

CIK(n::AbstractString) = CIK(parse(Int, n))

CIK(n::Symbol) = CIK(String(n))

value(n::CIK) = lpad(n.val, 10, "0")



"""
    Cusip6 <: FirmIdentifier

    Cusip6(s::Union{AbstractString, Symbol})

    Cusip6(x::Cusip)

    Cusip6(x::AbstractIdentifier, d::Date)

Cusip6 is the firm identifier component of [`Cusip`](@ref). It
can contain numbers or letters (with a maximum length of 6
characters) and is stored as a symbol.

!!! note
    `Cusip6` is different than [`NCusip6`](@ref). The standard in
    CRSP (and some other WRDS datasets) is that `Cusip6` represents
    the most recently available `NCusip6` for a given firm, while
    `NCusip6` will provide a historical view of that firm.
"""
struct Cusip6 <: FirmIdentifier
    val::Symbol# assumed to already be checked as 6 characters
    function Cusip6(s::AbstractString)
        if length(s) < 6
            error("Cusip6 must be 6 characters")
        end
        new(Symbol(s[1:6]))
    end
    function Cusip6(s::Symbol)
        # assumed to already be checked as 6 characters
        new(s)
    end
end


value(n::Cusip6) = String(n.val)

"""
    NCusip6 <: FirmIdentifier

    NCusip6(s::Union{AbstractString, Symbol})

    NCusip6(x::Cusip)

    NCusip6(x::AbstractIdentifier, d::Date)

NCusip6 is the firm identifier component of [`NCusip`](@ref). It
can contain numbers or letters (with a maximum length of 6
characters) and is stored as a symbol.

!!! note
    `NCusip6` is different than [`Cusip6`](@ref). The standard in
    CRSP (and some other WRDS datasets) is that `Cusip6` represents
    the most recently available `NCusip6` for a given firm, while
    `NCusip6` will provide a historical view of that firm.
"""
struct NCusip6 <: FirmIdentifier
    val::Symbol# assumed to already be checked as 6 characters
    function NCusip6(s::AbstractString)
        if length(s) < 6
            error("Cusip6 must be 6 characters")
        end
        new(Symbol(s[1:6]))
    end
    function NCusip6(s::Symbol)
        # assumed to already be checked as 6 characters
        new(s)
    end
end

value(n::NCusip6) = String(n.val)



"""
    struct Cusip <: SecurityIdentifier
        issuer::Cusip6
        issue::Symbol
        checksum::Int
    end
    
    Cusip(s::AbstractString)

    Cusip(x::AbstractIdentifier, d::Date)

`Cusip` is a common identifier within and outside of WRDS
WRDS tracks older `Cusip`s as [`NCusip`](@ref)
all `Cusip`s are made up of 3 parts, issuer (6 characters and saved as
[`Cusip6`](@ref)), issue (2 characters), and a checksum
most databases in WRDS only use the 8 characters

!!! note
    `Cusip` is different than [`NCusip`](@ref). The standard in
    CRSP (and some other WRDS datasets) is that `Cusip` represents
    the most recently available `NCusip` for a given firm, while
    `NCusip` will provide a historical view of that firm.
"""
struct Cusip <: SecurityIdentifier
    issuer::Cusip6
    issue::Symbol
    checksum::Int
    function Cusip(issuer::Cusip6, issue::Symbol, checksum::Int)
        new(issuer, issue, checksum)
    end
end

function Cusip(s::AbstractString)
    if length(s) == 9
        if luhn_checksum(s[1:8]) != parse(Int, s[9])
            @warn("Invalid Checksum in parsing Cusip, this observation\
            might not match other Cusips. To correct this error, pass\
            the first 8 characters of the string instead.")
        end
        Cusip(Cusip6(s[1:6]), Symbol(s[7:8]), parse(Int, s[9]))
    elseif length(s) == 8
        Cusip(Cusip6(s[1:6]), Symbol(s[7:8]), luhn_checksum(s))
    else
        error("Cusip must be 8 or 9 characters")
    end
end

function Cusip(issuer::AbstractString, issue::AbstractString, checksum::Int=lugn_checksum(issuer * issue))
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
        @warn("Invalid Checksum in parsing Cusip")
    end
    new(Symbol(issuer), Symbol(issue), checksum)
end


function value(n::Cusip, l::Int=8)
    out = value(n.issuer) * String(n.issue)
    if l == 9
        out * string(n.checksum)
    else
        out
    end
end

"""
    struct NCusip <: SecurityIdentifier
        issuer::NCusip6
        issue::Symbol
        checksum::Int
    end
    
    NCusip(s::AbstractString)

    NCusip(x::AbstractIdentifier, d::Date)

`NCusip` is a common identifier within and outside of WRDS
WRDS tracks the most recent `NCusip`s as [`Cusip`](@ref)
all `NCusip`s are made up of 3 parts, issuer (6 characters and saved as
[`NCusip6`](@ref)), issue (2 characters), and a checksum
most databases in WRDS only use the 8 characters

!!! note
    `NCusip` is different than [`Cusip`](@ref). The standard in
    CRSP (and some other WRDS datasets) is that `Cusip` represents
    the most recently available `NCusip` for a given firm, while
    `NCusip` will provide a historical view of that firm.
"""
struct NCusip <: SecurityIdentifier
    issuer::NCusip6
    issue::Symbol
    checksum::Int
    function NCusip(issuer::NCusip6, issue::Symbol, checksum::Int)
        new(issuer, issue, checksum)
    end
end

function NCusip(s::AbstractString)
    if length(s) == 9
        if luhn_checksum(s[1:8]) != parse(Int, s[9])
            @warn("Invalid Checksum in parsing NCusip, this observation\
            might not match other NCusips. To correct this error, pass\
            the first 8 characters of the string instead.")
        end
        NCusip(NCusip6(s[1:6]), Symbol(s[7:8]), parse(Int, s[9]))
    elseif length(s) == 8
        NCusip(NCusip6(s[1:6]), Symbol(s[7:8]), luhn_checksum(s))
    else
        error("NCusip must be 8 or 9 characters")
    end
end

function NCusip(issuer::AbstractString, issue::AbstractString, checksum::Int=luhn_checksum(issuer * issue))
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
        @warn("Invalid Checksum in parsing NCusip")
    end
    new(Symbol(issuer), Symbol(issue), checksum)
end


function value(n::NCusip, l::Int=8)
    out = value(n.issuer) * String(n.issue)
    if l == 9
        out * string(n.checksum)
    else
        out
    end
end

Cusip6(x::Cusip) = Cusip6(x.issuer)
NCusip6(x::NCusip) = NCusip6(x.issuer)



"""
    Ticker <: SecurityIdentifier

    Ticker(s::Union{AbstractString, Symbol})

    Ticker(x::AbstractIdentifier, d::Date)

Ticker is a stock market ticker that is often seen on the NYSE or other exchanges.
It typically consists of 1-4 characters and is stored as a Symbol.

!!! note
    `Ticker` should be kept distinct from [`IbesTicker`](@ref). `IbesTicker`
    is within the IBES database and often differs from `Ticker`.
"""
struct Ticker <: SecurityIdentifier
    val::Symbol
    function Ticker(s::Union{AbstractString, Symbol})
        new(Symbol(s))
    end
end

value(n::Ticker) = String(n.val)


"""
    IbesTicker <: SecurityIdentifier

    IbesTicker(s::Union{AbstractString, Symbol})

    IbesTicker(x::AbstractIdentifier, d::Date)

IbesTicker is the primary identifier in the IBES universe.
"""
struct IbesTicker <: SecurityIdentifier
    val::Symbol
    function IbesTicker(s::Union{AbstractString, Symbol})
        new(Symbol(s))
    end
end

value(n::IbesTicker) = String(n.val)


"""
    Permno <: SecurityIdentifier

    Permno(x::Real)

    Permno(x::AbstractIdentifier, d::Date)

Permno is the primary identifier of securities in the CRSP universe, it is also
one of the most common methods of linking between databases since it is easy to
find links to [`Cusip`](@ref) and Compustat ([`GVKey`](@ref)), and Cusip.
"""
struct Permno <: SecurityIdentifier
    val::Int
    Permno(x::Real) = new(Int(x))
end

value(n::Permno) = n.val

"""
    Permco <: FirmIdentifier

    Permco(x::Real)

    Permco(x::AbstractIdentifier, d::Date)

Permco is the primary firm identifier of securities in the CRSP universe.
"""
struct Permco <: FirmIdentifier
    val::Int
    Permco(x::Real) = new(Int(x))
end

value(n::Permco) = n.val

"""
    RPEntity <: FirmIdentifier

    RPEntity(x::Real)

    RPEntity(x::AbstractIdentifier, d::Date)

RPEntity is used within RavenPack to identify different entities.
"""
struct RPEntity <: FirmIdentifier
    val::Symbol
    function RPEntity(s::Union{AbstractString, Symbol})
        new(Symbol(s))
    end
end

value(n::RPEntity) = String(n.val)

"""
    SecID <: SecurityIdentifier

    SecID(x::Real)

    SecID(x::AbstractIdentifier, d::Date)

SecID is the primary identifier within the OptionMetrics database.
"""
struct SecID <: SecurityIdentifier
    val::Int
    SecID(x::Real) = new(Int(x))
end

value(n::SecID) = n.val



(::Type{ID})(x::Missing) where {ID <: AbstractIdentifier} = x
(::Type{ID})(x::ID) where {ID <: AbstractIdentifier} = x

"""
    value(x::AbstractIdentifier)

Converts an identifier into a common Julia type (typically `Int` or `String`).
"""
value(x::Missing) = x

Base.show(io::IOContext, id::AbstractIdentifier) = show(io, value(id))
Base.print(io::IO, id::AbstractIdentifier) = print(io, value(id))

