"""
Supertype for all Firm Identifiers
"""
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

    GVKey(s::Union{<:Real, <:AbstractString})::GVKey

    GVKey(x::AbstractIdentifier, d::Date)::String


GVKey is the primary identifier in the Compustat universe
It only contains numeric values, though is often represented as a
string with 6 digits, therefore, it is stored as a String.

## Example

```jldoctest
julia> GVKey(2968) # GVKey for Chase
GVKey("002968")

julia> GVKey("002968")
GVKey("002968")

julia> GVKey(Permno(47896), Date(2020))
"002968"
```
"""
struct GVKey <: FirmIdentifier
    val::String
    function GVKey(s::AbstractString)
        if length(s) > 6
            error("GVKey can only have 6 digits")
        end
        new(lpad(String(s), 6, "0"))
    end
end

function GVKey(n::Real)
    if n > 999999
        error("GVKey can only have 6 digits")
    end
    GVKey(lpad(n, 6, "0"))
end

value(n::GVKey) = n.val



"""
    CIK <: FirmIdentifier

    CIK(s::Union{<:Real, <:AbstractString})::CIK

    CIK(x::AbstractIdentifier, d::Date)::String


CIK is a common identifier outside of WRDS
It only contains numeric values, though is often represented as a
string with 10 digits, therefore, it is stored as a String.

## Example
```jldoctest
julia> CIK(19617) # CIK for Chase
CIK("0000019617")

julia> CIK("0000019617")
CIK("0000019617")

julia> CIK(GVKey(2968), Date(2020))
"0000019617"

julia> CIK(GVKey(2968)) # Date for GVKey <-> CIK is unnecessary
"0000019617"
```
"""
struct CIK <: FirmIdentifier
    val::String
    function CIK(s::AbstractString)
        if length(s) > 10
            error("CIK can only have 10 digits")
        end
        new(lpad(String(s), 10, "0"))
    end
end

function CIK(n::Real)
    if n > 9_999_999
        error("CIK can only have 10 digits")
    end
    CIK(lpad(n, 10, "0"))
end

value(n::CIK) = n.val



"""
    Cusip6 <: FirmIdentifier

    Cusip6(s::AbstractString)::Cusip6

    Cusip6(x::Cusip)::Cusip6

    Cusip6(x::AbstractIdentifier, d::Date)::String

Cusip6 is the firm identifier component of [`Cusip`](@ref). It
can contain numbers or letters (with a maximum length of 6
characters).

!!! note
    `Cusip6` is a parameterized type: `NCusip6 = Cusip6{:historical}` and
    `HdrCusip6 = Cusip6{:current}`. Calling `Cusip6(x)` defaults to
    historical (`NCusip6`).

    In CRSP V1, the column `cusip` (first 6 chars) was the current/header
    value and `ncusip` (first 6 chars) was historical. In CRSP V2, the column
    `cusip` (first 6 chars) is historical and `hdrcusip` (first 6 chars) is
    the current value.

## Example

```jldoctest
julia> HdrCusip6("46625H") # Cusip6 for Chase
HdrCusip6("46625H")

julia> HdrCusip6(HdrCusip("46625H10")) # Cusip6 is the first 6 digits of a Cusip
HdrCusip6("46625H")

julia> HdrCusip6(Permno(47896), Date(2020))
"46625H"
```

Related to the note on the difference between `Cusip6` and `NCusip6`:
```jldoctest
julia> HdrCusip6(Permno(47896), Date(2020))
"46625H"

julia> NCusip6(Permno(47896), Date(2020))
"46625H"

julia> HdrCusip6(Permno(47896), Date(1998))
"46625H"

julia> NCusip6(Permno(47896), Date(1998))
"16161A"
```
"""
struct Cusip6{HistCode} <: FirmIdentifier
    val::String
    function Cusip6(s::AbstractString; historical=true)
        if length(s) < 6
            error("Cusip6 must be 6 characters")
        end
        if historical
            new{:historical}(String(s[1:6]))
        else
            new{:current}(String(s[1:6]))
        end
    end
    function Cusip6{HistCode}(s::AbstractString) where {HistCode}
        if length(s) < 6
            error("Cusip6 must be 6 characters")
        end
        new{HistCode}(String(s[1:6]))
    end
end

value(n::Cusip6) = n.val

"""
    NCusip6 <: FirmIdentifier

    NCusip6(s::AbstractString)::NCusip6

    NCusip6(x::NCusip)::NCusip6

    NCusip6(x::AbstractIdentifier, d::Date)::String

NCusip6 is the firm identifier component of [`NCusip`](@ref). It
can contain numbers or letters (with a maximum length of 6
characters).

!!! note
    `NCusip6` is from CRSP v1 where it would provide a historical view
    of the first 6 characters of the Cusip, while Cusip6 would provide
    the most recently available 6 characters.

    In CRSP v2, Cusip6 would represent the historical information while
    HdrCusip6 would represent the most recently available 6 characters.


## Example

```jldoctest
julia> NCusip6("46625H") # NCusip6 for Chase
NCusip6("46625H")

julia> NCusip6(NCusip("46625H10")) # NCusip6 is the first 6 digits of a Cusip
NCusip6("46625H")

julia> NCusip6(Permno(47896), Date(2020))
"46625H"
```

Related to the note on the difference between `Cusip6` and `NCusip6`:
```jldoctest
julia> HdrCusip6(Permno(47896), Date(2020))
"46625H"

julia> NCusip6(Permno(47896), Date(2020))
"46625H"

julia> HdrCusip6(Permno(47896), Date(1998))
"46625H"

julia> NCusip6(Permno(47896), Date(1998))
"16161A"
```
"""
const NCusip6 = Cusip6{:historical}

const HdrCusip6 = Cusip6{:current}


"""
    Cusip <: SecurityIdentifier
    
    Cusip(s::AbstractString)::Cusip

    Cusip(issuer::AbstractString, issue::AbstractString, checksum=nothing)::Cusip

    Cusip(x::AbstractIdentifier, d::Date)::String

`Cusip` is a common identifier within and outside of WRDS
WRDS tracks older `Cusip`s as [`NCusip`](@ref)
all `Cusip`s are made up of 3 parts, issuer (first 6 characters),
issue (next 2 characters), and a checksum
most databases in WRDS only use the 8 characters. If 9 digits are passed
or the checksum is explicitly passed, the checksum is validated and
a warning is given if it is an invalid checksum.

!!! note
    `Cusip` is a parameterized type: `NCusip = Cusip{:historical}` and
    `HdrCusip = Cusip{:current}`. Calling `Cusip(x)` defaults to
    historical (`NCusip`).

    In CRSP V1, the column `cusip` was the current/header value and
    `ncusip` was historical. In CRSP V2, the column `cusip` is historical
    and `hdrcusip` is the current value. The default of `Cusip(x)` returning
    a historical identifier is consistent with the V2 convention and is
    generally the safer default since the inexact date matching
    (see [`choose_best_match`](@ref)) will still find the correct link
    when only one match exists.

!!! note
    `Cusip` only stores 8 characters (not the checksum digit) and,
    by default, only returns those 8 characters. This means that if
    using a join function on a set of Cusips with 9 characters and
    the default output, there will be no matches. It is easiest to
    shorten the 9 digit Cusips to 8 digits before joining.

## Example

```jldoctest
julia> HdrCusip("46625H10") # Cusip for Chase
HdrCusip("46625H10")

julia> HdrCusip("46625H", "10") # can also provide the parts separately
HdrCusip("46625H10")

julia> HdrCusip(Permno(47896), Date(2020))
"46625H10"
```

Related to the note on the difference between `Cusip` and `NCusip`:
```jldoctest
julia> HdrCusip(Permno(47896), Date(2020))
"46625H10"

julia> NCusip(Permno(47896), Date(2020))
"46625H10"

julia> HdrCusip(Permno(47896), Date(1998))
"46625H10"

julia> NCusip(Permno(47896), Date(1998))
"16161A10"
```
"""
struct Cusip{HistCode} <: SecurityIdentifier
    val::String
    function Cusip(s::AbstractString; historical=true)
        if length(s) < 8
            error("Too few characters for Cusip")
        end
        if length(s) > 9
            error("Too many characters for Cusip")
        end
        if length(s) == 9 && luhn_checksum(s[1:8]) != parse(Int, s[9])
            @warn("Invalid Checksum in parsing Cusip, this observation " *
            "might not match other Cusips. To correct this error, pass " *
            "the first 8 characters of the string instead.")
        end
        if historical
            new{:historical}(s[1:8])
        else
            new{:current}(s[1:8])
        end
    end
    function Cusip{HistCode}(s::AbstractString) where {HistCode}
        if length(s) < 8
            error("Too few characters for Cusip")
        end
        if length(s) > 9
            error("Too many characters for Cusip")
        end
        if length(s) == 9 && luhn_checksum(s[1:8]) != parse(Int, s[9])
            @warn("Invalid Checksum in parsing Cusip, this observation " *
            "might not match other Cusips. To correct this error, pass " *
            "the first 8 characters of the string instead.")
        end
        new{HistCode}(s[1:8])
    end
    function Cusip(s::AbstractString, issue::AbstractString, checksum=nothing; historical=true)
        if length(s) != 6
            error("Issuer identification must be 6 characters")
        end
        if length(issue) != 2
            error("Issue must be 2 characters")
        end
        if checksum !== nothing && (!)(0 ≤ checksum ≤ 9)
            error("Checksum must be between 0 and 9 (inclusive)")
        elseif checksum !== nothing && luhn_checksum(s * issue) != checksum
            @warn("Invalid Checksum in parsing Cusip")
        end
        Cusip(s * issue; historical=historical)
    end
    function Cusip{HistCode}(s::AbstractString, issue::AbstractString, checksum=nothing) where {HistCode}
        if length(s) != 6
            error("Issuer identification must be 6 characters")
        end
        if length(issue) != 2
            error("Issue must be 2 characters")
        end
        if checksum !== nothing && (!)(0 ≤ checksum ≤ 9)
            error("Checksum must be between 0 and 9 (inclusive)")
        elseif checksum !== nothing && luhn_checksum(s * issue) != checksum
            @warn("Invalid Checksum in parsing Cusip")
        end
        new{HistCode}(s * issue)
    end
end


function value(n::Cusip, l::Int=8)
    out = n.val
    if l == 9
        out * string(luhn_checksum(n.val))
    else
        out
    end
end

"""
    NCusip <: SecurityIdentifier
        
    NCusip(s::AbstractString)::NCusip

    NCusip(issuer::AbstractString, issue::AbstractString, checksum=nothing)::NCusip

    NCusip(x::AbstractIdentifier, d::Date)::String

`NCusip` is a type alias for `Cusip{:historical}`, representing the
historical Cusip identifier for a security. It is a common identifier
within and outside of WRDS.

All `NCusip`s are made up of 3 parts: issuer (first 6 characters),
issue (next 2 characters), and a checksum.
Most databases in WRDS only use the 8 characters. If 9 digits are passed
or the checksum is explicitly passed, the checksum is validated and
a warning is given if it is an invalid checksum.

!!! note
    In CRSP V1, `ncusip` was the column name for the historical Cusip.
    In CRSP V2, the plain `cusip` column is the historical value (and
    maps to `NCusip`), while `hdrcusip` is the current/header value
    (mapped to [`HdrCusip`](@ref)). `NCusip` is equivalent to calling
    `Cusip(x)` (which defaults to historical).

!!! note
    `NCusip` only stores 8 characters (not the checksum digit) and,
    by default, only returns those 8 characters. This means that if
    using a join function on a set of NCusips with 9 characters and
    the default output, there will be no matches. It is easiest to
    shorten the 9 digit NCusips to 8 digits before joining.

## Example

```jldoctest
julia> NCusip("46625H10") # NCusip for Chase
NCusip("46625H10")

julia> NCusip("46625H", "10") # can also provide the parts separately
NCusip("46625H10")

julia> NCusip(Permno(47896), Date(2020))
"46625H10"
```

Related to the note on the difference between `Cusip` and `NCusip`:
```jldoctest
julia> HdrCusip(Permno(47896), Date(2020))
"46625H10"

julia> NCusip(Permno(47896), Date(2020))
"46625H10"

julia> HdrCusip(Permno(47896), Date(1998))
"46625H10"

julia> NCusip(Permno(47896), Date(1998))
"16161A10"
```
"""
const NCusip = Cusip{:historical}
const HdrCusip = Cusip{:current}

Cusip6{HistCode}(c::Cusip{HistCode}) where {HistCode} = Cusip6{HistCode}(value(c))



"""
    Ticker <: SecurityIdentifier

    Ticker(s::AbstractString)::Ticker

    Ticker(x::AbstractIdentifier, d::Date)::String

Ticker is a stock market ticker that is often seen on the NYSE or other exchanges.
It typically consists of 1-4 characters.

!!! note
    `Ticker` should be kept distinct from [`IbesTicker`](@ref). `IbesTicker`
    is within the IBES database and often differs from `Ticker`.

## Example

```jldoctest
julia> Ticker("JPM") # Ticker for Chase
Ticker("JPM")

julia> Ticker(Permno(47896), Date(2020))
"JPM"
```
"""
struct Ticker <: SecurityIdentifier
    val::String
    function Ticker(s::Union{AbstractString, Symbol})
        new(String(s))
    end
end

value(n::Ticker) = n.val


"""
    IbesTicker <: SecurityIdentifier

    IbesTicker(s::AbstractString)::IbesTicker

    IbesTicker(x::AbstractIdentifier, d::Date)::String

IbesTicker is the primary identifier in the IBES universe and typically
consists of 1-4 characters.

## Example

```jldoctest
julia> IbesTicker("CHL") # IbesTicker for Chase
IbesTicker("CHL")

julia> IbesTicker(Permno(47896), Date(2020))
"CHL"
```
"""
struct IbesTicker <: SecurityIdentifier
    val::String
    function IbesTicker(s::Union{AbstractString, Symbol})
        new(String(s))
    end
end

value(n::IbesTicker) = n.val


"""
    Permno <: SecurityIdentifier

    Permno(x::Real)::Permno

    Permno(x::AbstractIdentifier, d::Date)::Int

Permno is the primary security identifier in the CRSP universe, it is also
one of the most common methods of linking between databases since it is easy to
find links to [`Cusip`](@ref) and Compustat ([`GVKey`](@ref)), and Cusip.

## Example

```jldoctest
julia> Permno(47896) # Permno for Chase
Permno(47896)

julia> Permno(NCusip6("46625H"), Date(2020))
47896
```
"""
struct Permno <: SecurityIdentifier
    val::Int
    Permno(x::Real) = new(Int(x))
end

value(n::Permno) = n.val

"""
    Permco <: FirmIdentifier

    Permco(x::Real)::Permco

    Permco(x::AbstractIdentifier, d::Date)::Int

Permco is the primary firm identifier in the CRSP universe.

## Example

```jldoctest
julia> Permco(20436) # Permco for Chase
Permco(20436)

julia> Permco(NCusip6("46625H"), Date(2020))
20436
```
"""
struct Permco <: FirmIdentifier
    val::Int
    Permco(x::Real) = new(Int(x))
end

value(n::Permco) = n.val

"""
    RPEntity <: FirmIdentifier

    RPEntity(x::String)::RPEntity

    RPEntity(x::AbstractIdentifier, d::Date)::String

RPEntity is used within RavenPack to identify different entities.

## Example

```jldoctest
julia> RPEntity("619882") # RPEntity for Chase
RPEntity("619882")

julia> RPEntity(NCusip6("46625H"), Date(2020))
"619882"

julia> NCusip6(RPEntity("619882"), Date(2020))
"46625H"
```

!!! note
    The RavenPack links are especially messy, for example, there are two links for
    RPEntity -> NCusip6 from 2001-01-01 - 2001-05-31, and there is not easy way
    to distinguish these. This package simply returns the first value in such cases

```jldoctest
julia> RPEntity(NCusip6("46625H"), Date(2001, 3))
"619882"

julia> RPEntity(NCusip6("616880"), Date(2001, 3))
"619882"

julia> NCusip6(RPEntity("619882"), Date(2001, 3))
"46625H"
```
"""
struct RPEntity <: FirmIdentifier
    val::String
    function RPEntity(s::AbstractString)
        new(String(s))
    end
end

value(n::RPEntity) = n.val

"""
    SecID <: SecurityIdentifier

    SecID(x::Real)::SecID

    SecID(x::AbstractIdentifier, d::Date)::Int

SecID is the primary identifier within the OptionMetrics database.

```jldoctest
julia> SecID(102936) # SecID for Chase
SecID(102936)

julia> SecID(NCusip("46625H10"), Date(2020))
102936
```
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

# Base.show(io::IOContext, id::AbstractIdentifier) = show(io, value(id))
# Base.print(io::IO, id::AbstractIdentifier) = print(io, value(id))

