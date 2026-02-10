
# Identifier Types

This page lists all identifier types provided by default. Identifiers are
thin wrappers around `String` or `Int` values that carry type information,
enabling the linking system to dispatch to the correct conversion method.

There are two categories:
- **Firm identifiers** ([`FirmIdentifier`](@ref)): identify a company (e.g., GVKey, CIK, Permco)
- **Security identifiers** ([`SecurityIdentifier`](@ref)): identify a specific security (e.g., Permno, Cusip, Ticker)

Some security identifiers have a "parent" firm identifier. For example,
[`NCusip`](@ref) (8-character CUSIP) has parent [`NCusip6`](@ref) (6-character CUSIP).
This parent relationship is used for fallback matching — see
[Parent Firms](@ref) in Default Behavior.

```@index
Pages = ["linking_identifiers.md"]
```

```@docs
AbstractIdentifier
```

## Firm Identifiers

Firm identifiers represent a company across all of its securities.

```@docs
FirmIdentifier
GVKey
CIK
Permco
Cusip6
NCusip6
HdrCusip6
RPEntity
```

## Security Identifiers

Security identifiers represent a specific stock, bond, or other instrument.

```@docs
SecurityIdentifier
Permno
Cusip
NCusip
HdrCusip
Ticker
IbesTicker
SecID
```
