convert_identifier(::Type{NCusip6}, x::NCusip, args...; vargs...) = NCusip6(x)
convert_identifier(::Type{Cusip6}, x::Cusip, args...; vargs...) = Cusip6(x)
convert_identifier(::Type{NCusip6}, x::NCusip, dt::Date, args...; vargs...) = NCusip6(x)
convert_identifier(::Type{Cusip6}, x::Cusip, dt::Date, args...; vargs...) = Cusip6(x)
# CIK <-> GVKey does not require a date, so just insert any date
CIK(x::GVKey) = CIK(x, today())
GVKey(x::CIK) = GVKey(x, today())