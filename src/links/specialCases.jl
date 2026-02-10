convert_identifier(::Type{Cusip6{HistCode}}, x::Cusip{HistCode}, args...; vargs...) where {HistCode} = Cusip6{HistCode}(x)
convert_identifier(::Type{Cusip6{HistCode}}, x::Cusip{HistCode}, dt::Date, args...; vargs...) where {HistCode} = Cusip6{HistCode}(x)
identifier_data(::Type{Cusip6{HistCode}}, ::Type{Cusip{HistCode}}) where {HistCode} = Dict{Cusip{HistCode}, Vector{LinkPair{Cusip{HistCode}, Cusip6{HistCode}}}}()
# CIK <-> GVKey does not require a date, so just insert any date
CIK(x::GVKey) = CIK(x, today())
GVKey(x::CIK) = GVKey(x, today())