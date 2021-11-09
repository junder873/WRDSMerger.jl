

create_filter(x::AbstractArray{<:Real}) = "IN ($(join(x, ", ")))"
create_filter(x::AbstractArray) = "IN ('$(join(x, "', '"))')"
create_filter(x::Missing) = "IS NOT NULL"
create_filter(x::Real) = "= $x"
create_filter(x::AbstractString) = "= '$x'"

function create_filter(
    filters::Dict{String, <:Any},
    fil = ""
)
    for (key, data) in filters
        if length(fil) > 0
            fil *= " AND "
        else
            fil *= " WHERE "
        end
        fil *= "$key $(create_filter(data))"
    end
    return fil
end



buy_hold_return(x) = prod(x .+ 1) - 1
bhar_calc(firm, market) = buy_hold_return(firm) - buy_hold_return(market)


