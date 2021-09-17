
struct BDay <: DatePeriod
    value::Int64
    calendar::Union{Symbol, String}
    BDay(v::Number, cal::Union{Symbol, String}) = new(v, cal)
end

Base.:(+)(dt::Date, z::BDay) = advancebdays(z.calendar, dt, z.value)
Base.:(-)(dt::Date, z::BDay) = advancebdays(z.calendar, dt, -1 * z.value)

    