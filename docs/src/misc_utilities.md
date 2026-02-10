# Utilities

## Range Joins

[`range_join`](@ref) performs inequality-based joins between DataFrames,
useful when matching on date ranges or other interval conditions.
Conditions are specified with the [`Conditions`](@ref) struct.

```@docs
Conditions
range_join
```

## Internal Helpers

```@docs
WRDSMerger.check_schema_perms
WRDSMerger.approx_row_count
WRDSMerger.modify_col!
```
