"""
    function list_libraries(conn::Union{LibPQ.Connection, 
        DBInterface.Connection}
    )
Load the list of Postgres schemata
the user has permission to access
"""
function list_libraries(conn::Union{LibPQ.Connection, DBInterface.Connection})
    query = """
        WITH RECURSIVE "names"("name") AS (
            SELECT n.nspname AS "name"
                FROM pg_catalog.pg_namespace n
                WHERE n.nspname !~ '^pg_'
                    AND n.nspname <> 'information_schema')
            SELECT "name"
                FROM "names"
                WHERE pg_catalog.has_schema_privilege(
                    current_user, "name", 'USAGE') = TRUE;
        """

        return run_sql_query(conn, query)
end


"""
    function check_schema_perms(conn::Union{LibPQ.Connection, 
        DBInterface.Connection}, library::String
    )::Bool
Verify that the user can access a schema
"""
function check_schema_perms(conn::Union{LibPQ.Connection, DBInterface.Connection}, library::String)
    # Verify that the library exists

    if library in list_libraries(conn).name
        return true
    else 
        schemas = run_sql_query(conn, query).schema_name
        if library in schemas
            error("You do not have permission to access the $library library")
        else
            error("The $library library is not found")
        end
    end
end

"""
    function list_tables(conn::Union{LibPQ.Connection, 
            DBInterface.Connection}, library::String
    )

List all of the views/tables/foreign tables within a schema
"""
function list_tables(conn::Union{LibPQ.Connection, DBInterface.Connection}, library::String)
    if check_schema_perms(conn, library)
        query = """SELECT table_name FROM INFORMATION_SCHEMA.views 
                    WHERE table_schema IN ('$library');"""
        return run_sql_query(conn, query)
    end
end

"""
    function approx_row_count(conn::Union{LibPQ.Connection,
        DBInterface.Connection}, library::String, table::String
    )

Get an approximate count of the number of rows in a table
"""
function approx_row_count(conn::Union{LibPQ.Connection, DBInterface.Connection}, library::String, table::String)
    if check_schema_perms(conn, library)
        query = """
            SELECT reltuples::bigint AS estimate
            FROM   pg_class
            WHERE  oid = '$library.$table'::regclass;
        """
        #TODO: We should be able to cast the result directly to int, instead of DF
        return run_sql_query(conn, query)[1, "estimate"]
    end

end


"""
    function describe_table(conn::Union{LibPQ.Connection, 
        DBInterface.Connection}, library::String, 
        table::String
    )

Get a table's description (row count, columns, column types)
"""
function describe_table(conn::Union{LibPQ.Connection, DBInterface.Connection}, library::String, table::String)
    if check_schema_perms(conn, library)
        row_count = approx_row_count(conn, library, table)
        println("There are approximately $row_count rows in $library.$table")

        query = """
            SELECT column_name, data_type, is_nullable
            FROM
                information_schema.columns
            WHERE
                table_name = '$table';
        """
        return run_sql_query(conn, query)
    end
end


"""
    function get_table(conn::Union{LibPQ.Connection, 
        DBInterface.Connection}, library::String, table::String;
        obs::Int = nothing, offset::Int = 0, cols = nothing
    )

Create a DataFrame from a table
"""
function get_table(conn::Union{LibPQ.Connection, DBInterface.Connection},
                    library::String,
                    table::String;
                    obs::Union{Nothing, Int} = nothing,
                    offset::Int = 0,
                    cols = nothing
                )
    if check_schema_perms(conn, library)

        limit = obs !== nothing && obs > 0 ? "LIMIT $obs" : ""
        columns = cols === nothing ? "*" : join(cols, ",")

        query = "SELECT $columns FROM $library.$table $limit OFFSET $offset"

        return run_sql_query(conn, query)
    end
end


"""
    function raw_sql(conn::Union{LibPQ.Connection, 
        DBInterface.Connection},
        query::String
    )
Executes raw sql code, and converts code to a DataFrame
"""
function raw_sql(conn::Union{LibPQ.Connection, DBInterface.Connection},
                query::String)
    return run_sql_query(conn, query)
end