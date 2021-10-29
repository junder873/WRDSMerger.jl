"""
function list_libraries(conn::Union{LibPQ.Connection, 
                    DBInterface.Connection})::DataFrame

Load the list of Postgres schemata
the user has permission to access
"""
function list_libraries(conn::Union{LibPQ.Connection, DBInterface.Connection})::DataFrame
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

        return execute(conn, query) |> DataFrame
end


"""
function check_schema_perms(conn::Union{LibPQ.Connection, 
                DBInterface.Connection}, library::String)::Bool
Verify that the user can access a schema
"""
function check_schema_perms(conn::Union{LibPQ.Connection, DBInterface.Connection}, library::String)::Bool
    # Verify that the library exists

    if library in list_libraries(conn).name
        return true
    else 
        schemas = DataFrame(execute(conn, query)).schema_name
        if library in schemas
            error("You do not have permission to access the $library library")
        else
            error("The $library library is not found")
        end
    end
end

"""
function list_tables(conn::Union{LibPQ.Connection, 
            DBInterface.Connection}, library::String)::DataFrame

List all of the views/tables/foreign tables within a schema
"""
function list_tables(conn::Union{LibPQ.Connection, DBInterface.Connection}, library::String)::DataFrame
    if check_schema_perms(conn, library)
        query = """SELECT table_name FROM INFORMATION_SCHEMA.views 
                    WHERE table_schema IN ('$library');"""
        return execute(conn, query) |> DataFrame
    end
end

"""
function approx_row_count(conn::Union{LibPQ.Connection,
             DBInterface.Connection}, library::String, table::String)::Int

Get an approximate count of the number of rows in a table
"""
function approx_row_count(conn::Union{LibPQ.Connection, DBInterface.Connection}, library::String, table::String)::Int
    if check_schema_perms(conn, library)
        query = """
            SELECT reltuples::bigint AS estimate
            FROM   pg_class
            WHERE  oid = '$library.$table'::regclass;
        """
        #TODO: We should be able to cast the result directly to int, instead of DF
        return DataFrame(execute(conn, query))[1, "estimate"]
    end

end


"""
function describe_table(conn::Union{LibPQ.Connection, 
            DBInterface.Connection}, library::String, 
            table::String)::DataFrame

Get a table's description (row count, columns, column types)
"""
function describe_table(conn::Union{LibPQ.Connection, DBInterface.Connection}, library::String, table::String)::DataFrame
    if check_schema_perms(conn, library)
        row_count = approx_row_count(conn::LibPQ.Connection, library::String, table::String)
        println("There are approximately $row_count rows in $library.$table")

        query = """
            SELECT column_name, data_type, is_nullable
            FROM
                information_schema.columns
            WHERE
                table_name = '$table';
        """
        return execute(conn, query) |> DataFrame
    end
end


"""
function get_table(conn::Union{LibPQ.Connection, 
            DBInterface.Connection}, library::String, table::String;

Create a DataFrame from a table
"""
function get_table(conn::Union{LibPQ.Connection, DBInterface.Connection}, library::String, table::String;
                    obs::Int = nothing, offset::Int = 0,
                    cols = nothing)::DataFrame
    if check_schema_perms(conn, library)
        columns = "*"
        limit = ""
        if cols !== nothing
            columns = join(cols, ",")
        end

        if obs !== nothing || obs > 0
            limit = "LIMIT $obs"
        end

        query = "SELECT $columns FROM $library.$table $limit OFFSET $offset"

        return execute(conn, query) |> DataFrame
    end
end


"""
function raw_sql(conn::Union{LibPQ.Connection, 
                DBInterface.Connection},
                query::String)::DataFrame
Executes raw sql code, and converts code to a DataFrame
"""
function raw_sql(conn::Union{LibPQ.Connection, DBInterface.Connection},
                query::String)::DataFrame
    return execute(conn, query) |> DataFrame
end