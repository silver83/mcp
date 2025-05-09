# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance
# with the License. A copy of the License is located at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# or in the 'license' file accompanying this file. This file is distributed on an 'AS IS' BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions
# and limitations under the License.i

"""awslabs Aurora DSQL MCP Server implementation."""

import argparse
import asyncio
import boto3
import psycopg
import sys
from loguru import logger
from mcp.server.fastmcp import FastMCP
from pydantic import Field
from typing import Annotated, Any, List, Optional


# global variables
cluster_endpoint = None
database_user = None
region = None

mcp = FastMCP(
    'Aurora DSQL MCP server. This is the starting point for all solutions created',
    dependencies=[
        'loguru',
    ],
)


@mcp.tool(name='query', description='Run a SQL query')
async def query(  # noqa: D103
    sql: Annotated[str, Field(description='The SQL query to run')],
    query_parameters: Annotated[
        Optional[List[Any]], Field(description='List of parameters for the SQL query')
    ] = None,
) -> str:
    logger.info(f'query: {sql} with {query_parameters}')

    try:
        rows = await execute_query(sql, query_parameters)
        if rows is None:
            return 'No results'
        return str(rows)
    except Exception as e:
        raise e


@mcp.tool(name='transact', description='Write or modify data using SQL, in a transaction')
async def transact(  # noqa: D103
    sql_list: Annotated[
        List[Any],
        Field(description='List of one or more SQL statements to execute in a transaction'),
    ],
) -> str:
    logger.info(f'transact: {sql_list}')

    if not sql_list:
        return ''

    conn = await create_connection()

    await execute_query('BEGIN')
    try:
        for query in sql_list:
            rows = await execute_query(query, None, conn)
        await execute_query('COMMIT')
        return str(rows)
    except Exception as e:
        await execute_query('ROLLBACK')
        raise e
    finally:
        await conn.close()


@mcp.tool(name='schema', description='Get the schema of the given table')
async def schema(table_name: Annotated[str, Field(description='name of the table')]) -> str:  # noqa: D103
    logger.info(f'schema: {table_name}')

    query = 'SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s'

    try:
        rows = await execute_query(query, [table_name])
        if rows is None:
            return 'No results'
        return str(rows)
    except Exception as e:
        raise e


@mcp.tool(name='explain', description='Explain the given SQL query using Postgres EXPLAIN ANALYZE')
async def explain(sql: Annotated[str, Field(description='SQL query to explain analyze')]) -> str:  # noqa: D103
    logger.info(f'sql: {sql}')

    query = f'EXPLAIN ANALYZE {sql}'

    try:
        rows = await execute_query(query)
        if rows is None:
            return 'No results'
        return str(rows)
    except Exception as e:
        raise e


async def create_connection():
    """Create a connection to the Aurora DSQL cluster."""
    # Generate a fresh password token for each connection, to ensure the token is not expired
    # when the connection is established
    client = boto3.client('dsql', region_name=region)

    if database_user == 'admin':
        password_token = client.generate_db_connect_admin_auth_token(cluster_endpoint, region)
    else:
        password_token = client.generate_db_connect_auth_token(cluster_endpoint, region)

    conn_params = {
        'dbname': 'postgres',
        'user': database_user,
        'host': cluster_endpoint,
        'port': '5432',
        'password': password_token,
        'application_name': 'awslabs.aurora-dsql-mcp-server',
    }

    try:
        conn = await psycopg.AsyncConnection.connect(**conn_params, autocommit=True)
    except Exception as e:
        logger.info(f'Failed to create connection due to error : {e}')
        raise e

    if database_user == 'admin':
        schema = 'public'
    else:
        schema = 'myschema'

    try:
        async with conn.cursor() as cur:
            await cur.execute(f'SET search_path = {schema};')
    except Exception as e:
        logger.info(f'Failed to prepare newly created connection due to error : {e}')
        await conn.close()
        raise e

    return conn


async def execute_query(query: str, params=None, conn_to_use=None) -> psycopg.rows:
    """Run a SQL query.

    Args:
        query: The sql statement to run
        params: The parameters to use in the sql statement
        conn_to_use: DB connection object to use. If None, a new connection will be created.

    Returns:
        The result of the query
    """
    conn = await create_connection() if conn_to_use is None else conn_to_use
    try:
        async with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            await cur.execute(query, params)
            if cur.rownumber is None:
                return 'No results'
            else:
                return await cur.fetchall()
    except Exception as e:
        logger.info(f'Failed to execute query due to error {e}')
        raise e
    finally:
        if conn_to_use is None:
            await conn.close()


def main():
    """Run the MCP server with CLI argument support."""
    parser = argparse.ArgumentParser(
        description='An AWS Labs Model Context Protocol (MCP) server for Aurora DSQL'
    )
    parser.add_argument('--sse', action='store_true', help='Use SSE transport')
    parser.add_argument('--port', type=int, default=8888, help='Port to run the server on')
    parser.add_argument(
        '--cluster_endpoint', required=True, help='Endpoint for your Aurora DSQL cluster'
    )
    parser.add_argument('--database_user', required=True, help='Database username')
    parser.add_argument(
        '--region',
        required=True,
        default='us-west-2',
        help='AWS region for Aurora DSQL Cluster (default: us-west-2)',
    )
    args = parser.parse_args()

    global cluster_endpoint
    cluster_endpoint = args.cluster_endpoint

    global region
    region = args.region

    global database_user
    database_user = args.database_user

    logger.info(
        'Aurora DSQL MCP init with CLUSTER_ENDPOINT:{}, REGION: {}, DATABASE_USER:{}',
        cluster_endpoint,
        region,
        database_user,
    )

    try:
        logger.info('Validating connection to cluster')
        asyncio.run(execute_query('SELECT 1'))
    except Exception as e:
        logger.error(
            f'Failed to create and validate db connection to Aurora DSQL. Exit the MCP server. error: {e.response["Error"]["Message"]}'
        )
        sys.exit(1)

    logger.success('Successfully validated connection to Aurora DSQL Cluster')

    # Run server with appropriate transport
    if args.sse:
        mcp.settings.port = args.port
        mcp.run(transport='sse')
    else:
        logger.info('Starting Aurora DSQL MCP server')
        mcp.run()


if __name__ == '__main__':
    main()
