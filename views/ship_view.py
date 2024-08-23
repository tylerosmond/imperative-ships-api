import sqlite3
import json


def update_ship(id, ship_data):
    with sqlite3.connect("./shipping.db") as conn:
        db_cursor = conn.cursor()

        db_cursor.execute(
            """
            UPDATE Ship
                SET
                    name = ?,
                    hauler_id = ?
            WHERE id = ?
            """,
            (ship_data["name"], ship_data["hauler_id"], id),
        )

        rows_affected = db_cursor.rowcount

    return True if rows_affected > 0 else False


def delete_ship(pk):
    with sqlite3.connect("./shipping.db") as conn:
        conn.row_factory = sqlite3.Row
        db_cursor = conn.cursor()

        # Write the SQL query to get the information you want
        db_cursor.execute(
            """
        DELETE FROM Ship WHERE id = ?
        """,
            (pk,),
        )
        number_of_rows_deleted = db_cursor.rowcount

    return True if number_of_rows_deleted > 0 else False


def list_ships(url):
    # Open a connection to the database
    with sqlite3.connect("./shipping.db") as conn:
        conn.row_factory = sqlite3.Row
        db_cursor = conn.cursor()

        # Check if the '_expand' query string parameter exists in the URL
        if "_expand" in url["query_params"]:
            # SQL query to join Ship and Hauler tables
            db_cursor.execute(
                """
                SELECT
                    s.id,
                    s.name,
                    s.hauler_id,
                    h.id AS haulerId,
                    h.name AS haulerName,
                    h.dock_id
                FROM Ship s
                JOIN Hauler h
                    ON h.id = s.hauler_id
                """
            )
        else:
            # Original SQL query if '_expand' is not present
            db_cursor.execute(
                """
                SELECT
                    s.id,
                    s.name,
                    s.hauler_id
                FROM Ship s
                """
            )

        # Fetch all results from the executed query
        query_results = db_cursor.fetchall()

        # Initialize an empty list to hold the ship dictionaries
        ships = []

        # Loop through each row in the query results
        for row in query_results:
            if "_expand" in url["query_params"]:
                # Build the hauler dictionary
                hauler = {
                    "id": row["haulerId"],
                    "name": row["haulerName"],
                    "dock_id": row["dock_id"],
                }
                # Build the ship dictionary, including the hauler dictionary
                ship = {
                    "id": row["id"],
                    "name": row["name"],
                    "hauler_id": row["hauler_id"],
                    "hauler": hauler,
                }
            else:
                # If '_expand' is not present, build a simpler ship dictionary
                ship = {
                    "id": row["id"],
                    "name": row["name"],
                    "hauler_id": row["hauler_id"],
                }

            # Append the ship dictionary to the list of ships
            ships.append(ship)

        # Serialize the list of ships to a JSON encoded string
        serialized_ships = json.dumps(ships)

    return serialized_ships


def retrieve_ship(pk, url):
    # Open a connection to the database
    with sqlite3.connect("./shipping.db") as conn:
        conn.row_factory = sqlite3.Row
        db_cursor = conn.cursor()

        # Check if the '_expand' query string parameter exists in the URL
        if "_expand" in url["query_params"]:
            # SQL query to join Ship and Hauler tables for the specified ship
            db_cursor.execute(
                """
                SELECT
                    s.id,
                    s.name,
                    s.hauler_id,
                    h.id AS haulerId,
                    h.name AS haulerName,
                    h.dock_id
                FROM Ship s
                JOIN Hauler h
                    ON h.id = s.hauler_id
                WHERE s.id = ?
                """,
                (pk,),
            )
        else:
            # Original SQL query if '_expand' is not present
            db_cursor.execute(
                """
                SELECT
                    s.id,
                    s.name,
                    s.hauler_id
                FROM Ship s
                WHERE s.id = ?
                """,
                (pk,),
            )

        # Fetch the single result from the executed query
        query_results = db_cursor.fetchone()

        # Check if the query returned a result
        if query_results:
            if "_expand" in url["query_params"]:
                # Build the hauler dictionary
                hauler = {
                    "id": query_results["haulerId"],
                    "name": query_results["haulerName"],
                    "dock_id": query_results["dock_id"],
                }
                # Build the ship dictionary, including the hauler dictionary
                ship = {
                    "id": query_results["id"],
                    "name": query_results["name"],
                    "hauler_id": query_results["hauler_id"],
                    "hauler": hauler,
                }
            else:
                # If '_expand' is not present, build a simpler ship dictionary
                ship = {
                    "id": query_results["id"],
                    "name": query_results["name"],
                    "hauler_id": query_results["hauler_id"],
                }

            # Serialize the ship dictionary to a JSON encoded string
            serialized_ship = json.dumps(ship)
        else:
            serialized_ship = json.dumps(
                {}
            )  # Return an empty JSON object if no ship was found

    return serialized_ship


def create_ship(ship_data):
    with sqlite3.connect("./shipping.db") as conn:
        db_cursor = conn.cursor()

        db_cursor.execute(
            """
            INSERT INTO Ship
                (name, hauler_id)
            VALUES
                (?, ?)
            """,
            (ship_data["name"], ship_data["hauler_id"]),
        )

        id = db_cursor.lastrowid

        # Return the newly created ship with its assigned ID
        ship_data["id"] = id
        return json.dumps(ship_data)
