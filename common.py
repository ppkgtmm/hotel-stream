addon_schema = "id INT, name STRING, price FLOAT, updated_at LONG"
roomtype_schema = "id INT, name STRING, price FLOAT, updated_at LONG"
guest_schema = "id INT, email STRING, dob LONG, gender STRING, updated_at LONG"
location_schema = "id INT, state STRING, country STRING, updated_at LONG"
room_schema = "id INT, roomtype INT, updated_at LONG"
booking_schema = "id INT, checkin LONG, checkout LONG, updated_at LONG"
booking_room_schema = "id INT, booking INT, room INT, guest INT, updated_at LONG"
booking_addon_schema = (
    "id INT, booking_room INT, addon INT, quantity INT, timestamp LONG, updated_at LONG"
)

schema_map = {
    "addon": addon_schema,
    "roomtype": roomtype_schema,
    "guest": guest_schema,
    "location": location_schema,
    "room": room_schema,
    "booking": booking_schema,
    "booking_room": booking_room_schema,
    "booking_addon": booking_addon_schema,
}

addon_clean = (
    "id AS _id",
    "name",
    "price",
    "CAST(updated_at/1000000 AS TIMESTAMP) AS effective_from",
)
roomtype_clean = (
    "id AS _id",
    "name",
    "price",
    "CAST(updated_at/1000000 AS TIMESTAMP) AS effective_from",
)
guest_clean = (
    "id AS _id",
    "email",
    "date_from_unix_date(dob) AS dob",
    "gender",
    "CAST(updated_at/1000000 AS TIMESTAMP) AS effective_from",
)
location_clean = (
    "id AS _id",
    "state",
    "country",
    "CAST(updated_at/1000000 AS TIMESTAMP) AS effective_from",
)
room_clean = ("id", "roomtype", "CAST(updated_at/1000000 AS TIMESTAMP) AS updated_at")
booking_clean = (
    "id",
    "date_from_unix_date(checkin) AS checkin",
    "date_from_unix_date(checkout) AS checkout",
    "CAST(updated_at/1000000 AS TIMESTAMP) AS updated_at",
)
booking_room_clean = (
    "id",
    "booking",
    "room",
    "guest",
    "CAST(updated_at/1000000 AS TIMESTAMP) AS updated_at",
)
booking_addon_clean = (
    "id",
    "booking_room",
    "addon",
    "quantity",
    "CAST(timestamp/1000000 AS TIMESTAMP) AS timestamp",
    "CAST(updated_at/1000000 AS TIMESTAMP) AS updated_at",
)
clean_map = {
    "addon": addon_clean,
    "roomtype": roomtype_clean,
    "guest": guest_clean,
    "location": location_clean,
    "room": room_clean,
    "booking": booking_clean,
    "booking_room": booking_room_clean,
    "booking_addon": booking_addon_clean,
}

dim_upsert_query = """
    UPDATE {dim}
    SET effective_until = {stage}.effective_from
    FROM {stage}
    WHERE {dim}._id = {stage}._id AND {dim}.effective_until IS NULL
"""

dim_delete_query = """
    UPDATE {dim}
    SET effective_until = current_timestamp
    FROM {stage}
    WHERE {dim}._id = {stage}._id AND {dim}.effective_until IS NULL
"""

stg_delete_query = """
    UPDATE {stg}
    SET is_deleted = {temp}.is_deleted
    FROM {temp}
    WHERE {stg}.id = {temp}.id
"""


def get_upsert_query(staging_table: str, temp_table: str, columns: list[str]):
    column_list = ["{} = {}.{}".format(col, temp_table, col) for col in columns]
    return """
        MERGE INTO {stg}
        USING {temp}
        ON {stg}.id = {temp}.id
        WHEN MATCHED THEN UPDATE SET {column_list}
        WHEN NOT MATCHED THEN INSERT 
    """.format(
        stg=staging_table, temp=temp_table, column_list=", ".join(column_list)
    )
