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

maxid_query = "SELECT COALESCE(MAX(id), 0) FROM {dim}"

dim_upsert_query = """
    MERGE INTO {dim} d USING {stage} s ON d._id = s._id
    WHEN MATCHED AND d.effective_until IS NULL THEN UPDATE SET effective_until = s.effective_from
    WHEN NOT MATCHED THEN INSERT (id, {columns}) VALUES ({maxid} + rownum, {columns})
"""

dim_delete_query = """
    MERGE INTO {dim} d USING {stage} s ON d._id = s._id
    WHEN MATCHED AND d.effective_until IS NULL THEN UPDATE SET effective_until = current_timestamp
"""

stg_delete_query = """
    MERGE INTO {stg} s USING {temp} t ON s.id = t.id
    WHEN MATCHED THEN SET is_deleted = t.is_deleted
"""


def get_upsert_query(staging_table: str, temp_table: str, columns: list[str]):
    to_update = ["{} = {}.{}".format(col, temp_table, col) for col in columns]
    to_insert = ["{}.{}".format(temp_table, col) for col in columns]
    return """
        MERGE INTO {stg}
        USING {temp}
        ON {stg}.id = {temp}.id
        WHEN MATCHED THEN UPDATE SET {to_update}
        WHEN NOT MATCHED THEN INSERT VALUES ({to_insert})
    """.format(
        stg=staging_table,
        temp=temp_table,
        to_update=", ".join(to_update),
        to_insert=", ".join(to_insert),
    )
