addon_schema = "id INT, name STRING, price FLOAT, updated_at LONG"
roomtype_schema = "id INT, name STRING, price FLOAT, updated_at LONG"
guest_schema = "id INT, email STRING, dob LONG, gender STRING, updated_at LONG"
location_schema = "id INT, state STRING, country STRING, updated_at LONG"
guest_stg_schema = "id INT, location INT, updated_at LONG"
room_schema = "id INT, roomtype INT, updated_at LONG"
booking_schema = "id INT, checkin LONG, checkout LONG, updated_at LONG"
booking_room_schema = "id INT, booking INT, room INT, guest INT, updated_at LONG"
booking_addon_schema = (
    "id INT, booking_room INT, addon INT, quantity INT, datetime LONG, updated_at LONG"
)

dim_schema_map = {
    "addon": addon_schema,
    "roomtype": roomtype_schema,
    "guest": guest_schema,
    "location": location_schema,
}

stg_schema_map = {
    "booking": booking_schema,
    "booking_room": booking_room_schema,
    "booking_addon": booking_addon_schema,
}

temp_schema_map = {
    "guest": guest_stg_schema,
    "room": room_schema,
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
guest_stg_clean = (
    "id",
    "location",
    "CAST(updated_at/1000000 AS TIMESTAMP) AS updated_at",
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
    "CAST(datetime/1000000 AS TIMESTAMP) AS datetime",
    "CAST(updated_at/1000000 AS TIMESTAMP) AS updated_at",
)

dim_clean_map = {
    "addon": addon_clean,
    "roomtype": roomtype_clean,
    "guest": guest_clean,
    "location": location_clean,
}

stg_clean_map = {
    "booking": booking_clean,
    "booking_room": booking_room_clean,
    "booking_addon": booking_addon_clean,
}

temp_clean_map = {
    "room": room_clean,
    "guest": guest_stg_clean,
}

maxid_query = "SELECT COALESCE(MAX(id), 0) FROM {dim}"

dim_upsert_query = """
    MERGE INTO {dim} d USING (
        SELECT *, ROW_NUMBER() OVER(ORDER BY effective_from) AS rownum
        FROM {stage}
    ) s ON d._id = s._id AND d.effective_until IS NULL
    WHEN MATCHED THEN UPDATE SET effective_until = s.effective_from
    WHEN NOT MATCHED THEN INSERT (id, {columns}) VALUES ({maxid} + rownum, {columns})
"""

dim_delete_query = """
    MERGE INTO {dim} d USING {stage} s ON d._id = s._id AND d.effective_until IS NULL
    WHEN MATCHED THEN UPDATE SET effective_until = current_timestamp
"""

stg_delete_query = """
    MERGE INTO {stg} s USING {temp} t ON s.id = t.id
    WHEN MATCHED THEN UPDATE SET is_deleted = t.is_deleted
"""


def get_upsert_query(staging_table: str, temp_table: str, columns: list):
    return f"""
        MERGE INTO {staging_table} s USING {temp_table} t ON s.id = t.id
        WHEN MATCHED THEN UPDATE SET {", ".join(["{} = t.{}".format(col, col) for col in columns])}
        WHEN NOT MATCHED THEN INSERT ({", ".join(columns)}) VALUES ({", ".join(columns)})
    """
