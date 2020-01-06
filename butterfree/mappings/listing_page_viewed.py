from pyspark.sql.types import (
    StructType,
    StringType,
    LongType,
    IntegerType,
    BooleanType,
    FloatType,
)

from quintoandar_butterfree.core.input.kafka.event_mapping import EventMapping


class ListingPageViewedMapping(EventMapping):
    topic = "snowplow-pwa-tenants-listing_page_viewed"
    schema = (
        StructType()
        .add("schema", StringType(), False)
        .add(
            "data",
            StructType()
            .add("schema", StringType(), False)
            .add(
                "data",
                StructType()
                .add("event_name", StringType(), False)
                .add(
                    "user",
                    StructType().add("amplitude_device_id", StringType(), False),
                    False,
                )
                .add(
                    "event",
                    StructType()
                    .add("house_id", LongType(), True)
                    .add("valor_aluguel", IntegerType(), True)
                    .add("valor_condominio", IntegerType(), True)
                    .add("valor_total", IntegerType(), True)
                    .add("nbr_bathrooms", IntegerType(), True)
                    .add("nbr_dorms", IntegerType(), True)
                    .add("accepts_pets", BooleanType(), True)
                    .add("is_prox_metro", BooleanType(), True)
                    .add("is_furnished", BooleanType(), True)
                    .add("type_property", StringType(), True)
                    .add("nbr_parking_slots", IntegerType(), True)
                    .add("m2", IntegerType(), True)
                    .add("sub_region", StringType(), True)
                    .add("sub_region_id", LongType(), True)
                    .add("macro_region", StringType(), True)
                    .add("macro_region_id", LongType(), True)
                    .add("city", StringType(), True)
                    .add("state", StringType(), True)
                    .add("country", StringType(), True)
                    .add("is_exclusive", BooleanType(), True)
                    .add("closing_probability", FloatType(), True)
                    .add("has_listing_video", BooleanType(), True)
                    .add("has_description", BooleanType(), True)
                    .add("has_info_furniture", BooleanType(), True)
                    .add("has_info_negotiation", BooleanType(), True)
                    .add("allow_negotiation", BooleanType(), True)
                    .add("display_allow_negotiation", BooleanType(), True)
                    .add("visit_status", StringType(), True)
                    .add("directoffer_active", BooleanType(), True)
                    .add("imovel_publication", StringType(), True)
                    .add("business_context", StringType(), True),
                    False,
                ),
                True,
            ),
            False,
        )
    )
    filter_query = (
        "SELECT "
        "   json.data.data.user AS user, "
        "   json.data.data.event AS event, "
        "   current_timestamp as timestamp "
        "FROM dataframe "
        "WHERE json.data.data.event.business_context = 'rent'"
    )
