from quintoandar_butterfree.core.data_types import FeatureType
from quintoandar_butterfree.core.feature import Feature
from quintoandar_butterfree.core.feature_set import FeatureSet
from quintoandar_butterfree.core.output.cassandra import Cassandra
from quintoandar_butterfree.core.source import Source
from quintoandar_butterfree.input.kafka import Kafka

feature_set = FeatureSet(
    source=Source(
        views={"lpv": Kafka("listing_page_viewed")},
        query=(
            "SELECT "
            "   lpv.user.amplitude_device_id as user_id, "
            "   lpv.event.house_id as house_id, "
            "   lpv.event.valor_aluguel as listing_page_viewed__rent_per_month ,"
            "   lpv.timestamp as timestamp "
            "FROM "
            "   lpv "
        ),
    ),
    features=[
        Feature(
            column="user_id",
            data_type=FeatureType.TEXT,
            description="The user's Main ID or device ID",
        ),
        Feature(
            column="listing_page_viewed__rent_per_month",
            aggregations=["avg", "stddev"],
            group_by="user_id",
            windows=["5 minutes", "15 minutes", "1 hour", "1 day", "7 days"],
            slide_duration="5 minutes",
            data_type=FeatureType.FLOAT,
            description="Average of the rent per month value of listings viewed",
        ),
        Feature(
            column="timestamp",
            data_type=FeatureType.TIMESTAMP,
            description="The event processing timestamp",
        ),
    ],
    outputs=[Cassandra(column_family="experimental_user")],
)

if __name__ == "__main__":
    feature_set.run()
