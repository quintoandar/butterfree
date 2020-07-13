##Feature Set
- name: feature_set
- description: description
- source: 
```json
[{'reader': 'Table Reader', 'location': 'db.table'}, {'reader': 'File Reader', 'location': 'path'}]
```
- sink:
```json
[{'writer': 'Historical Feature Store Writer'}, {'writer': 'Online Feature Store Writer'}]
```
- features:
```json
[{'column_name': 'user_id', 'data_type': 'IntegerType', 'description': "The user's Main ID or device ID"}, {'column_name': 'timestamp', 'data_type': 'TimestampType', 'description': 'Time tag for the state of all features.'}, {'column_name': 'listing_page_viewed__rent_per_month__avg_over_7_days_fixed_windows', 'data_type': 'FloatType', 'description': 'Average of something.'}, {'column_name': 'listing_page_viewed__rent_per_month__avg_over_2_weeks_fixed_windows', 'data_type': 'FloatType', 'description': 'Average of something.'}]
```