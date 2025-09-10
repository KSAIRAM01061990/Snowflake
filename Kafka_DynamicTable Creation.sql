SELECT * FROM TOPIC_REGIONAL_TXN_RT_TBL

CREATE SCHEMA RPT ;


CREATE  OR REPLACE  DYNAMIC TABLE ANALYTICS.RPT.WM_STORE_TXN_SUMMARY
  TARGET_LAG =  '60  seconds '  
  WAREHOUSE = COMPUTE_WH
  AS   
with data as 
(select parse_json(record_content) rc 
 from ANALYTICS.KAFKA_SCHEMA.TOPIC_REGIONAL_TXN_RT_TBL
)
,intm as (select rc:publisher_id::string as store_id,
            rc:publisher_name::string as store_name,
            rc:publisher_addres::string as store_address,
            rc:txn_id::string as txn_id,
            rc:txn_amount::int as txn_amount
from data )

select store_id,
        store_name,
        count(*) as txn_count,
        sum(txn_amount) as total_txn_amount
        from intm
        group by all
        order by store_id;

SELECT * FROM ANALYTICS.RPT.WM_STORE_TXN_SUMMARY