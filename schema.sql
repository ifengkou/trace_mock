CREATE TABLE kudu.default.trace001 (
"$day"                  int                  WITH ( primary_key = true ) ,
"event_id"              int                  WITH ( primary_key = true ) ,
"user_id"               varchar              WITH ( primary_key = true ) ,
"event_time"            bigint               WITH ( primary_key = true ) ,
"$id"                   int                  WITH ( primary_key = true ) ,
"event_name"            varchar              WITH ( nullable = true ) ,
"$device_type"          varchar              WITH ( nullable = true ) ,
"$os"                   varchar              WITH ( nullable = true ) ,
"$os_version"           varchar              WITH ( nullable = true ) ,
"$model"                varchar              WITH ( nullable = true ) ,
"$brand"                varchar              WITH ( nullable = true ) ,
"$manufacturer"         varchar              WITH ( nullable = true ) ,
"$browser"              varchar              WITH ( nullable = true ) ,
"$browser_version"      varchar              WITH ( nullable = true ) ,
"$lib"                  varchar              WITH ( nullable = true ) ,
"$ip"                   varchar              WITH ( nullable = true ) ,
"$network"              varchar              WITH ( nullable = true ) ,
"$screen_width"         int                  WITH ( nullable = true ) ,
"$screen_height"        int                  WITH ( nullable = true ) ,
"$channel"              varchar              WITH ( nullable = true ) ,
"pagename"              varchar              WITH ( nullable = true ) ,
"uri_path"              varchar              WITH ( nullable = true ) ,
"time_in"               decimal(10,3)        WITH ( nullable = true ) ,
"time_load"             decimal(10,3)        WITH ( nullable = true ) ,
"time_ux"               decimal(10,3)        WITH ( nullable = true ) ,
"is_success"            boolean              WITH ( nullable = true ) ,
"login_method"          varchar              WITH ( nullable = true ) ,
"business_module"       varchar              WITH ( nullable = true ) ,
"phone"                 varchar              WITH ( nullable = true ) ,
"fund_name"             varchar              WITH ( nullable = true ) ,
"risk_level"            varchar              WITH ( nullable = true ) ,
"fund_type"             varchar              WITH ( nullable = true ) ,
"return_rate_6m"        decimal(10,3)        WITH ( nullable = true ) ,
"purchase_amout"        int                  WITH ( nullable = true ) ,
"is_first"              boolean              WITH ( nullable = true ) ,
"risk_level_result"     varchar              WITH ( nullable = true ) ,
"is_show"               boolean              WITH ( nullable = true ) ,
"title"                 varchar              WITH ( nullable = true ) ,
"pop_type"              varchar              WITH ( nullable = true ) 
) 
 WITH (
    number_of_replicas = 1,
    partition_by_hash_buckets = 3,
    partition_by_hash_columns = ARRAY['user_id'],
    partition_by_range_columns = ARRAY['$day'],
    range_partitions = '[{"lower":null,"upper":20220701},{"lower":20220701,"upper":20220702},{"lower":20220702,"upper":20220703},{"lower":20220703,"upper":20220704},{"lower":20220704,"upper":20220705},{"lower":20220705,"upper":20220706},{"lower":20220706,"upper":20220707},{"lower":20220707,"upper":20220708},{"lower":20220708,"upper":20220709},{"lower":20220709,"upper":20220710},{"lower":20220710,"upper":20220711},{"lower":20220711,"upper":20220712},{"lower":20220712,"upper":20220713},{"lower":20220713,"upper":20220714},{"lower":20220714,"upper":20220715},{"lower":20220715,"upper":20220716},{"lower":20220716,"upper":20220717},{"lower":20220717,"upper":20220718},{"lower":20220718,"upper":20220719},{"lower":20220719,"upper":20220720},{"lower":20220720,"upper":20220721},{"lower":20220721,"upper":20220722},{"lower":20220722,"upper":20220723},{"lower":20220723,"upper":20220724},{"lower":20220724,"upper":20220725},{"lower":20220725,"upper":20220726},{"lower":20220726,"upper":20220727},{"lower":20220727,"upper":20220728},{"lower":20220728,"upper":20220729},{"lower":20220729,"upper":20220730}]'
 )