CREATE TABLE transactions_v2 (
    call_id             Utf8 NOT NULL,
    call_time           Datetime NOT NULL,
    client_id           Utf8 NOT NULL,
    region_code         Utf8 NOT NULL,
    campaign_type       Utf8 NOT NULL,
    call_status         Utf8 NOT NULL,
    client_response     Utf8,
    duration_sec        Int32,
    follow_up_required  Bool NOT NULL,
    PRIMARY KEY (call_id)
);