create table event_log (
  event_id bigserial primary key,
  transaction_time timestamptz default current_timestamp,
  type varchar,
  stream_id uuid not null,
  correlation_id uuid not null,
  seq_no bigint not null,
  payload json
);

create index event_log_by_stream_seq on event_log(stream_id, seq_no);
create index event_log_by_transaction_time on event_log(transaction_time);

create table event_stream_list (
  stream_id uuid not null primary key,
  stream_type varchar not null,
  last_event_id bigint,
  last_transaction_time timestamptz,
  last_seq_no bigint
);

create table lease (
  lease_key varchar not null primary key,
  owner_key varchar not null,
  expiration_date timestamptz
);
