-- create schemas
create schema atm_landing_refdata;
create schema atm_staging_refdata;
create schema atm_refdata;
create schema reference;

-- create job sequence
create sequence reference.job_sequence;

-- create job status and count tables
create table reference.job_counts
(
    job_id bigint,
    job_layer text,
    job_type text,
    start_time timestamp,
    stop_time timestamp,
    src_count integer,
    tar_inserts integer,
    tar_updates integer,
    tar_deletes integer
);

create table reference.job_status
(
    job_id bigint,
    layer text,
    tablename text,
    start_time timestamp,
    status text
);