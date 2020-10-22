\set ON_ERROR_STOP true
drop database if exists clinvar_qc;
create database clinvar_qc;
\c patches

create table patch_types (
    name text primary key
);
insert into patch_types(operation_name)
values
    ('PRELOAD'),        -- Load all PRELOADs before any other messages
    ('INSERT_BEFORE'),  -- Insert payload before matched message
    ('INSERT_AFTER'),   -- Insert payload after matched message
    ('SKIP'),           -- Skip matched message
    ('REPLACE');        -- Replace matched message with payload

create table message_matchers (
    patch_id int,

    -- Field name to match against in the message JSON structure.
    -- Supports definite path matcher using dot syntax.
    -- Examples:
    --     content.entity_type
    -- future release may support JsonPath for indefinite array filters, etc.
    field_selector text,

    -- Value serialized as string. For non-string types like int, array, must de/serialize to match
    field_value text,

    foreign key(patch_id) references patches(id)
);

create table patches (
    id serial primary key,

--    release_date text not null,

    patch_type text,
    patch_payload text,

    foreign key(patch_type) references patch_types(name)
);



haplotype 1 -> childids [2 3 4]

patch ['INSERT_BEFORE'
        payload={simpleallele 2}]
        message_matchers [release_date=2020-07-01
                          content.subclass_type=haplotype
                          content.entity_type=variation
                          content.id=1]
