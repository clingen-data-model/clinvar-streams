-- all datetimes are ISO-8601 UTC strings

-- Note on constraints: because objects are tagged with release versions
-- and not all related objects will change in each release, we cannot use
-- foreign key constraints for some links between records.
-- We use foreign keys to link wholly owned sub-records back to their owner
-- but not peer relationships between related records (e.g. variation->child variation)
-- CHECK constraints could be used to ensure existence of referenced objects,
-- or this logic could be implemented at the application layer above, with more
-- meaningful error/warning handling than a constraint violation.

drop table if exists submitter;
drop table if exists submission;
drop table if exists trait;
drop table if exists trait_alternate_names;
drop table if exists trait_alternate_symbols;
drop table if exists trait_keywords;
drop table if exists trait_attribute_content;
drop table if exists trait_xrefs;
drop table if exists trait_set;
drop table if exists trait_set_trait_ids;
drop table if exists clinical_assertion_trait_set;
drop table if exists clinical_assertion_trait_set_clinical_assertion_trait_ids;
drop table if exists clinical_assertion_trait;
drop table if exists gene_association;
drop table if exists variation_archive;
drop table if exists rcv_accession;
drop table if exists clinical_assertion;
drop table if exists clinical_assertion_observation_ids;
drop table if exists clinical_assertion_observation;
drop table if exists clinical_assertion_trait_mapping;
drop table if exists clinical_assertion_variation;
drop table if exists clinical_assertion_variation_child_ids;
drop table if exists clinical_assertion_variation_descendant_ids;
drop table if exists trait_mapping;

drop table if exists release_sentinels;
drop table if exists release_sentinels_rules;

drop table if exists topic_offsets;
create table topic_offsets (
    topic_name text,
    partition int,
    offset int,
    primary key(topic_name, partition) on conflict replace
);

create table release_sentinels (
    release_date text,          -- 2020-10-15.0 2020-10-15.1
--    clingen_version text,       -- 2020-10-15.0.ruleset-1
    dirty int,
    event_type text,
    sentinel_type text,
    source text,
    reason text,
    notes text,
    rules text, -- JSON serialized array
    primary key(release_date, sentinel_type) on conflict replace
);
--create table release_sentinels_rules (
--    release_date text,
--    rule_id text
--);

-- Submission
create table submitter (
    release_date text,
    dirty int,
    event_type text,
    id int,
    org_category text,
    current_name text,
    current_abbrev text,
    all_names text, -- JSON serialized array
    all_abbrevs text, -- JSON serialized array
    primary key(id, release_date) on conflict replace
);

create table submission (
    release_date text,
    dirty int,
    event_type text,
    id text,
    submission_date text,
    submitter_id int,
    additional_submitter_ids text, -- JSON serialized array
    primary key(id, release_date) on conflict replace
);


-- Traits
create table trait (
    release_date text,
    dirty int,
    event_type text,
    id text,
    medgen_id text, -- Model lists as int in trait.medgen_id but text in trait_mapping.medgen_id
    type text,
    name text,
    content text,
    alternate_names text, -- JSON serialized array
    alternate_symbols text, -- JSON serialized array
    keywords text, -- JSON serialized array
    attribute_content text, -- JSON serialized object
    xrefs text, -- JSON serialized array
    primary key(id, release_date) on conflict replace
);


-- Trait Set
create table trait_set (
    release_date text,
    dirty int,
    event_type text,
    id int,
    type text,
    content text,
    trait_ids text, -- JSON serialized array
    primary key(id, release_date) on conflict replace
);


-- Clinical Assertion Trait Set
create table clinical_assertion_trait_set (
    release_date text,
    dirty int,
    event_type text,
    id text,
    type text,
    content text,
    clinical_assertion_trait_ids text, -- JSON serialized array
    primary key(id, release_date) on conflict replace
);

create table clinical_assertion_trait_set_clinical_assertion_trait_ids (
    release_date text,
    clinical_assertion_trait_set_id text,
    clinical_assertion_trait_id text,
    primary key(clinical_assertion_trait_set_id, clinical_assertion_trait_id, release_date)
        on conflict replace,
    foreign key(clinical_assertion_trait_set_id, release_date)
        references clinical_assertion_trait_set(id, release_date) on delete cascade
);

-- Clinical Assertion Trait
create table clinical_assertion_trait (
    release_date text,
    dirty int,
    event_type text,
    id text,
    type text,
    name text,
    medgen_id text,
    trait_id int,
    content text,
    xrefs text, -- JSON serialized array
    alternate_names text, -- JSON serialized array
    primary key(id, release_date) on conflict replace
);
-- part of clinical_assertion_trait_set
--create table clinical_assertion_trait_set_clinical_assertion_traits (
--    release_date text,
--    clinical_assertion_trait_set_id text,
--    clinical_assertion_trait_id text,
--    foreign key(clinical_assertion_trait_set_id, release_date)
--        references clinical_assertion_trait_set(id, release_date) on delete cascade
--);

-- Variation
drop table if exists gene;
create table gene (
    release_date text,
    dirty int,
    event_type text,
    id int,
    hgnc_id text,
    symbol text,
    full_name text,
    primary key(id, release_date) on conflict replace
);
drop table if exists variation;
create table variation (
    release_date text,
    dirty int,
    event_type text,
    id text,
    name text,
    variation_type text,
    subclass_type text, -- SimpleAllele|Haplotype|Genotype
    allele_id int,
    number_of_copies int,
    content text,
    protein_changes text, -- JSON serialized array
    child_ids text, -- JSON serialized array
    descendant_ids text, -- JSON serialized array
    primary key(id, release_date) on conflict replace
);

drop view if exists variation_latest;
create view variation_latest as
select *
from variation a
where a.release_date =
    (select release_date
    from variation
    where id = a.id
    order by release_date desc
    limit 1);

---- descendant and child ids exist, but maybe not in this release changeset
---- part of variation
-- create table variation_child_ids (
--    release_date text,
--    variation_id text,
--    variation_child_id text
--    , foreign key(variation_id, release_date) references variation(id, release_date) on delete cascade
-- --    , foreign key(variation_child_id, release_date) references variation(id, release_date) on delete cascade
-- );
-- part of variation
drop table if exists variation_descendant_ids;
create table variation_descendant_ids (
    release_date text,
    variation_id text,
    descendant_id text,
--    descendant_release_date text
    primary key (release_date, variation_id, descendant_id) on conflict replace,
    foreign key(variation_id, release_date) references variation(id, release_date) on delete cascade
-- --    , foreign key(variation_descendant_id) references variation(id) on delete cascade
);

-- List of (most recent) variations which are dirty or have a descendant that is dirty.
drop view if exists dirty_root_variations_view;
create view dirty_root_variations_view as
select distinct v.* from variation_latest v
    left join variation_descendant_ids vdi
      on v.id = vdi.variation_id
      and v.release_date = vdi.release_date
    left join variation_latest vd
      on vdi.descendant_id = vd.id
where (
-- Variation or any of its descendants are dirty
    v.dirty = 1
    or vd.dirty = 1)
-- No other variation has this variation as a descendant
and not exists (
    select * from variation_latest v2
    left join variation_descendant_ids vdi2
        on v2.id = vdi2.variation_id
        and v2.release_date = vdi2.release_date
    where vdi2.descendant_id = v.id)
order by v.id, v.release_date
;


create table gene_association (
    release_date text,
    dirty int,
    event_type text,
    relationship_type text,
    source text,
    content text,
    variation_id int,
    gene_id int,
    primary key(variation_id, gene_id, release_date) on conflict replace
--    , foreign key(variation_id) references variation(id) on delete cascade
--    , foreign key(gene_id) references gene(id) on delete cascade
);

drop view if exists gene_association_latest;
create view gene_association_latest as
select *
from gene_association a
where a.release_date =
      (select release_date
       from gene_association
       where variation_id = a.variation_id
         and gene_id = a.gene_id
       order by release_date desc
       limit 1);

create table variation_archive (
    release_date text,
    dirty int,
    event_type text,
    id text,
    version int,
    variation_id int,
    date_created text,
    date_last_updated text,
    num_submissions int,
    num_submitters int,
    record_status text,
    review_status text,
    species text,
    interp_date_last_evaluated text, -- TODO spell out interpretation?
    interp_type text,
    interp_description text,
    interp_explanation text,
    interp_content text,
    content text,
    primary key(id, release_date) on conflict replace
);

create table rcv_accession (
    release_date text,
    dirty int,
    event_type text,
    id text,
    version int,
    title text,
    date_last_evaluated text,
    review_status text, -- TODO ReviewStatusEnum
    interpretation text,
    submission_count int,
    variation_archive_id text,
    variation_id int,
    trait_set_id int,
    primary key(id, release_date) on conflict replace
);


-- Clinical Assertion
create table clinical_assertion (
    release_date text,
    dirty int,
    event_type text,
    id text,
    version int,
    internal_id int,
    title text,
    local_key text,
    assertion_type text,
    date_created text,
    date_last_updated text,
    submitted_assembly text,
    review_status text,
    interpretation_description text,
    interpretation_date_last_evaluated text,
    variation_archive_id text,
    variation_id int,
    submitter_id int,
    submission_id text,
    rcv_accession_id text,
    trait_set_id int,
    clinical_assertion_trait_set_id text,
    content text,
    interpretation_comments text, -- JSON serialized array
    submission_names text, -- JSON serialized array
    clinical_assertion_observation_ids text, -- JSON serialized array

    primary key(id, release_date) on conflict replace
--    -- clinical_assertion_observation has a clinical_assertion_trait_set_id as well
--    , foreign key(clinical_assertion_trait_set_id, release_date)
--        references clinical_assertion_trait_set(id, release_date) on delete cascade
);
-- part of clinical_assertion
--create table clinical_assertion_interpretation_comments (
--    release_date text,
--    clinical_assertion_id text not null,
--    comment text,
--    foreign key(clinical_assertion_id, release_date)
--        references clinical_assertion(id, release_date) on delete cascade
--);

create table clinical_assertion_observation (
    release_date text,
    dirty int,
    event_type text,
    id text,
    clinical_assertion_trait_set_id text,
    content text,
    primary key(id, release_date) on conflict replace
--    , foreign key(clinical_assertion_id) references clinical_assertion(id) on delete cascade
);
-- part of clinical_assertion
-- mapping clinical_assertion -> clinical_assertion_observation
create table clinical_assertion_observation_ids (
    release_date text, -- release date of clinical assertion
    clinical_assertion_id text,
    observation_id text, -- foreign key clinical_assertion_observation(id), not enforced
    primary key(clinical_assertion_id, observation_id, release_date) on conflict replace,
    foreign key(clinical_assertion_id, release_date)
        references clinical_assertion(id, release_date) on delete cascade
);


create table clinical_assertion_variation (
    release_date text,
    dirty int,
    event_type text,
    id text,
    variation_type text,
    subclass_type text, -- SimpleAllele|Haplotype|Genotype
    clinical_assertion_id text,
    content text,
    child_ids text, -- JSON serialized array
    descendant_ids text, -- JSON serialized array
    primary key(id, release_date) on conflict replace
--    foreign key(clinical_assertion_id) references clinical_assertion(id) on delete cascade
);
--create table clinical_assertion_variation_child_ids (
--    release_date text,
--    clinical_assertion_variation_id text,
--    clinical_assertion_variation_child_id text,
--    foreign key(clinical_assertion_variation_id) references clinical_assertion_variation(id) on delete cascade
--    -- foreign key(clinical_assertion_variation_child_id) references clinical_assertion_variation(id) on delete cascade
--);
create table clinical_assertion_variation_descendant_ids (
    release_date text,
    clinical_assertion_variation_id text,
    clinical_assertion_variation_descendant_id text,
    primary key(clinical_assertion_variation_id, clinical_assertion_variation_descendant_id, release_date)
         on conflict replace,
    foreign key(clinical_assertion_variation_id, release_date)
        references clinical_assertion_variation(id, release_date) on delete cascade
    -- foreign key(clinical_assertion_variation_descendant_id)
    --    references clinical_assertion_variation(id) on delete cascade
);

-- 1 <- 1 with clinical assertion <- trait_mapping
create table trait_mapping (
    release_date text,
    dirty int,
    event_type text,
    clinical_assertion_id text,
    trait_type text,
    mapping_type text,
    mapping_value text,
    mapping_ref text,
    medgen_id text,
    medgen_name text,
    primary key(release_date, clinical_assertion_id, mapping_type, mapping_value, mapping_ref) on conflict replace
--    foreign key(clinical_assertion_id, release_date) references clinical_assertion(id, release_date) on delete cascade
);
