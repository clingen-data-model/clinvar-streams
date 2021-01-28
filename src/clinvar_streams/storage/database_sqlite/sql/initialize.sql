-- all datetimes are ISO-8601 UTC strings

drop table if exists submitter;
drop table if exists submitter_names;
drop table if exists submitter_abbrevs;
drop table if exists submission;
drop table if exists submission_additional_submitters;
drop table if exists trait;
drop table if exists trait_alternate_names;
drop table if exists trait_alternate_symbols;
drop table if exists trait_keywords;
drop table if exists trait_attribute_content;
drop table if exists trait_xrefs;
drop table if exists trait_set;
drop table if exists trait_set_trait_ids;
drop table if exists clinical_assertion_trait_set;
drop table if exists clinical_assertion_trait;
drop table if exists clinical_assertion_trait_xrefs;
drop table if exists clinical_assertion_trait_alternate_names;
drop table if exists clinical_assertion_trait_set_clinical_assertion_traits;
drop table if exists gene;
drop table if exists variation;
drop table if exists variation_protein_changes;
drop table if exists variation_child_ids;
drop table if exists variation_descendant_ids;
drop table if exists gene_association;
drop table if exists variation_archive;
drop table if exists rcv_accession;
drop table if exists clinical_assertion;
drop table if exists interpretation_comments;
drop table if exists clinical_assertion_observation;
drop table if exists clinical_assertion_submission_names;
drop table if exists clinical_assertion_trait_mapping;
drop table if exists clinical_assertion_variation;
drop table if exists clinical_assertion_variation_child_ids;
drop table if exists clinical_assertion_variation_descendant_ids;
drop table if exists trait_mapping;

drop table if exists test;
create table test (
    id int primary key,
    first text,
    last text,
    phone text
);

-- Submission
create table submitter (
    id int,
    release_date text
    org_category text,
    current_name text,
    current_abbrev text
--    primary key(id, release_date)
);
create table submitter_names (
    submitter_id int,
    name text,
    foreign key(submitter_id) references submitter(id) on delete cascade
);
create table submitter_abbrevs (
    submitter_id int,
    abbrev text,
    foreign key(submitter_id) references submitter(id) on delete cascade
);
create table submission (
    id text primary key,
    submission_date text,
    submitter_id int,
    foreign key(submitter_id) references submitter(id) on delete cascade
);
create table submission_additional_submitters (
    submission_id text,
    submitter_id int,
    foreign key(submission_id) references submission(id) on delete cascade,
    foreign key(submitter_id) references submitter(id) on delete cascade
);

-- Traits
create table trait (
    id text primary key,
    medgen_id text, -- Model lists as int in trait.medgen_id but text in trait_mapping.medgen_id
    type text,
    name text,
    content text
);
create table trait_alternate_names (
    trait_id text,
    alternate_name text,
    foreign key(trait_id) references trait(id) on delete cascade
);
create table trait_alternate_symbols (
    trait_id text,
    alternate_symbol text,
    foreign key(trait_id) references trait(id) on delete cascade
);
create table trait_keywords (
    trait_id text,
    keyword text,
    foreign key(trait_id) references trait(id) on delete cascade
);
create table trait_attribute_content (
    trait_id text,
    attribute_content text,
    foreign key(trait_id) references trait(id) on delete cascade
);
create table trait_xrefs (
    trait_id text,
    xref text,
    foreign key(trait_id) references trait(id) on delete cascade
);

create table trait_set (
    id int primary key,
    type text,
    content text
);
create table trait_set_trait_ids (
    trait_set_id int,
    trait_id text,
    foreign key (trait_set_id) references trait_set(id) on delete cascade,
    foreign key (trait_id) references trait(id) on delete cascade
);

create table clinical_assertion_trait_set (
    id text primary key,
    type text,
    content text
);
create table clinical_assertion_trait (
    id text primary key,
    type text,
    name text,
    medgen_id text,
    trait_id int,
    content text,
    foreign key(trait_id) references trait(id) on delete cascade
);
create table clinical_assertion_trait_xrefs (
    clinical_assertion_trait_id text,
    xref text,
    foreign key(clinical_assertion_trait_id) references clinical_assertion_trait(id) on delete cascade
);
create table clinical_assertion_trait_alternate_names (
    clinical_assertion_trait_id text,
    alternate_name text,
    foreign key(clinical_assertion_trait_id) references clinical_assertion_trait(id) on delete cascade
);
create table clinical_assertion_trait_set_clinical_assertion_traits (
    clinical_assertion_trait_set_id text,
    clinical_assertion_trait_id text,
    foreign key(clinical_assertion_trait_set_id) references clinical_assertion_trait_set(id) on delete cascade,
    foreign key(clinical_assertion_trait_id) references clinical_assertion_trait(id) on delete cascade
);

-- Variation
create table gene (
    id int,
    hgnc_id text,
    symbol text,
    full_name text
);
create table variation (
    id int primary key,
    name text,
    variation_type text,
    subclass_type text, -- SimpleAllele|Haplotype|Genotype
    allele_id int,
    number_of_copies int,
    content text
);
create table variation_protein_changes (
    variation_id int,
    protein_change text,
    foreign key(variation_id) references variation(id) on delete cascade
);
-- For child and descendants, we do not have a guarantee from
-- upstream that foreign key referenced records exist first
create table variation_child_ids (
    variation_id text,
    variation_child_id text
    , foreign key(variation_id) references variation(id) on delete cascade,
    foreign key(variation_child_id) references variation(id) on delete cascade
);
create table variation_descendant_ids (
    variation_id text,
    variation_descendant_id text
    , foreign key(variation_id) references variation(id) on delete cascade,
    foreign key(variation_descendant_id) references variation(id) on delete cascade
);
create table gene_association (
    relationship_type text,
    source text,
    content text,
    variation_id int,
    gene_id int,
    primary key(variation_id, gene_id),
    foreign key(variation_id) references variation(id) on delete cascade,
    foreign key(gene_id) references gene(id) on delete cascade
);

create table variation_archive (
    id text primary key,
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

    foreign key(variation_id) references variation(id) on delete cascade
);
create table rcv_accession (
    id text primary key,
    version int,
    title text,
    date_last_evaluated text,
    review_status text, -- TODO ReviewStatusEnum
    interpretation text,
    submission_count int,
    variation_archive_id text,
    variation_id int,
    trait_set_id int,

    foreign key(variation_archive_id) references variation_archive(id) on delete cascade,
    foreign key(variation_id) references variation(id) on delete cascade,
    foreign key(trait_set_id) references trait_set(id) on delete cascade
);


-- Clinical Assertion
create table clinical_assertion (
    id text primary key,
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

    foreign key(variation_archive_id) references variation_archive(id) on delete cascade,
    foreign key(variation_id) references variation(id) on delete cascade,
    foreign key(submitter_id) references submitter(id) on delete cascade,
    foreign key(submission_id) references submission(id) on delete cascade,
    foreign key(rcv_accession_id) references rcv_accession(id) on delete cascade,
    foreign key(trait_set_id) references trait_set(id) on delete cascade,
    foreign key(clinical_assertion_trait_set_id) references clinical_assertion_trait_set(id) on delete cascade
);

create table interpretation_comments (
    clinical_assertion_id text not null,
    comment text,
    foreign key(clinical_assertion_id) references clinical_assertion(id) on delete cascade
);

create table clinical_assertion_observation (
    id text primary key,
    clinical_assertion_id text not null,
    content text,
    foreign key(clinical_assertion_id) references clinical_assertion(id) on delete cascade
);

create table clinical_assertion_submission_names (
    clinical_assertion_id text not null,
    submission_name text,
    foreign key(clinical_assertion_id) references clinical_assertion(id) on delete cascade
);

create table clinical_assertion_trait_mapping (
    clinical_assertion_id text,
    trait_type text,
    mapping_type text,
    mapping_value text,
    mapping_ref text,
    medgen_id text,
    medgen_name text,
    foreign key(clinical_assertion_id) references clinical_assertion(id) on delete cascade
);

create table clinical_assertion_variation (
    id text primary key,
    variation_type text,
    subclass_type text, -- SimpleAllele|Haplotype|Genotype
    clinical_assertion_id text,
    content text,
    foreign key(clinical_assertion_id) references clinical_assertion(id) on delete cascade
);
create table clinical_assertion_variation_child_ids (
    clinical_assertion_variation_id text,
    clinical_assertion_variation_child_id text,
    foreign key(clinical_assertion_variation_id) references clinical_assertion_variation(id) on delete cascade,
    foreign key(clinical_assertion_variation_child_id) references clinical_assertion_variation(id) on delete cascade
);
create table clinical_assertion_variation_descendant_ids (
    clinical_assertion_variation_id text,
    clinical_assertion_variation_descendant_id text,
    foreign key(clinical_assertion_variation_id) references clinical_assertion_variation(id) on delete cascade,
    foreign key(clinical_assertion_variation_descendant_id) references clinical_assertion_variation(id) on delete cascade
);

create table trait_mapping (
    clinical_assertion_id text,
    trait_type text,
    mapping_type text,
    mapping_value text,
    mapping_ref text,
    medgen_id text,
    medgen_name text,

    foreign key(clinical_assertion_id) references clinical_assertion(id) on delete cascade
);