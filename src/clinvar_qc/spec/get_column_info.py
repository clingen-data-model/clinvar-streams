from google.cloud import bigquery

client = bigquery.Client()
project = "clingen-dev"
if project != client.project:
    raise RuntimeError("Project specified was %s but authenticated with project %s" % (project, client.project))
dataset = "clinvar_backfill_2019_04"
table_names = [
    "clinical_assertion",
    "clinical_assertion_observation",
    "clinical_assertion_trait",
    "clinical_assertion_trait_set",
    "clinical_assertion_variation",
    "gene",
    "gene_association",
    "rcv_accession",
    "submission",
    "submitter",
    "trait",
    "trait_mapping",
    "trait_set",
    "variation",
    "variation_archive"
]


def get_count(project, dataset, table_name, column_name, where=None):
    if getattr(get_count, "cache", None) is None:
        cache = {}
        setattr(get_count, "cache", cache)
    cache = getattr(get_count, "cache")
    if (project, dataset, table_name, column_name, where) in cache:
        return cache[(project, dataset, column_name)]
    else:
        count_query = "SELECT count(*) as ct from `{project}.{dataset}.{table_name}`".format(
            project=project, dataset=dataset, table_name=table_name)
        if where:
            count_query += " " + where
        count_job = client.query(count_query)
        if count_job.exception():
            raise RuntimeError(count_job.exception())
        count = 0
        for row in count_job.result():
            count += row["ct"]
        cache[(project, dataset, table_name, column_name)] = count
        return count


for table_name in table_names:
    query = "SELECT * from `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS` where table_name = \"{table_name}\"".format(
        project=project, dataset=dataset, table_name=table_name
    )
    query_job = client.query(query)
    rows = sorted([r for r in query_job.result()], key=lambda r: r["column_name"])
    for column_info in rows:
        column_name = column_info["column_name"]
        is_nullable = column_info["is_nullable"]
        data_type = column_info["data_type"]
        hint = data_type
        if is_nullable.upper() == "NO":
            hint += " required"
        else:
            hint += " optional"
            row_count = get_count(project, dataset, table_name, column_name)
            null_count = get_count(project, dataset, table_name, column_name,
                                   where="where %s is null" % column_name)

            hint += " (%d/%d %.4f%% null)" % (null_count, row_count, (float(null_count)/row_count*100))

        if "ARRAY" in data_type.upper():
            # Regardless of required/optional, for ARRAY fields, useful to know if any are empty (not same as null)
            row_count = get_count(project, dataset, table_name, column_name)
            empty_array_count = get_count(project, dataset, table_name, column_name,
                                          where="where ARRAY_LENGTH(%s) = 0" % column_name)
            hint += " (%d/%d %.4f%% empty arrays)" % (
                empty_array_count, row_count, (float(empty_array_count)/row_count*100))

        print("%s %s %s" % (
            table_name, column_name, hint
        ))