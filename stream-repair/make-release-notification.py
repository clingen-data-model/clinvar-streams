import json
import re
import os
import pathlib
import google.cloud
from google.cloud import storage

storage_client = storage.Client(project="broad-dsp-monster-clingen-prod")
bucket_name = "broad-dsp-monster-clingen-prod-ingest-results"

"""
Notification structure example:
{"release_date": "2019-07-01", "bucket": "broad-dsp-monster-clingen-prod-ingest-results", "files": ["backdiff_20190701/clinical_assertion/created/000000000000", "backdiff_20190701/clinical_assertion/created/000000000001", "backdiff_20190701/clinical_assertion_observation/created/000000000000", "backdiff_20190701/clinical_assertion_trait/created/000000000000", "backdiff_20190701/clinical_assertion_trait_set/created/000000000000", "backdiff_20190701/clinical_assertion_variation/created/000000000000", "backdiff_20190701/gene/created/000000000000", "backdiff_20190701/gene_association/created/000000000000",
    "backdiff_20190701/gene_association/created/000000000001", "backdiff_20190701/rcv_accession/created/000000000000", "backdiff_20190701/release_date.txt", "backdiff_20190701/submission/created/000000000000", "backdiff_20190701/submitter/created/000000000000", "backdiff_20190701/trait/created/000000000000", "backdiff_20190701/trait_mapping/created/000000000000", "backdiff_20190701/trait_set/created/000000000000", "backdiff_20190701/variation/created/000000000000", "backdiff_20190701/variation/created/000000000001", "backdiff_20190701/variation/created/000000000002", "backdiff_20190701/variation_archive/created/000000000000"]}
"""
# Verify bucket exists
bucket = storage_client.get_bucket(bucket_name)


def flatten1(l):
    out = []
    for a in l:
        for b in a:
            out.append(b)
    return out


def ensure_trailing_slash(s: str) -> str:
    if s.endswith("/"):
        return s
    else:
        return s + "/"


def is_a_release_file(filename: str) -> bool:
    if isinstance(filename, google.cloud.storage.blob.Blob):
        filename = blob_path(filename)
    # Is a diff file
    terms = filename.split("/")
    print(terms)
    if (len(terms) == 4 and
            terms[2] in ["created", "updated", "deleted"]):
        return True
    # Is a release_date.txt file
    if (len(terms) == 2 and
            terms[1] == "release_date.txt"):
        return True
    with open("exclusions.log", "a") as fout:
        msg = "excluded blob: " + str(filename)
        print(msg)
        fout.write(msg + "\n")
    return False


def generate_notif_for_release(client, bucket, release_prefix):
    # List all files in bucket with release prefix
    blob_iterator = client.list_blobs(bucket,
                                      prefix=ensure_trailing_slash(release_prefix))
    all_blobs = flatten1([[pb for pb in page] for page in blob_iterator.pages])
    all_blobs = list(filter(is_a_release_file, all_blobs))
    # Get the release date stored in this release directory
    release_date_files = list(filter(lambda blob: blob.name.endswith("release_date.txt"),
                                     all_blobs))
    assert len(release_date_files) == 1, str(
        "release_date_files len was not 1" + " " + release_prefix)
    release_date_file = release_date_files[0]
    with release_date_file.open() as f:
        stored_release_date = f.read().strip()

    # Generate structure
    notification_msg = {
        "release_date": stored_release_date,
        "bucket": bucket_name,
        "files": [b.name for b in all_blobs]
    }
    return notification_msg


def list_subtract(A: list, B: list) -> list:
    """
    Returns a copy of A with the elements in B removed.
    If any element appears more in A than in B, those extras will be included in the return.
    """
    A_out = [a for a in A]
    for b in B:
        if b in A_out:
            A_out.remove(b)
    return A_out


def list_diff(A, B):
    """
    Returns [<elements in A-B>
             <elements in B-A>]
    """
    A_minus_B = list_subtract(A, B)
    B_minus_A = list_subtract(B, A)
    return (A_minus_B, B_minus_A)


def validate_notifs_equal(expecteds, actuals):
    """
    Throws error if any entry from topic_notifs doesn't match the entry at the same
    index in generated_notifs.
    """
    if len(expecteds) != len(actuals):
        raise RuntimeError(
            "expecteds and actuals notif lists were not the same length")
    for exp, act in zip(expecteds, actuals):
        topic_release_date = exp["release_date"]
        topic_bucket = exp["bucket"]
        # topic_files = exp["files"]
        release_prefix = exp["files"][0].split("/")[0] + "/"
        # Sanity check that all files are in the same directory
        for tf in exp["files"]:
            if not tf.startswith(release_prefix):
                raise RuntimeError("Files did not all start with same prefix:\n" +
                                   str(exp))
        # Check fields
        if act["release_date"] != topic_release_date:
            raise RuntimeError(
                ("Generated release date ({}) did not match topic release date ({})"
                 ", Generated notification: {}").format(
                    act["release_date"], topic_release_date, act))
        # Check files
        exp_files = list(sorted(exp["files"]))
        act_files = list(sorted(act["files"]))
        (exp_files_diff, act_files_diff) = list_diff(exp_files, act_files)
        if ([], []) != (exp_files_diff, act_files_diff):
            raise RuntimeError(
                ("File listings are not equal"
                 "\nexp_files_diff: {}"
                 "\nact_files_diff: {}"
                 "\nexpected: {}"
                 "\nactual: {}")
                .format(exp_files_diff, act_files_diff,
                        json.dumps(exp), json.dumps(act)))


def regenerate_notifs(client, notifs: list) -> list:
    """
    Takes a list of notification messages, and regenerates them based on the bucket and dir info in it.
    Returns a list of the regenerated notifications in the same order iterated over the input collection.
    """
    out = []
    for in_notif in notifs:
        topic_bucket = in_notif["bucket"]
        topic_files = in_notif["files"]
        release_prefix = topic_files[0].split("/")[0] + "/"
        # Sanity check that all files are in the same directory
        for tf in topic_files:
            if not tf.startswith(release_prefix):
                raise RuntimeError("Files did not all start with same prefix:\n" +
                                   str(in_notif))
        # Generate a release notification that should match the one on the topic
        generated_notif = generate_notif_for_release(
            client,
            topic_bucket,
            release_prefix)
        out.append(generated_notif)
    return out


def release_to_dir_mapping(notifs: list) -> list:
    """
    Returns a list of tuples of <release_date> <directory_in_bucket>
    """
    out = []
    for notif in notifs:
        files = notif["files"]
        release_date = notif["release_date"]
        release_prefix = files[0].split("/")[0]
        out.append((release_date, release_prefix))
    return out


def release_dir_map_to_notifications(client, bucket, release_dir_mappings: list) -> list:
    """
    Takes a list of (release_date, dirname) and generates and
    returns the notification messages in the same order
    """
    out = []
    for (release_date, release_prefix) in release_dir_mappings:
        notif = generate_notif_for_release(client, bucket, release_prefix)
        if release_date != notif["release_date"]:
            raise RuntimeError(
                ("Release date retrieved from bucket prefix did not match" +
                 " the release date in the input mappings:\n" +
                 str({"release_mapping": (release_date, release_prefix),
                     "generated_notif": notif})))
        out.append(notif)
    return out


def release_notification_file_sizes(client, bucket, files: list) -> dict:
    """
    For each file in the list, obtain the file size in the bucket.
    Returns a dict of filename(str) -> size in bytes(int).
    """
    # https://cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.blob.Blob
    # if bucket was str, should resolve here
    bucket = client.get_bucket(bucket)
    out = {}
    for file_name in files:
        blob = bucket.get_blob(file_name)
        out[file_name] = blob.size
    return out


def makeparents(path: str):
    """
    Makes the directories that are the parents of the file specified by path.
    If path is intended to be a directory, does not create the final directory.
    Doing makeparents and then os.mkdir on the same arg will meet that use case.
    makeparents(path)
    os.mkdir(path)
    """
    pathlib.Path(os.path.dirname(path)).mkdir(parents=True, exist_ok=True)


def blob_download_if_not(blob):
    path = blob_path(blob)
    assert len(path) > 0, {"blob": blob}
    if os.path.exists(path) and os.path.isfile(path):
        if os.path.getsize(path) == blob.size:
            return path
        os.remove(path)
    print(f"Downloading {path}")
    makeparents(path)
    blob.download_to_filename(path)
    return path


def blob_download_open(blob):
    """
    Opens a blob by downloading it first and then opening that local file.
    Useful for opening a remote blob, but caching it locally for future reads.
    """
    return open(blob_download_if_not(blob))


def blob_path(blob):
    """
    Returns the path of the blob, not including the bucket.
    e.g. gs://mybucket/p1/p2/fileA -> p1/p2/fileA
    """
    return blob.name


def release_notification_file_record_counts(client, bucket, files: list, cache_locally=True) -> dict:
    """
    For each file in the list, obtain the number of nonempty lines.
    Returns a dict of filename(str) -> count(int).

    If cache_locally is true, will download all of the blobs in the files list to the working
    directory and read from there. Next use of blob_download_open should be faster.
    """
    # https://cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.blob.Blob
    # if bucket was str, should resolve here
    bucket = client.get_bucket(bucket)
    out = {}

    def do_open(blob):
        if cache_locally:
            return blob_download_open(blob)
        else:
            return blob.open()

    for file_name in files:
        blob = bucket.get_blob(file_name)
        line_count = 0
        # with blob.open() as f:
        with do_open(blob) as f:
            for line in f:
                if len(line.strip()) > 0:
                    line_count += 1
        out[file_name] = line_count
        # print("Line count for file {} was {}".format(file_name, line_count))
    return out


def read_release_mappings(filename) -> list:
    out = []
    with open(filename) as f:
        lines = f.readlines()
    for line in lines:
        line_items = re.split("\s+", line.strip())
        assert len(line_items) == 2, line_items
        out.append(line_items)
    return out


def write_release_mappings(filename, mappings):
    with open(filename, "w") as fout:
        for (m1, m2) in mappings:
            fout.write("{}    {}\n"
                       .format(str(m1), str(m2)))


def notif_by_release_date(notifications, release_date, matched_index=0):
    """
    Returns the notification with a given release_date
    """
    return list(filter(lambda n: n["release_date"] == release_date, notifications))[matched_index]


def counts_by_table_op(count_map):
    """
    Takes a map of filename -> count
    Sums counts over the group by table and operation.
    releasedir/<table>/<operation>/basename : count.

    Returns:

    {table: {created: X
             updated: Y
             deleted: Z}
     ...}
    """
    splits = [[f.split("/"), count] for f, count in count_map.items()]
    # include only the diff files (not release_date.txt)
    diff_splits = list(filter(lambda terms_count: len(terms_count[0]) == 4,
                              splits))
    agg = {}
    for (dir, table, opname, basename), count in diff_splits:
        if table not in agg:
            agg[table] = {}
        if opname not in agg[table]:
            agg[table][opname] = 0
        agg[table][opname] += count

    # flatten into [table op count]
    flattened = []
    for table, op_counts in agg.items():
        for op, count in op_counts.items():
            flattened.append([table, op, count])
    return flattened


def validate_0330_0413():
    # Load the fixed set of notifications
    fixed_release_mappings = read_release_mappings(
        "broad-dsp-clinvar_release_mappings_FIXED.txt")
    fixed_notifications = release_dir_map_to_notifications(
        storage_client, bucket_name, fixed_release_mappings)

    notifications_to_compare = [
        (notif_by_release_date(received_notifications, "2022-03-20"),
            notif_by_release_date(fixed_notifications, "2022-03-20")),

        (notif_by_release_date(received_notifications, "2022-03-30"),
            notif_by_release_date(fixed_notifications, "2022-03-30")),

        (notif_by_release_date(received_notifications, "2022-04-03"),
            notif_by_release_date(fixed_notifications, "2022-04-03")),

        (notif_by_release_date(received_notifications, "2022-04-13"),
            notif_by_release_date(fixed_notifications, "2022-04-13")),
    ]
    # Should be no created or deleted submitters between 3/20 and 3/30

    records_counts = []
    for n1, n2 in notifications_to_compare:
        n1_record_counts = release_notification_file_record_counts(
            storage_client,
            bucket_name,
            n1["files"])
        n2_record_counts = release_notification_file_record_counts(
            storage_client,
            bucket_name,
            n2["files"])
        records_counts.append({
            "notifs": [n1, n2],
            "release_date": [n1["release_date"],
                             n2["release_date"]],
            "record_counts": [n1_record_counts,
                              n2_record_counts]
        })

    # Annotate each count record with an op_counts which is the counts per
    # file aggregated by the table and create/update/delete operation
    for record_count in records_counts:
        n1_record_counts = record_count["record_counts"][0]
        n2_record_counts = record_count["record_counts"][1]
        n1_op_counts = counts_by_table_op(n1_record_counts)
        n2_op_counts = counts_by_table_op(n2_record_counts)
        record_count["op_counts"] = [
            n1_op_counts,
            n2_op_counts
        ]

    with open("record_counts.json", "w") as fout:
        json.dump(records_counts, fout)


def validate_20220620_20220626():
    # Load the fixed set of notifications
    fixed_release_mappings = read_release_mappings(
        "broad-dsp-clinvar_release_mappings_FIXED.txt")
    fixed_release_mappings = list(filter(lambda rd_dir: rd_dir[0].startswith("2022-06"),
                                         fixed_release_mappings))
    fixed_notifications = release_dir_map_to_notifications(
        storage_client, bucket_name, fixed_release_mappings)

    notifications_to_compare = [
        (notif_by_release_date(received_notifications, "2022-06-19"),
            notif_by_release_date(fixed_notifications, "2022-06-20")),

        (notif_by_release_date(received_notifications, "2022-06-26"),
            notif_by_release_date(fixed_notifications, "2022-06-26"))
    ]
    # Should be no created or deleted submitters between 3/20 and 3/30

    records_counts = []
    for n1, n2 in notifications_to_compare:
        n1_record_counts = release_notification_file_record_counts(
            storage_client,
            bucket_name,
            n1["files"])
        n2_record_counts = release_notification_file_record_counts(
            storage_client,
            bucket_name,
            n2["files"])
        records_counts.append({
            "notifs": [n1, n2],
            "release_date": [n1["release_date"],
                             n2["release_date"]],
            "record_counts": [n1_record_counts,
                              n2_record_counts],
        })

    # Annotate each count record with an op_counts which is the counts per
    # file aggregated by the table and create/update/delete operation
    for record_count in records_counts:
        n1_record_counts = record_count["record_counts"][0]
        n2_record_counts = record_count["record_counts"][1]
        n1_op_counts = counts_by_table_op(n1_record_counts)
        n2_op_counts = counts_by_table_op(n2_record_counts)
        record_count["op_counts"] = [
            n1_op_counts,
            n2_op_counts
        ]

    # Annotate with op count diffs
    for record_count in records_counts:
        n1_op_counts = record_count["op_counts"][0]
        n2_op_counts = record_count["op_counts"][1]
        # Each op count is a vector [<table> <op> <count>]
        [op1_uniq, op2_uniq] = list_diff(n1_op_counts, n2_op_counts)
        record_count["record_count_diffs"] = [op1_uniq, op2_uniq]

    with open("compared_record_counts_20220620_20220626.json", "w") as fout:
        json.dump(records_counts, fout)

    # Create a simple fixed record count listing (not a before/after comparison)
    # Look at the second item in each pair in records_counts
    fixed_record_counts = []
    for record_count in records_counts:
        fixed_release_date = record_count["release_date"][1]
        fixed_counts = record_count["op_counts"][1]
        fixed_record_counts.append({
            "release_date": fixed_release_date,
            "op_counts": fixed_counts
        })
    with open("fixed_record_counts_20220620_20220626.json", "w") as fout:
        json.dump(fixed_record_counts, fout)


# Generate a mapping file based on a file of release notifications
# with open("broad-dsp-clinvar_backup_20221201.txt") as f:
#     notif_lines = f.readlines()
# topic_notifs = [json.loads(line) for line in notif_lines]
# # topic_notifs = topic_notifs[:20]
# release_dir_mappings = release_to_dir_mapping(topic_notifs)
# write_release_mappings("broad-dsp-clinvar_release_mappings.txt",
#                        release_dir_mappings)


# Validate that a file of release notifications matches what
# is in the bucket for that release
# regenerated_topic_notifs = regenerate_notifs(storage_client, topic_notifs)
# validate_notifs_equal(topic_notifs, regenerated_topic_notifs)


# Generate notifications based on a mappings file
mapping_file = "broad-dsp-clinvar_release_mappings_FIXED.txt"
generated_notifications_file = "broad-dsp-clinvar_generated_notifications.txt"
release_dir_mappings = read_release_mappings(mapping_file)
generated_notifications = release_dir_map_to_notifications(storage_client,
                                                           bucket_name,
                                                           release_dir_mappings)
with open(generated_notifications_file, "w") as fout:
    for notif in generated_notifications:
        fout.write(json.dumps(notif))
        fout.write("\n")


# Validate that the generated notifications matches a file of the topic of those same mappings
received_notifications_file = "broad-dsp-clinvar_backup_20221201.txt"
with open(received_notifications_file) as f:
    received_notifications = [json.loads(line.strip()) for line in f]
assert len(received_notifications) == len(generated_notifications)
exceptions = []
for (received, generated) in zip(received_notifications, generated_notifications):
    try:
        validate_notifs_equal([received], [generated])
    except Exception as e:
        exceptions.append({
            "received": received,
            "generated": generated,
            "exception": e
        })


# Check record counts in diffs
# record_counts_20220403 = release_notification_file_record_counts(
#     storage_client, bucket_name, notif_by_release_date(notifications, "2022-04-03")["files"])
# print("2022-04-03 Diff record counts:")
# print(json.dumps(record_counts_20220403, indent=2))

# record_counts_20220413 = release_notification_file_record_counts(
#     storage_client, bucket_name, notif_by_release_date(notifications, "2022-04-13")["files"])
# print("2022-04-13 Diff record counts:")
# print(json.dumps(record_counts_20220413, indent=2))


# # Check fixed file sizes
# fixed_record_counts_20220403 = release_notification_file_record_counts(
#     storage_client, bucket_name, notif_by_release_date(fixed_notifications, "2022-04-03")["files"])
# print("2022-04-03 FIXED diff record counts:")
# print(json.dumps(fixed_record_counts_20220403, indent=2))

# fixed_record_counts_20220413 = release_notification_file_record_counts(
#     storage_client, bucket_name, notif_by_release_date(fixed_notifications, "2022-04-13")["files"])
# print("2022-04-13 FIXED diff record counts:")
# print(json.dumps(fixed_record_counts_20220413, indent=2))


# # Compare previous and fixed record counts
# # Strip the release prefix from each file path
# def strip_first_path_term(path_str):
#     return "/".join(path_str.split("/")[1:])


# def compare_record_counts(count_map1, count_map2):
#     counts1 = [[strip_first_path_term(k), v] for k, v in count_map1.items()]
#     counts2 = [[strip_first_path_term(k), v] for k, v in count_map2.items()]
#     return list_diff(counts1, counts2)


# print("Difference between 2022-04-03 counts before and after fix")
# print(compare_record_counts(record_counts_20220403, fixed_record_counts_20220403))

# print("Difference between 2022-04-13 counts before and after fix")
# print(compare_record_counts(record_counts_20220413, fixed_record_counts_20220413))
