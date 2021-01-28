#!/bin/zsh
set -x

#dsp_topic=broad-dsp-clinvar-testdata_20201028
#raw_topic=clinvar-raw-testdata_20201028
dsp_topic=broad-dsp-clinvar-testdata_20210122
raw_topic=clinvar-raw-testdata_20210122

combined_topic=clinvar-combined-testdata_20210122

cluster=clingen-streams-dev
cluster_id=$(ccloud kafka cluster list | grep $cluster | awk '{print $1}')

ccloud kafka topic delete --cluster $cluster_id $dsp_topic
ccloud kafka topic delete --cluster $cluster_id $raw_topic
ccloud kafka topic delete --cluster $cluster_id $combined_topic

set -e
ccloud kafka topic create --cluster $cluster_id $dsp_topic --partitions 1 --config retention.ms=-1
ccloud kafka topic create --cluster $cluster_id $raw_topic --partitions 1 --config retention.ms=-1
ccloud kafka topic create --cluster $cluster_id $combined_topic --partitions 1 --config retention.ms=-1
