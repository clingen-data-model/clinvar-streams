#!/bin/zsh
#topics=(clinvar-raw)
#topics=(broad-dsp-clinvar)
topics=(broad-dsp-clinvar-testdata_20210122 clinvar-raw-testdata_20210122)
offset=0
group=broad-dev

for topic in $topics; do
    echo $topic
kafka-consumer-groups --command-config kafka-dev.properties \
  --bootstrap-server pkc-4yyd6.us-east1.gcp.confluent.cloud:9092 \
  --reset-offsets \
  --to-offset $offset \
  --group $group --topic $topic \
  --timeout 10000 \
  --execute
done
