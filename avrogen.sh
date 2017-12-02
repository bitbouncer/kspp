#!/usr/bin/env bash
set -ef
mkdir -p include/kspp/metrics

kspp_avrogencpp -i schemas/kspp_metrics.schema  -n avro -o include/kspp/metrics/avro_kspp_metrics_t.h

