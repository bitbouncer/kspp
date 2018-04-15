#!/usr/bin/env bash
set -ef
mkdir -p include/kspp/metrics

kspp_avrogencpp -i schemas/metrics20.key.schema -o include/kspp/metrics/metric20_key_t.h
