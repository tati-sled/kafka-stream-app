#!/usr/bin/env bash

helm upgrade --install  --wait -f k8s/prometheus-adapter/values.yaml prometheus-adapter prometheus-community/prometheus-adapter
