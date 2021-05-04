#!/usr/bin/env bash

helm upgrade --install  --wait -f k8s/prometheus-operator/values.yaml prometheus-operator prometheus-community/prometheus-operator
