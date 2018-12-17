#!/usr/bin/env bash

mkdir -p backups
cp allsamples.db backups/bck_allsamples_$(date +%F_%T | sed -s 's/[-:]//g').db
