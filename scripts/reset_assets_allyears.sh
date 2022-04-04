#!/bin/bash

# `earthengine authenticate` before running

dir=""
asset=$1   # hii or water
if [ $# -gt 1 ]
then
  dir="${1}/"  # driver
  asset=$2
fi
year=2001

while [ $year -le 2021 ]; do
  earthengine rm projects/HII/v1/"${dir}${asset}_archive/${asset}_${year}-01-01";
  earthengine mv projects/HII/v1/"${dir}${asset}/${asset}_${year}-01-01" projects/HII/v1/"${dir}${asset}_archive/${asset}_${year}-01-01";
  ((year++))
done
