#!/usr/bin/env bash

# ENTRADA, a big data platform for network data analytics
#
# Copyright (C) 2016 SIDN [https://www.sidn.nl]
#
# This file is part of ENTRADA.
#
# ENTRADA is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# ENTRADA is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with ENTRADA.  If not, see [<http://www.gnu.org/licenses/].

############################################################
#
# Update the Maxmind GEO-IP databases
#
############################################################

MAXMIND_TEMP_DIR=$TMPDIR/maxmind
MAXMIND_DEST_DIR=$(pwd)

#database are updated on the first Tuesday of each month.
COUNTRY_URL=http://geolite.maxmind.com/download/geoip/database/GeoLite2-Country.mmdb.gz
ASN_URL=http://geolite.maxmind.com/download/geoip/database/GeoLite2-ASN.tar.gz
CITY_URL=https://geolite.maxmind.com/download/geoip/database/GeoLite2-City.tar.gz


echo "[$(date)] : start update of Maxmind databases"
if [ ! -d $MAXMIND_TEMP_DIR ];
then
    echo "[$(date)] : create directory $MAXMIND_TEMP_DIR"
    mkdir -p $MAXMIND_TEMP_DIR
fi

#goto download location
cd $MAXMIND_TEMP_DIR

#remove old databases
rm -f *.dat
rm -f *.mmdb

echo "[$(date)] : fetch $COUNTRY_URL"
curl -sH 'Accept-encoding: gzip' $COUNTRY_URL | gunzip - > GeoLite2-Country.mmdb
echo "[$(date)] : fetch $ASN_URL"
curl -sH 'Accept-encoding: gzip' $ASN_URL | tar xvz
mv GeoLite2-ASN_*/GeoLite2-ASN.mmdb GeoLite2-ASN.mmdb
rm -rf GeoLite2-ASN_*

curl -sH 'Accept-encoding: gzip' $CITY_URL | tar xvz
mv GeoLite2-City_*/GeoLite2-City.mmdb .
rm -rf GeoLite2-City_*

mv $MAXMIND_TEMP_DIR/* $MAXMIND_DEST_DIR/

echo "[$(date)] : Maxmind update done"
echo "[$(date)] : Files in $MAXMIND_DEST_DIR : "
ls -lh $MAXMIND_DEST_DIR
echo "[$(date)] : =====  done  ==="
