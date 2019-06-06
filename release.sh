#!/usr/bin/env bash
mvn release:clean release:prepare -DskipTests -Darguments="-DskipTests" -B -U -DgenerateBackupPoms=false $@
# -DreleaseVersion=1.2 -DdevelopmentVersion=2.0-SNAPSHOT
