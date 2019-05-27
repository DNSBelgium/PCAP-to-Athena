#!/usr/bin/env bash

# By default test that depend on AWS are skipped with @IfProfileValue
#
# To run these test you need to specify an AWS access key and secret key in ~/.aws/credentials
#

mvn -Dtest-groups=aws-integration-tests test
