#!/usr/bin/python3

import argparse

# Initialize parser
parser = argparse.ArgumentParser(description="Hello world scripting with argsparse!")

# Adding optional argument
parser.add_argument("--host", type=str, required=True, help="AWS Elasticsearch cluster url")
parser.add_argument("--aws-profile", type=str, required=True, choices=["default, saml"], help=".aws/credentials profile name to take AWS credentials from")
parser.add_argument("--index-regex", type=str, required=True, help="reindex only indices matching INDEX_REGEX")
parser.add_argument("--older-than", type=int, default=1, help="reindex only indices older than OLDER_THAN, in days")
parser.add_argument("--dry-run", type=bool, default=True, help="reindex in DRY-RUN mode, not affecting elasticsearch cluster")

# Read arguments from command line
args = parser.parse_args()

print(args)