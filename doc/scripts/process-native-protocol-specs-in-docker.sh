#!/bin/sh
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

[ -f "../build.xml" ] || { echo "build.xml must exist (current directory needs to be cassandra repo"; exit 1; }

# Variables
GO_VERSION="1.23.1"
GO_TAR="go${GO_VERSION}.linux-amd64.tar.gz"
TMPDIR="${TMPDIR:-/tmp}"

# Step 0: Download and install Go
echo "Downloading Go $GO_VERSION..."
wget -q "https://golang.org/dl/$GO_TAR" -O "$TMPDIR/$GO_TAR"

echo "Installing Go..."
tar -C "$TMPDIR" -xzf "$TMPDIR/$GO_TAR"
rm "$TMPDIR/$GO_TAR"

# Set Go environment variables
export PATH="$PATH:$TMPDIR/go/bin"
export GOPATH="$TMPDIR/go"

# Step 1: Building the parser
echo "Building the cqlprotodoc..."
DIR="$(pwd)"
cd "${TMPDIR}"

# FIXME
git clone -n --depth=1 --filter=tree:0 -b "mck/native-protocols-page-upgrade" https://github.com/thelastpickle/cassandra-website
#git clone -n --depth=1 --filter=tree:0 https://github.com/apache/cassandra-website

cd "${TMPDIR}/cassandra-website"
git sparse-checkout set --no-cone /cqlprotodoc
git checkout
cd "${TMPDIR}/cassandra-website/cqlprotodoc"
go build -o "$TMPDIR"/cqlprotodoc

# Step 2: Process the spec files using the parser
echo "Processing the .spec files..."
cd "${DIR}"
output_dir="modules/cassandra/assets/attachments"
mkdir -p "${output_dir}"
"$TMPDIR"/cqlprotodoc . "${output_dir}"

# Step 3: Cleanup - Remove the Cassandra and parser directories
echo "Cleaning up..."
cd "${DIR}"
rm -rf "$TMPDIR/go" "${TMPDIR}/cassandra-website" "$TMPDIR"/cqlprotodoc 2>/dev/null

echo "Script completed successfully."
