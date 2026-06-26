#!/usr/bin/env bash
# Generate a local development PKI for VaultFS: a self-signed cluster CA plus
# certificates for the master, chunkserver, and client roles. Every certificate
# carries both serverAuth and clientAuth usages so any node can authenticate to
# any other (mutual TLS). SANs cover the docker-compose service hostnames and
# localhost.
#
# Usage: deploy/gen-certs.sh [output-dir]   (default: deploy/certs)
set -euo pipefail

CERTS_DIR="${1:-deploy/certs}"
DAYS_CA=3650
DAYS_LEAF=825

mkdir -p "$CERTS_DIR"
cd "$CERTS_DIR"

echo "Generating cluster CA..."
openssl ecparam -name prime256v1 -genkey -noout -out ca.key
openssl req -new -x509 -key ca.key -out ca.crt -days "$DAYS_CA" \
  -subj "/CN=VaultFS-CA" 2>/dev/null

gen_cert() {
  local name="$1" sans="$2"
  echo "Generating certificate: $name"
  openssl ecparam -name prime256v1 -genkey -noout -out "$name.key"
  openssl req -new -key "$name.key" -out "$name.csr" -subj "/CN=$name" 2>/dev/null
  openssl x509 -req -in "$name.csr" -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out "$name.crt" -days "$DAYS_LEAF" \
    -extfile <(printf "subjectAltName=%s\nextendedKeyUsage=serverAuth,clientAuth\n" "$sans") \
    2>/dev/null
  rm -f "$name.csr"
}

gen_cert master      "DNS:localhost,DNS:master-0,DNS:master-1,DNS:master-2,IP:127.0.0.1"
gen_cert chunkserver "DNS:localhost,DNS:chunkserver-0,DNS:chunkserver-1,DNS:chunkserver-2,IP:127.0.0.1"
gen_cert client      "DNS:localhost,IP:127.0.0.1"

rm -f ca.srl
echo "Certificates written to $CERTS_DIR (gitignored)"
