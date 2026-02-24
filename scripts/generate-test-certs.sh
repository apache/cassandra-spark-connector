#!/bin/bash
#
# Generates TLS certificates for integration tests.
# Certificates are self-signed and include Subject Alternative Names
# for all test IP addresses used by CCM.
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="${1:-$SCRIPT_DIR/../test-support/src/main/resources/tls}"
KEYSTORE_PASSWORD="cassandra1sfun"

mkdir -p "$OUTPUT_DIR"

# List of all certificate files that should exist
CERT_FILES=(
    "server.crt" "server.key" "server.keystore"
    "client.crt" "client.key" "client.keystore"
    "client.truststore" "server.truststore" "server_truststore.pem"
)

# Check if all certificates already exist
all_exist=true
for file in "${CERT_FILES[@]}"; do
    if [[ ! -f "$OUTPUT_DIR/$file" ]]; then
        all_exist=false
        break
    fi
done

if $all_exist; then
    echo "TLS certificates already exist in $OUTPUT_DIR"
    exit 0
fi

echo "Generating TLS certificates in $OUTPUT_DIR..."

# Clean up any existing files to ensure a fresh start
for file in "${CERT_FILES[@]}"; do
    rm -f "$OUTPUT_DIR/$file"
done

# Create temporary OpenSSL config for server certificate with SANs
cat > "$OUTPUT_DIR/server_san.cnf" << 'EOF'
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
C = US
ST = California
L = Santa Clara
O = DataStax Inc.
OU = Drivers and Tools
CN = Cassandra Server

[v3_req]
basicConstraints = CA:FALSE
keyUsage = nonRepudiation, digitalSignature, keyEncipherment
subjectAltName = @alt_names

[alt_names]
IP.1 = 127.0.0.1
IP.2 = 127.0.0.2
IP.3 = 127.0.0.3
IP.4 = 127.0.0.4
IP.5 = 127.0.0.5
IP.6 = 127.1.0.1
IP.7 = 127.2.0.1
IP.8 = 127.3.0.1
IP.9 = 127.4.0.1
IP.10 = 127.5.0.1
IP.11 = 127.6.0.1
IP.12 = 127.7.0.1
IP.13 = 127.8.0.1
IP.14 = 127.9.0.1
IP.15 = 127.10.0.1
DNS.1 = localhost
EOF

# Create temporary OpenSSL config for client certificate
cat > "$OUTPUT_DIR/client_san.cnf" << 'EOF'
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
C = US
ST = California
L = Santa Clara
O = DataStax Inc.
OU = Drivers and Tools
CN = Driver Client

[v3_req]
basicConstraints = CA:FALSE
keyUsage = nonRepudiation, digitalSignature, keyEncipherment
EOF

# Generate server certificate and key (PEM format for Scylla)
echo "Generating server certificate..."
openssl req -x509 -nodes -newkey rsa:2048 \
    -keyout "$OUTPUT_DIR/server.key" \
    -out "$OUTPUT_DIR/server.crt" \
    -days 3650 \
    -config "$OUTPUT_DIR/server_san.cnf" \
    -extensions v3_req 2>/dev/null

# Generate client certificate and key (PEM format)
echo "Generating client certificate..."
openssl req -x509 -nodes -newkey rsa:2048 \
    -keyout "$OUTPUT_DIR/client.key" \
    -out "$OUTPUT_DIR/client.crt" \
    -days 3650 \
    -config "$OUTPUT_DIR/client_san.cnf" \
    -extensions v3_req 2>/dev/null

# Convert server cert/key to PKCS12, then to JKS keystore (for Cassandra)
echo "Creating server keystore (JKS)..."
openssl pkcs12 -export \
    -in "$OUTPUT_DIR/server.crt" \
    -inkey "$OUTPUT_DIR/server.key" \
    -out "$OUTPUT_DIR/server.p12" \
    -name node1 \
    -password "pass:$KEYSTORE_PASSWORD" 2>/dev/null

keytool -importkeystore \
    -srckeystore "$OUTPUT_DIR/server.p12" \
    -srcstoretype PKCS12 \
    -srcstorepass "$KEYSTORE_PASSWORD" \
    -destkeystore "$OUTPUT_DIR/server.keystore" \
    -deststoretype JKS \
    -deststorepass "$KEYSTORE_PASSWORD" \
    -noprompt 2>/dev/null

# Convert client cert/key to PKCS12, then to JKS keystore
echo "Creating client keystore (JKS)..."
openssl pkcs12 -export \
    -in "$OUTPUT_DIR/client.crt" \
    -inkey "$OUTPUT_DIR/client.key" \
    -out "$OUTPUT_DIR/client.p12" \
    -name client \
    -password "pass:$KEYSTORE_PASSWORD" 2>/dev/null

keytool -importkeystore \
    -srckeystore "$OUTPUT_DIR/client.p12" \
    -srcstoretype PKCS12 \
    -srcstorepass "$KEYSTORE_PASSWORD" \
    -destkeystore "$OUTPUT_DIR/client.keystore" \
    -deststoretype JKS \
    -deststorepass "$KEYSTORE_PASSWORD" \
    -noprompt 2>/dev/null

# Create client truststore (contains server certificate)
echo "Creating client truststore..."
keytool -import -noprompt \
    -alias server \
    -file "$OUTPUT_DIR/server.crt" \
    -keystore "$OUTPUT_DIR/client.truststore" \
    -storepass "$KEYSTORE_PASSWORD" 2>/dev/null

# Create server truststore (contains client certificate, for client auth)
echo "Creating server truststore..."
keytool -import -noprompt \
    -alias client \
    -file "$OUTPUT_DIR/client.crt" \
    -keystore "$OUTPUT_DIR/server.truststore" \
    -storepass "$KEYSTORE_PASSWORD" 2>/dev/null

# Create PEM truststore for Scylla (client cert for server-side client auth)
echo "Creating server truststore (PEM)..."
cp "$OUTPUT_DIR/client.crt" "$OUTPUT_DIR/server_truststore.pem"

# Cleanup temporary files
rm -f "$OUTPUT_DIR/server.p12" "$OUTPUT_DIR/client.p12" \
      "$OUTPUT_DIR/server_san.cnf" "$OUTPUT_DIR/client_san.cnf"

echo "TLS certificates generated successfully in $OUTPUT_DIR"
echo "Files created:"
ls -la "$OUTPUT_DIR"
