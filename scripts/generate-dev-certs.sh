#!/usr/bin/env bash
set -euo pipefail

# Generate self-signed TLS certificates for local development.
# Usage: ./scripts/generate-dev-certs.sh
#
# Creates:
#   certs/ca.key, certs/ca.crt          — Certificate Authority
#   certs/server.key, certs/server.crt  — Orchestrator server cert
#   certs/worker.key, certs/worker.crt  — Worker client cert (for mTLS)

CERT_DIR="certs"
DAYS=365
CA_SUBJ="/CN=DAC Cloud Dev CA"
SERVER_SUBJ="/CN=localhost"
WORKER_SUBJ="/CN=dac-worker"

mkdir -p "$CERT_DIR"

echo "=== Generating CA ==="
openssl genrsa -out "$CERT_DIR/ca.key" 4096
openssl req -new -x509 -key "$CERT_DIR/ca.key" -out "$CERT_DIR/ca.crt" \
  -days "$DAYS" -subj "$CA_SUBJ"

echo "=== Generating server certificate ==="
openssl genrsa -out "$CERT_DIR/server.key" 2048
openssl req -new -key "$CERT_DIR/server.key" -out "$CERT_DIR/server.csr" \
  -subj "$SERVER_SUBJ"

cat > "$CERT_DIR/server-ext.cnf" <<EOF
subjectAltName = DNS:localhost, IP:127.0.0.1
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
EOF

openssl x509 -req -in "$CERT_DIR/server.csr" -CA "$CERT_DIR/ca.crt" -CAkey "$CERT_DIR/ca.key" \
  -CAcreateserial -out "$CERT_DIR/server.crt" -days "$DAYS" -extfile "$CERT_DIR/server-ext.cnf"
rm "$CERT_DIR/server.csr" "$CERT_DIR/server-ext.cnf"

echo "=== Generating worker client certificate (for mTLS) ==="
openssl genrsa -out "$CERT_DIR/worker.key" 2048
openssl req -new -key "$CERT_DIR/worker.key" -out "$CERT_DIR/worker.csr" \
  -subj "$WORKER_SUBJ"

cat > "$CERT_DIR/worker-ext.cnf" <<EOF
basicConstraints = CA:FALSE
keyUsage = digitalSignature
extendedKeyUsage = clientAuth
EOF

openssl x509 -req -in "$CERT_DIR/worker.csr" -CA "$CERT_DIR/ca.crt" -CAkey "$CERT_DIR/ca.key" \
  -CAcreateserial -out "$CERT_DIR/worker.crt" -days "$DAYS" -extfile "$CERT_DIR/worker-ext.cnf"
rm "$CERT_DIR/worker.csr" "$CERT_DIR/worker-ext.cnf"

echo ""
echo "=== Certificates generated in $CERT_DIR/ ==="
echo ""
echo "Add to orchestrator .env:"
echo "  TLS_ENABLED=true"
echo "  TLS_CERT_PATH=$CERT_DIR/server.crt"
echo "  TLS_KEY_PATH=$CERT_DIR/server.key"
echo "  TLS_CA_PATH=$CERT_DIR/ca.crt"
echo "  TLS_REQUIRE_CLIENT_CERT=false"
echo ""
echo "Add to worker .env:"
echo "  ORCHESTRATOR_URL=wss://localhost:8443"
echo "  TLS_CA_PATH=$CERT_DIR/ca.crt"
echo "  TLS_CERT_PATH=$CERT_DIR/worker.crt  (for mTLS)"
echo "  TLS_KEY_PATH=$CERT_DIR/worker.key    (for mTLS)"
echo ""
echo "For dev without TLS: set TLS_ENABLED=false"
