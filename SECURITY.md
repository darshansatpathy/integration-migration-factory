# Security policy

## Reporting a vulnerability

Please do not open a public issue for security problems. Use GitHub's
[private vulnerability reporting](https://github.com/darshansatpathy/integration-migration-factory/security/advisories/new)
for this repository. You should get an acknowledgement within three working days.

## Operating it safely

- The compose file runs Kafka without TLS or authentication, for local development only.
- Flows classified `confidential` get shorter retention in the generated topic specs; in production
  also restrict their topics and DLQs with ACLs to the ingest and deliver principals and the owner.
- DLQ messages contain the original source line, so they carry the same classification as the
  flow's data.
- Flow definitions are parsed with the standard library XML parser and layouts with `yaml.safe_load`;
  filters are a fixed comparison grammar, never evaluated as code.
