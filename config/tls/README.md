# Development certificates

Generate local mTLS files from the repository root:

```bash
go run ./cmd/certgen
```

The generated certificates and private keys are ignored by Git. They are for local development only. Use a managed PKI and a separate certificate for every node in production.
