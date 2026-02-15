# Config Package (`internal/config`)

This package loads and validates tolerance configuration used by leader startup.

## Tolerance Config

The tolerance value is read from a key-value file.

Format:

- `TOLERANCE=N`

Rules:

- File must exist.
- Key must be exactly `TOLERANCE`.
- `N` must be an integer greater than zero.

## Tests

`config_test.go` validates common cases:

- Valid value is parsed correctly (`TOLERANCE=3`).
- Invalid key fails (`MY_VAL=3`).
- Invalid value fails (`TOLERANCE=-1`).

Run:

```bash
go test -v ./internal/config
```
