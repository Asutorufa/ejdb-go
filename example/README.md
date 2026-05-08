# Example

Run:

```bash
go run ./example
```

The example creates a Pebble database under `example/data/demo.pebble` and demonstrates:

- Query objects and placeholder binding
- Indexes, including a unique index
- `apply` / `del`
- Projection + collection join
- `Exec` visitor
- `ForceSync` + `Backup` + backup reopen
