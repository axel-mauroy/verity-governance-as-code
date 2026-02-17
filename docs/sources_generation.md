# Automatic Sources Generation (sources.yaml)

The `verity` CLI provides a robust command to automatically discover data files and maintain your `sources.yaml` configuration with production-grade stability.

## Command Usage

```bash
verity sources generate [FLAGS] [OPTIONS]
```

### Options
- `--data-dir <path>`: Directory to scan (default: `data`).
- `--owner <name>`: Default owner for **new** discovered sources.
- `--pii`: Mark **new** sources as containing PII by default.
- `--interactive`: (Future) Enabling CLI prompts for name overrides.

## Production-Ready Features

### 1. Stability (Smart Merge)
Verity uses a non-destructive merge strategy. If a file path is already tracked in `sources.yaml`, the generator will **preserve** its existing name, owner, and governance settings. This prevents "implicit renaming" that could break downstream SQL models.

### 2. Deterministic Naming
For new files, Verity uses a stable `domain_filename` pattern:
- `data/finance/transactions.csv` -> `finance_transactions`
- `data/customer/profiles.csv` -> `customer_profiles` (if not already named `customer`)

### 3. Governance Injection
To ensure generated sources are immediately compatible with Verity's strict governance checks, you can pre-fill metadata:
- New sources are tagged with the provided `--owner`.
- PII flags are set based on the `--pii` flag.

## Example Workflow

1. **Initial Generation**:
   ```bash
   verity sources generate --owner "data-ops"
   ```
2. **Adding New Data**:
   After adding `data/sales/orders.csv`, run again:
   ```bash
   verity sources generate
   ```
   - Existing sources remain untouched.
   - `sales_orders` is added with default governance.

## Benefits
- **Zero-Downtime Config**: Adding files never breaks existing references.
- **Compliance-First**: Sources come pre-tagged for auditability.
- **Simplicity**: No more manual YAML editing.
