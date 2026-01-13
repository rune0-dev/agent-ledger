# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

Instead, please report them via email to: **security@rune0.com**

You should receive a response within 48 hours. If for some reason you do not, please follow up via email to ensure we received your original message.

Please include the following information in your report:

- Type of issue (e.g., SQL injection, race condition, data exposure)
- Full paths of source file(s) related to the issue
- Location of the affected source code (tag/branch/commit or direct URL)
- Step-by-step instructions to reproduce the issue
- Proof-of-concept or exploit code (if possible)
- Impact of the issue, including how an attacker might exploit it

## What to Expect

1. **Acknowledgment**: We will acknowledge receipt of your vulnerability report within 48 hours.

2. **Assessment**: We will assess the vulnerability and determine its severity and impact. We may reach out for additional information.

3. **Resolution**: We will work on a fix and coordinate disclosure timing with you.

4. **Disclosure**: Once the fix is released, we will publicly acknowledge your contribution (unless you prefer to remain anonymous).

## Security Best Practices for Users

### PostgreSQL Configuration

- Use SSL/TLS connections (`sslmode=require` or `sslmode=verify-full`)
- Use dedicated database users with minimal privileges
- Enable connection pooling with appropriate limits

### Argument Handling

- Validate and sanitize all user inputs before passing to `ToolCall.args`
- Use the built-in `max_args_size_bytes` option to prevent oversized payloads
- Consider implementing secret redaction for sensitive data

### Network Security

- Deploy behind a firewall
- Use private networking between application and database
- Enable audit logging for compliance requirements

## Known Limitations

### Stale Detection Trade-offs

When stale detection is enabled (`stale.after_ms > 0`), there is a theoretical window where a handler could execute twice (once by the original worker, once by the takeover worker). The ledger guarantees data consistency (only one result is recorded), but side effects may occur twice.

**Mitigation**: Set `stale.after_ms` significantly higher than your longest expected handler duration, or keep stale detection disabled for handlers with critical side effects.

See [PRODUCTION_READINESS_GAPS.md](PRODUCTION_READINESS_GAPS.md) for detailed analysis.
