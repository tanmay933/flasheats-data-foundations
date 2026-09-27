# Class 8 Knowledge Check — Answers

1. **B — Retry a bounded number of times.** HTTP 500 is treated as a transient API failure.
2. **C — Fail the validation gate with a useful message.** A missing required field is a schema problem, not something to hide with a retry.
3. **C — Replace the same logical output without duplicate effects.** That is the intended idempotent behavior.
4. **C — `Dispatch page=5 status=429 attempt=2/3 wait=1s`.** It gives operationally useful context.
5. **B — Data-quality rules become executable gates on every run.**
