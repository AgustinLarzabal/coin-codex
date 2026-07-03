# Build Single-Run Operator Console

CoinCodex will add a local prompt-driven terminal Operator Console that guides an operator through seeding Sources, creating one active Crawl Run, processing jobs, and inspecting results. The console will optimize for the painful single-run ingestion loop first, while leaving multi-run/source management and a richer pane-based TUI as later capabilities, because the current CLI friction comes from repeatedly coordinating commands and run ids rather than from broad dashboarding.

The Operator Console will be a first-class CLI command over the existing application services rather than a wrapper that shells out to other CLI commands. Inspection should be backed by a structured run inspection model that can be rendered both as the existing text `inspect-run` output and as visual console summaries.

The console will provide both "process next job" and "process until idle" actions. "Process until idle" will continue through job failures, surface failures in the progress/final summary, and stop when the worker reports no processable job or when a default 100-job per-action safety cap is reached.
