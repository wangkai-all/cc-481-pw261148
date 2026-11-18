"""Local lakehouse disaster-recovery drill toolkit.

Modules:
    config    - filesystem layout / DRContext
    state     - JSON persistence + timestamps
    jsonl     - JSON Lines helpers
    hashing   - checksum helpers
    lakehouse - simulated local lakehouse (tables, metadata catalog, CDC log)
    gates     - quality gate definitions (reused at backup and recovery time)
    backup    - backup engine (file versions, metadata snapshots, CDC positions)
    recovery  - staged point-in-time recovery engine with quarantine + resume
    web       - stdlib HTTP server exposing the JSON API and the web console
"""

__version__ = "1.0.0"
