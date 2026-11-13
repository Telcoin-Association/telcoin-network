You are one track of a multi-agent review of the `sls_storage_v2_3` branch of telcoin-network
(Rust) against `origin/main`. Work from /Users/sstanf/work/telcoin-network. Read
`tasks/review-storage-v2/GROUND_RULES.md` first and follow it exactly (scope, do-not-flag lists,
output format, checkpointing, no repo edits). Record HEAD from `tasks/review-storage-v2/HEAD.txt`.
The diff of a file is `git diff $(sed -n 2p tasks/review-storage-v2/HEAD.txt)...HEAD -- <file>`;
read the whole file at HEAD, not only the hunks. The coordinator runs the baseline (fmt, check,
clippy); run only targeted tests you need. Your deliverable is the file named in your brief.
Finish with a 10-line summary in your reply: the finding titles with severities, and the top three
things you ruled out. Checkpoint your output file as you go.
