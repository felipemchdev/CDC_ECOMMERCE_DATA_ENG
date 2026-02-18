# AGENTS.md

## Git Commit Policy (mandatory)
- Every meaningful change MUST be committed immediately.
- Use Conventional Commits only: feat:, fix:, refactor:, chore:, test:, docs:.
- Never finish a turn with uncommitted tracked changes.
- If a commit cannot be made, explain why in the final message.

## Branch Workflow
- Use `dev` for implementation commits.
- Merge `dev` into `qa` for validation.
- Open PR from `qa` to `master`.
