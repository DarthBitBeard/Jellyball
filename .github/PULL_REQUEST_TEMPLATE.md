## Summary

<!-- What does this change, and why? -->

## Test plan

<!-- How did you check it? Commands you ran, manual steps, screenshots. -->

## Checklist

- [ ] Tests added or updated for behavior changes
- [ ] `python -m unittest discover -p "test_*.py"` passes (run from `Jellyball/`)
- [ ] `ruff check Jellyball` passes (run from the repo root)
- [ ] `CHANGELOG.md` updated under `[Unreleased]` for user-visible changes
- [ ] No secrets, `.env` files, databases or real provider hostnames in code, tests or fixtures (use `*.example.test`)
- [ ] Streaming-path changes were also run through `JELLYBALL_E2E=1 python -m unittest test_e2e_tools` (from `Jellyball/`, needs ffmpeg)
