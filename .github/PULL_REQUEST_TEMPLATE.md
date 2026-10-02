## Summary

<!-- What does this change, and why? -->

## Test plan

<!-- How did you check it? Commands you ran, manual steps, screenshots. -->

## Checklist

- [ ] Tests added or updated for behavior changes
- [ ] `python -m unittest discover -p "test_*.py"` passes (run from `Jellyball/`)
- [ ] `ruff check Jellyball` passes (run from the repo root)
- [ ] A `changelog.d/<slug>.<type>.md` fragment added for user-visible changes (see `changelog.d/README.md`)
- [ ] No secrets, `.env` files, databases or real provider hostnames in code, tests or fixtures (use `*.example.test`)
- [ ] Streaming-path changes were also run through `JELLYBALL_E2E=1 python -m unittest test_e2e_tools` (from `Jellyball/`, needs ffmpeg)
