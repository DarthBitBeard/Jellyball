# Contributing to Jellyball

Thanks for helping improve Jellyball. The default branch is **`master`**.
It is protected: changes land through pull requests so they can be reviewed
and CI can run before merge.

## How community collaboration works

You do **not** need write access to this repository.

1. **Fork** https://github.com/DarthBitBeard/Jellyball
2. **Clone your fork** and create a branch off `master`
3. **Push** commits to your fork
4. Open a **pull request** into `DarthBitBeard/Jellyball` → `master`
5. Wait for CI (unit tests, ruff, e2e, Docker smoke) and maintainer review

```bash
git clone https://github.com/<your-username>/Jellyball.git
cd Jellyball
git checkout -b fix/my-change
# ... edit, test ...
git push -u origin fix/my-change
# Then open a PR on GitHub against DarthBitBeard/Jellyball:master
```

Trusted maintainers can be added as **collaborators** (Settings → Collaborators).
Even with write access, they should still use branches + PRs — direct pushes
to `master` are blocked by branch protection.

## Local checks before you open a PR

From `Jellyball/`:

```bash
python -m unittest discover -p "test_*.py"
ruff check .
```

Optional (needs ffmpeg):

```bash
export JELLYBALL_E2E=1
python -m unittest test_e2e_tools -v
```

Optional: run `pip install pre-commit && pre-commit install` to have ruff and a
few basic hygiene hooks run on every commit.

## What makes a good PR

- Keep the change focused; separate unrelated fixes
- Add or update unit tests when behavior changes
- Do not commit `.env`, databases, installer output, or secrets
- Prefer the public GitHub noreply email in commits if you care about privacy

## Reporting issues

Use GitHub Issues for bugs and feature requests. Include OS, install method
(Windows installer / Docker / source), Jellyball version, and relevant log
lines from the data directory (redact passwords and webhook URLs).

Do not report security problems in a public issue. Follow
[SECURITY.md](SECURITY.md) instead.
