# Rulesets

This folder documents the live "Protect master" ruleset (ruleset id 24269286)
for this repository. `protect-master.json` is its definition.

The ruleset requires:

- a pull request with one approving review, including review from a code owner
  (see `.github/CODEOWNERS`)
- linear history, and squash or rebase merges only
- the status checks `test (windows-latest)` and `test (ubuntu-latest)`

It also blocks branch deletion and force pushes. Repository admins can bypass
it, so a sole maintainer can still merge.

To apply it, run `scripts/setup-branch-protection.sh` (needs the `gh` CLI,
logged in as a repository admin). Or import `protect-master.json` in the GitHub
UI under Settings -> Rules -> Rulesets.
