# Maintaining asynctrino

## Versions

The version comes from git tags through uv-dynamic-versioning; there is no version field and no
generated version file. A commit tagged `vX.Y.Z` builds as `X.Y.Z`, and one tagged
`vX.Y.Z-rc.N` as `X.Y.ZrcN`. Untagged commits build as the next patch's development release,
such as `0.1.1.dev3+1a2b3c4`, and a checkout without tags as `0.0.1.devN`.

## Releasing

Pushing a tag runs `.github/workflows/release.yml`:

| Tag            | Build and install check | TestPyPI | PyPI (approval) | GitHub Release |
| -------------- | ----------------------- | -------- | --------------- | -------------- |
| `vX.Y.Z-rc.N`  | yes                     | yes      | no              | no             |
| `vX.Y.Z`       | yes                     | yes      | yes             | yes            |

The build checks that the packages' version matches the tag and that their metadata passes
`twine check --strict`, then installs the wheel and the sdist with the `sqlalchemy` extra on
Python 3.14 and 3.15 and imports them. Publishing uses PyPI trusted publishing, so the repository
holds no tokens.

1. Make sure `main` is green in CI and holds everything to ship.
2. Optionally, try a release candidate:

   ```shell
   git tag -a v0.1.0-rc.1 -m "asynctrino 0.1.0rc1"
   git push origin v0.1.0-rc.1
   pip install --index-url https://test.pypi.org/simple/ \
     --extra-index-url https://pypi.org/simple/ "asynctrino[sqlalchemy]==0.1.0rc1"
   ```

3. File the unreleased changes under the version and commit the changelog. The release workflow
   refuses a stable tag whose version has no `CHANGELOG.md` section.

   ```shell
   make changelog VERSION=v0.1.0
   git commit -am "chore(changelog): release 0.1.0"
   git push origin main
   ```

4. Tag that commit and push the tag:

   ```shell
   git tag -a v0.1.0 -m "asynctrino 0.1.0"
   git push origin v0.1.0
   ```

5. Approve the `pypi` deployment in the workflow run. The GitHub Release follows, with the
   version's changelog section as its notes and the packages attached.

`make dist/check` runs the same build and install check locally, and `make release/notes`
prints the notes of the release tagged at `HEAD`.

### One-time setup

- On PyPI and TestPyPI, add a trusted publisher for project `asynctrino`: owner `gledi-ai`,
  repository `asynctrino`, workflow `release.yml`, environment `pypi` on PyPI and `testpypi` on
  TestPyPI. Before the first upload, use "pending publisher".
- In the GitHub repository, create the environments `testpypi` and `pypi`, give `pypi` required
  reviewers, and limit both to `v*` tags.
- Protect `v*` tags with a ruleset so only maintainers can create them.

## Trino versions

asynctrino supports the latest two Trino releases (ADR-0003). They're pinned only in
`TRINO_VERSIONS` in `tests/integration/trino.py`; when Trino releases, bump both as a pair, run
the integration suite against each, and update the README's supported-versions table.

## Catch-ups

After 0.1.0, asynctrino tracks Upstream (trinodb/trino-python-client) with one Catch-up per
Upstream release. The baseline is Upstream 0.340.0, so the first Catch-up covers 0.340.0 to the
next release.

1. In an Upstream checkout, list the changes between the two release tags, for example
   `git log --reverse 0.340.0..0.341.0`.
2. Open one branch and pull request for the Catch-up, titled
   `Catch-up: Upstream 0.340.0..0.341.0`.
3. In its description, list every Upstream change as **ported** or **excluded with a reason**,
   such as legacy prepared statements, an unsupported Python or Trino version, or a feature
   that is out of scope. Mark any fix that also applies to the Sibling (aiotrino), so it can be
   ported there too.
4. Port each change into the protocol core or the HTTP layer with its tests, in Conventional
   Commits that name the Upstream change, such as
   `fix(spooling): retry Segment downloads on 503 (Upstream #NNN)`.
5. If a ported change introduces a deliberate break, add it to the README's "Differences from
   aiotrino and Upstream", and update the Upstream version the README names.
6. Merge, then release as usual.
