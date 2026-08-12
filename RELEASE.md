<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Release Process for Rust Components

This document describes the manual process for releasing Apache DataSketches Rust. The examples use `0.4.0` as the release version and `0.4.0-rc.1` as the first release candidate.

The signed source archive in the Apache distribution repository is the artifact approved by the PMC. The crates.io package and GitHub release are additional distribution channels.

## What CI validates

Pull requests targeting `main` run CI checks for:

- Tests on Linux, macOS, and Windows with both the MSRV and stable Rust.
- Linting, formatting, documentation, spelling, and license headers through `cargo x lint`.
- All targets under each individual feature and with all features through `cargo x check`.
- Serialization compatibility tests after preparing the pinned TCK data.
- A locked `Cargo.lock`.

CI does not run on every push to `main` or on release tags. Complete the local release checks below and make sure the release-preparation pull request has passed CI before creating a release candidate.

## Prerequisites

1. **crates.io access**: Run `cargo login` with an account authorized to publish `datasketches`.
2. **GPG setup**: Use an Apache code-signing key that is present in the DataSketches `KEYS` file.
3. **SVN access**: Confirm that the release manager can write to `dist/dev` and that a PMC member can write to `dist/release`.
4. **Release tools**: Install Git, GPG, SVN, `unzip`, and the tools required by `cargo x lint`.
5. **Clean checkout**: Start from a current checkout of `main` with no local changes.

## Step 1: Prepare the release on `main`

Create a release-preparation pull request that:

1. Changes the `datasketches` package version in `datasketches/Cargo.toml` and `Cargo.lock` to `0.4.0`.
2. Changes the `Unreleased` heading in `CHANGELOG.md` to `v0.4.0` and finalizes its user-facing entries.
3. Passes the complete local release checks:

```bash
cargo x prepare-testdata
cargo x lint
cargo x check
cargo x test
cargo package --list -p datasketches
cargo publish --dry-run --locked -p datasketches
```

After the pull request is merged, update the local checkout and record the exact commit:

```bash
git switch main
git pull --ff-only origin main
git status --short
git rev-parse HEAD
```

The status output must be empty. Do not add release changes after creating the release candidate; fixes require a new candidate.

## Step 2: Create the release candidate tag

Confirm that the release version and changelog are present in the candidate commit:

```bash
git grep 'version = "0.4.0"' -- datasketches/Cargo.toml Cargo.lock
git grep '^## v0.4.0$' -- CHANGELOG.md
git status --short
```

Create and push an annotated tag:

```bash
git tag -a 0.4.0-rc.1 -m "Release candidate 1 for 0.4.0"
git push origin 0.4.0-rc.1
```

Never move or reuse a release candidate tag after it has been pushed.

## Step 3: Publish the release candidate to crates.io

The crates.io pre-release is a convenience package for community testing. It is not the source artifact submitted for the ASF release vote.

Temporarily change the package version in `datasketches/Cargo.toml` from `0.4.0` to `0.4.0-rc.1`, then let Cargo update the workspace lockfile:

```bash
cargo check -p datasketches
git diff -- datasketches/Cargo.toml Cargo.lock
cargo package --list --allow-dirty -p datasketches
cargo publish --dry-run --locked --allow-dirty -p datasketches
cargo publish --locked --allow-dirty -p datasketches
```

Only the package version and its `Cargo.lock` entry should differ from the release candidate tag. Restore both files immediately after publishing:

```bash
git restore datasketches/Cargo.toml Cargo.lock
git status --short
```

The status output must be empty. Verify that `0.4.0-rc.1` is available from <https://crates.io/crates/datasketches>.

## Step 4: Create the signed source distribution

Use the shared DataSketches packaging script:

```bash
cd /path/to/datasketches-dist-scripts
./bashDeployToDist.sh \
  /absolute/path/to/datasketches-rust \
  datasketches-rust \
  0.4.0-rc.1
```

The script revision used for the release must correctly support dotted RC tags. Before approving its SVN commit, verify that its summary contains exactly:

```text
FileVersion (String) : 0.4.0
ZipName (File)       : apache-datasketches-rust-0.4.0-src.zip
LeafDir (target)     : 0.4.0-rc.1
RemoteSvnBasePath    : https://dist.apache.org/repos/dist/dev/datasketches
```

If any value differs, answer `N` and stop. In particular, do not publish a candidate whose source archive filename contains the RC suffix or whose target is `dist/release`.

The candidate directory must contain exactly:

```text
apache-datasketches-rust-0.4.0-src.zip
apache-datasketches-rust-0.4.0-src.zip.asc
apache-datasketches-rust-0.4.0-src.zip.sha512
```

at:

<https://dist.apache.org/repos/dist/dev/datasketches/rust/0.4.0-rc.1/>

## Step 5: Verify the staged candidate

Verify the files downloaded from `dist/dev` rather than the local files used to create them:

```bash
verify_dir=$(mktemp -d)
cd "$verify_dir"

curl -O https://dist.apache.org/repos/dist/dev/datasketches/KEYS
curl -O https://dist.apache.org/repos/dist/dev/datasketches/rust/0.4.0-rc.1/apache-datasketches-rust-0.4.0-src.zip
curl -O https://dist.apache.org/repos/dist/dev/datasketches/rust/0.4.0-rc.1/apache-datasketches-rust-0.4.0-src.zip.asc
curl -O https://dist.apache.org/repos/dist/dev/datasketches/rust/0.4.0-rc.1/apache-datasketches-rust-0.4.0-src.zip.sha512

shasum -a 512 --check apache-datasketches-rust-0.4.0-src.zip.sha512
gpg --import KEYS
gpg --show-keys --with-fingerprint KEYS
gpg --verify \
  apache-datasketches-rust-0.4.0-src.zip.asc \
  apache-datasketches-rust-0.4.0-src.zip

unzip apache-datasketches-rust-0.4.0-src.zip
cd apache-datasketches-rust-0.4.0-src

cargo x prepare-testdata
cargo x lint
cargo x check
cargo x test
cargo package --list -p datasketches
cargo publish --dry-run --locked -p datasketches
```

Confirm that the signing fingerprint matches the fingerprint stated in the vote email. Also inspect the archive for `LICENSE`, `NOTICE`, unexpected binary files, and the expected source version.

## Step 6: Send the release vote

Send the vote to `dev@datasketches.apache.org`.

**Subject:** `[VOTE] Release Apache DataSketches Rust 0.4.0 (RC1)`

Include:

- The exact 40-character Git commit SHA.
- The RC tag and immutable tag URL.
- The `dist/dev` candidate directory and exact artifact filename.
- The SHA512 checksum and signing-key fingerprint.
- A changelog link pinned to the RC tag.
- Verification and test instructions.
- An explicit closing date, time, and time zone at least 72 hours after the vote starts.

Example:

```text
Hi everyone,

I propose releasing Apache DataSketches Rust version 0.4.0.

The release candidate is based on commit:
<40-character commit SHA>

Source distribution:
https://dist.apache.org/repos/dist/dev/datasketches/rust/0.4.0-rc.1/

Git tag:
https://github.com/apache/datasketches-rust/tree/0.4.0-rc.1

Changelog:
https://github.com/apache/datasketches-rust/blob/0.4.0-rc.1/CHANGELOG.md

Signing key fingerprint:
<full fingerprint>

Testing:
- crates.io RC: cargo add datasketches@0.4.0-rc.1
- source: verify the checksum and signature, then run the release checks

Verification:
  shasum -a 512 --check apache-datasketches-rust-0.4.0-src.zip.sha512
  gpg --verify apache-datasketches-rust-0.4.0-src.zip.asc apache-datasketches-rust-0.4.0-src.zip

The vote will remain open until <YYYY-MM-DD HH:MM UTC>, at least 72 hours.

[ ] +1 approve
[ ] +0 no opinion
[ ] -1 disapprove, with a reason
```

A release requires at least three explicit binding `+1` votes from PMC members and more binding `+1` than binding `-1` votes. The release manager has no implicit vote.

### If the vote is cancelled

1. Send a `[CANCEL][VOTE]` message with the reason.
2. Remove the failed candidate from `dist/dev`:

   ```bash
   svn rm \
     https://dist.apache.org/repos/dist/dev/datasketches/rust/0.4.0-rc.1 \
     -m "Remove failed datasketches-rust 0.4.0 RC1"
   ```

3. Fix the problem through a new pull request.
4. Create a new immutable tag such as `0.4.0-rc.2` and repeat from the crates.io RC step.
5. Yank a broken crates.io pre-release if leaving it selectable would harm testers.

## Step 7: Publish an approved release

Send a `[RESULT][VOTE]` message that lists the binding and non-binding vote totals.

Move the exact approved artifacts from `dist/dev` to `dist/release` without rebuilding or renaming them:

```bash
svn mv \
  https://dist.apache.org/repos/dist/dev/datasketches/rust/0.4.0-rc.1 \
  https://dist.apache.org/repos/dist/release/datasketches/rust/0.4.0 \
  -m "Release datasketches-rust 0.4.0"
```

Create the final tag from the approved RC commit:

```bash
git switch --detach 0.4.0-rc.1
git tag -a 0.4.0 -m "Release version 0.4.0"
git push origin 0.4.0
```

Publish the final crate from that clean tag:

```bash
git status --short
cargo publish --dry-run --locked -p datasketches
cargo publish --locked -p datasketches
```

The status output must be empty. Verify `0.4.0` on crates.io.

## Step 8: Create the GitHub release

Create a GitHub release from the `0.4.0` tag. Use the `v0.4.0` changelog section for the release notes and link to the Apache DataSketches download page for the official source archive.

Do not describe GitHub-generated source archives as the official Apache source distribution.

## Step 9: Update downloads and announce

1. Confirm that the new files are available under <https://downloads.apache.org/datasketches/rust/0.4.0/>.
2. Remove the superseded release from `dist/release`. It remains available from the Apache archive:

   ```bash
   svn rm \
     https://dist.apache.org/repos/dist/release/datasketches/rust/0.3.0 \
     -m "Archive old release datasketches-rust 0.3.0"
   ```

3. Regenerate the website download table only after removing the old release, because the generator includes every Rust version still present in `dist/release`:

   ```bash
   cd /path/to/datasketches-dist-scripts
   ./createDownloadsInclude.sh /absolute/path/to/datasketches-website
   ```

4. Review the generated `_includes/downloadsInclude.txt`, submit the website change, and verify the download, signature, checksum, and `KEYS` links after it is published.
5. Wait at least one hour after the release first appears on `downloads.apache.org` before announcing it.
6. Send a plain-text announcement from an `@apache.org` address to `dev@datasketches.apache.org` and `announce@apache.org`. Include a short project description and links to the project download page, changelog, crates.io, docs.rs, and GitHub release.
7. Submit a post-release pull request that adds a new empty `Unreleased` section above `v0.4.0`, then close the release tracking issue.

## Troubleshooting

### Yank a broken pre-release

```bash
cargo yank --vers 0.4.0-rc.1 datasketches
```

Do not yank a final release merely because a newer version exists; publish a corrective release instead.

### GPG issues

- Confirm that the signing key is present in the DataSketches `KEYS` file and that its full fingerprint is correct.
- Confirm that `gpg-agent` is running.
- Pass both the signature and source archive to `gpg --verify`.

### crates.io publish failures

- Confirm that the package version is not already published.
- Confirm that `cargo login` is current.
- Inspect `cargo package --list -p datasketches`.
- Run the publish command from a clean checkout of the intended tag.
