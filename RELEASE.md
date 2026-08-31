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

This document describes the manual process for releasing Apache DataSketches Rust. The signed source archive in the Apache distribution repository is the artifact approved by the PMC. The crates.io package is an additional distribution channel.

## Release terminology and variables

The release process uses:

- `release_version`: the final version proposed for release.
- `previous_release_version`: the final version superseded by this release.
- `rc_version`: the release candidate and voting round suffix, such as `rc.1`.
- `release_candidate_version`: the candidate tag and `dist/dev` directory, formed as `${release_version}-${rc_version}`.
- `artifact_name`: the final-version source archive name. The RC suffix belongs in the tag and staging directory, not the archive name.

Set the release-specific values once in the shell used for the release. Replace every placeholder before continuing:

```bash
set -euo pipefail

release_version="X.Y.Z"
previous_release_version="A.B.C"
rc_version="rc.N"
signing_key_fingerprint="<40-character primary key fingerprint without spaces>"

if [[ ! "$release_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "invalid release_version: $release_version" >&2
  exit 1
fi
if [[ ! "$previous_release_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "invalid previous_release_version: $previous_release_version" >&2
  exit 1
fi
if [[ ! "$rc_version" =~ ^rc\.[1-9][0-9]*$ ]]; then
  echo "invalid rc_version: $rc_version" >&2
  exit 1
fi
if [[ ! "$signing_key_fingerprint" =~ ^[0-9A-Fa-f]{40}$ ]]; then
  echo "invalid signing key fingerprint" >&2
  exit 1
fi
if [[ "$release_version" == "$previous_release_version" ]]; then
  echo "release and previous release versions must differ" >&2
  exit 1
fi

signing_key_fingerprint="$(
  printf '%s' "$signing_key_fingerprint" | tr '[:lower:]' '[:upper:]'
)"
rc_number="${rc_version#rc.}"
release_candidate_version="${release_version}-${rc_version}"
artifact_stem="apache-datasketches-rust-${release_version}-src"
artifact_name="${artifact_stem}.zip"
archive_root="$artifact_stem"
dist_dev_base_url="https://dist.apache.org/repos/dist/dev/datasketches/rust"
dist_release_base_url="https://dist.apache.org/repos/dist/release/datasketches/rust"
candidate_url="${dist_dev_base_url}/${release_candidate_version}"

printf '%s\n' \
  "release_version=$release_version" \
  "previous_release_version=$previous_release_version" \
  "release_candidate_version=$release_candidate_version" \
  "artifact_name=$artifact_name" \
  "signing_key_fingerprint=$signing_key_fingerprint"

gpg --list-secret-keys "$signing_key_fingerprint"
if ! gpg --batch --with-colons --list-secret-keys "$signing_key_fingerprint" |
  awk -F: '$1 == "uid" { print $10 }' |
  grep -F '@apache.org' >/dev/null; then
  echo "signing key $signing_key_fingerprint has no user ID containing an @apache.org email address" >&2
  exit 1
fi
```

Run this block again if a later step starts in another shell. Increment `rc_version` and recompute the derived values when preparing another candidate.

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
2. **GPG setup**: Use an Apache code-signing key that is present in the DataSketches `KEYS` file and has a user ID containing an `@apache.org` email address.
3. **SVN access**: Confirm that the release manager can write to `dist/dev` and that a PMC member can write to `dist/release`.
4. **Release tools**: Install Git, GPG, SVN, `unzip`, and the tools required by `cargo x lint`.
5. **Clean checkout**: Start from a current checkout of `main` with no local changes.

## Step 1: Prepare the release on `main`

Create a release-preparation pull request that:

1. Changes the `datasketches` package version in `datasketches/Cargo.toml` and `Cargo.lock` to the value of `release_version`.
2. Adds a `v${release_version}` section without a date immediately below `Unreleased` in `CHANGELOG.md` and finalizes its user-facing entries.
3. Passes the complete local release checks:

```bash
cargo x prepare-testdata
cargo x lint
cargo x check
cargo x test
cmp LICENSE datasketches/LICENSE
cmp NOTICE datasketches/NOTICE
package_files="$(cargo package --list -p datasketches)"
printf '%s\n' "$package_files"
grep -Fx LICENSE <<<"$package_files"
grep -Fx NOTICE <<<"$package_files"
cargo publish --dry-run --locked -p datasketches
```

After the pull request is merged, update the local checkout and record the exact commit:

```bash
git switch main
git pull --ff-only origin main
test -z "$(git status --porcelain)"
release_commit="$(git rev-parse HEAD)"
printf 'release_commit=%s\n' "$release_commit"
```

Do not add release changes after creating the release candidate. Fixes require a new candidate.

## Step 2: Create the release candidate tag

Confirm that the release version and changelog are present in the candidate commit:

```bash
git grep -F "version = \"${release_version}\"" -- datasketches/Cargo.toml Cargo.lock
git grep -Fx "## v${release_version}" -- CHANGELOG.md
test -z "$(git status --porcelain)"

release_commit="$(git rev-parse HEAD)"
git tag -a "$release_candidate_version" "$release_commit" \
  -m "Release candidate $rc_number for $release_version"
git push origin "$release_candidate_version"
```

Never move or reuse a release candidate tag after it has been pushed. A later commit on `main` does not invalidate an existing candidate by itself.

## Step 3: Publish the release candidate to crates.io

The crates.io pre-release is a convenience package for community testing. It is not the source artifact submitted for the ASF release vote.

Temporarily change the package version in `datasketches/Cargo.toml` from the value of `release_version` to the value of `release_candidate_version`, then let Cargo update the workspace lockfile:

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
test -z "$(git status --porcelain)"
cargo info "datasketches@$release_candidate_version"
```

## Step 4: Build, sign, and stage the source distribution

Build the source archive directly from the immutable RC tag. The release does not depend on the shared DataSketches deployment script and does not add generated `git.properties` metadata. Run all four subsections in the same shell; if that shell is interrupted, restart Step 4.

### Build the source archive

```bash
release_work_dir="$(mktemp -d)"
artifact_dir="${release_work_dir}/artifacts"
source_check_dir="${release_work_dir}/source-check"

mkdir -p "$artifact_dir" "$source_check_dir"

test "$(git cat-file -t "$release_candidate_version")" = "tag"
candidate_commit="$(git rev-parse "${release_candidate_version}^{commit}")"
printf 'candidate_commit=%s\n' "$candidate_commit"

git archive \
  --format=zip \
  --prefix="${archive_root}/" \
  --output="${artifact_dir}/${artifact_name}" \
  "$release_candidate_version"
```

### Sign the archive and create its checksum

```bash
(
  cd "$artifact_dir"
  gpg \
    --local-user "$signing_key_fingerprint" \
    --armor \
    --detach-sign \
    --digest-algo SHA512 \
    "$artifact_name"
  shasum -a 512 "$artifact_name" > "${artifact_name}.sha512"
)
```

### Verify the local artifacts

```bash
(
  cd "$artifact_dir"
  shasum -a 512 --check "${artifact_name}.sha512"
  gpg --verify "${artifact_name}.asc" "$artifact_name"
  unzip -t "$artifact_name"
)

unzip -q "${artifact_dir}/${artifact_name}" -d "$source_check_dir"
test -f "${source_check_dir}/${archive_root}/LICENSE"
test -f "${source_check_dir}/${archive_root}/NOTICE"
test -f "${source_check_dir}/${archive_root}/Cargo.toml"
test -f "${source_check_dir}/${archive_root}/Cargo.lock"
test -f "${source_check_dir}/${archive_root}/CHANGELOG.md"

expected_artifacts="$(
  printf '%s\n' \
    "$artifact_name" \
    "${artifact_name}.asc" \
    "${artifact_name}.sha512" |
    sort
)"
local_artifacts="$(
  find "$artifact_dir" -mindepth 1 -maxdepth 1 -exec basename {} \; |
    sort
)"
printf '%s\n' "$local_artifacts"
test "$local_artifacts" = "$expected_artifacts"
```

### Upload the candidate with `svn import`

Import the reviewed directory directly into the exact candidate URL. Because the directory was checked against `expected_artifacts`, no other files are included.

```bash
if svn ls "${candidate_url}/" >/dev/null 2>&1; then
  echo "candidate destination already exists" >&2
  exit 1
fi

read -r -p "Type $release_candidate_version to upload this candidate: " confirmation
test "$confirmation" = "$release_candidate_version"

svn import "$artifact_dir" "$candidate_url" \
  -m "Prepare datasketches-rust $release_candidate_version"

remote_artifacts="$(svn ls "${candidate_url}/" | sort)"
printf '%s\n' "$remote_artifacts"
test "$remote_artifacts" = "$expected_artifacts"
```

Do not claim the candidate is uploaded until `svn import` returns a committed revision and the remote listing exactly matches the three local artifacts.

## Step 5: Verify the staged candidate

Verify files downloaded from `dist/dev` rather than the local files used to create them:

```bash
verify_dir="$(mktemp -d)"
verify_gnupg_home="${verify_dir}/gnupg"
verify_cargo_target_dir="${verify_dir}/cargo-target"
mkdir -m 700 "$verify_gnupg_home"

(
  cd "$verify_dir"

  curl --fail --location --remote-name \
    https://downloads.apache.org/datasketches/KEYS
  curl --fail --location --remote-name "${candidate_url}/${artifact_name}"
  curl --fail --location --remote-name "${candidate_url}/${artifact_name}.asc"
  curl --fail --location --remote-name "${candidate_url}/${artifact_name}.sha512"

  shasum -a 512 --check "${artifact_name}.sha512"
  GNUPGHOME="$verify_gnupg_home" gpg --import KEYS
  GNUPGHOME="$verify_gnupg_home" \
    gpg --with-colons --fingerprint "$signing_key_fingerprint" |
    awk -F: '$1 ~ /^f[p]r$/ { print $10 }' |
    grep -Fx -- "$signing_key_fingerprint"
  if ! GNUPGHOME="$verify_gnupg_home" \
    gpg --with-colons --list-keys "$signing_key_fingerprint" |
    awk -F: '$1 == "uid" { print $10 }' |
    grep -F '@apache.org' >/dev/null; then
    echo "KEYS entry for $signing_key_fingerprint has no user ID containing an @apache.org email address" >&2
    exit 1
  fi

  gpg_status="$(
    GNUPGHOME="$verify_gnupg_home" \
      gpg --status-fd 1 --verify "${artifact_name}.asc" "$artifact_name" 2>&1
  )"
  printf '%s\n' "$gpg_status"
  signature_fingerprint="$(
    printf '%s\n' "$gpg_status" |
      awk '$1 == "[GNUPG:]" && $2 == "VALIDSIG" {
        if (NF >= 12 && $12 != "") print $12; else print $3
      }'
  )"
  test "$signature_fingerprint" = "$signing_key_fingerprint"

  unzip "$artifact_name"
  cd "$archive_root"
  export CARGO_TARGET_DIR="$verify_cargo_target_dir"

  cargo package --list -p datasketches
  cargo publish --dry-run --locked -p datasketches
  cargo x prepare-testdata
  cargo x lint
  cargo x check
  cargo x test
)
```

The commands verify both that `signing_key_fingerprint` is present in `KEYS` and that the archive was signed by that exact key. Also inspect the archive for `LICENSE`, `NOTICE`, unexpected binary files, and the expected source version.

## Step 6: Send the release vote

Send the vote to `dev@datasketches.apache.org`.

**Subject:** `[VOTE] Release Apache DataSketches Rust ${release_version} (RC${rc_number})`

Include:

- The exact 40-character Git commit SHA.
- The RC tag and immutable tag URL.
- The `dist/dev` candidate directory and exact artifact filename.
- The SHA512 checksum and signing-key fingerprint.
- A changelog link pinned to the RC tag.
- Verification and test instructions.
- An explicit closing date, time, and time zone at least 72 hours after the vote starts.

Derive the immutable commit and checksum from the tag and staged files:

```bash
candidate_commit="$(git rev-parse "${release_candidate_version}^{commit}")"
checksum_line="$(
  curl --fail --location "${candidate_url}/${artifact_name}.sha512"
)"
artifact_sha512="${checksum_line%% *}"
if [[ ! "$artifact_sha512" =~ ^[0-9A-Fa-f]{128}$ ]]; then
  echo "invalid staged SHA-512 checksum" >&2
  exit 1
fi
printf '%s\n' \
  "candidate_commit=$candidate_commit" \
  "artifact_sha512=$artifact_sha512"
```

Populate this template with the values derived above:

```text
Hi everyone,

I propose releasing Apache DataSketches Rust version ${release_version}.

The release candidate is based on commit:
${candidate_commit}

Source distribution:
https://dist.apache.org/repos/dist/dev/datasketches/rust/${release_candidate_version}/

Git tag:
https://github.com/apache/datasketches-rust/tree/${release_candidate_version}

Changelog:
https://github.com/apache/datasketches-rust/blob/${release_candidate_version}/CHANGELOG.md

Signing key fingerprint:
${signing_key_fingerprint}

SHA-512:
${artifact_sha512}  ${artifact_name}

Testing:
- crates.io RC: cargo add datasketches@${release_candidate_version}
- source: verify the checksum and signature, then run the release checks

Verification:
  shasum -a 512 --check ${artifact_name}.sha512
  gpg --verify ${artifact_name}.asc ${artifact_name}

The vote will remain open until <YYYY-MM-DD HH:MM UTC>, at least 72 hours.

[ ] +1 approve
[ ] +0 no opinion
[ ] -1 disapprove, with a reason
```

A release requires at least three explicit binding `+1` votes from PMC members and more binding `+1` than binding `-1` votes. The release manager has no implicit vote.

### If the vote is cancelled

1. Send a `[CANCEL][VOTE]` message with the reason.
2. Confirm and remove the failed candidate from `dist/dev`:

   ```bash
   svn ls "${dist_dev_base_url}/${release_candidate_version}/"
   svn rm "${dist_dev_base_url}/${release_candidate_version}" \
     -m "Remove failed datasketches-rust $release_candidate_version"
   ```

3. Fix the problem through a new pull request.
4. Increment `rc_version`, recompute the derived variables, create a new immutable tag, and repeat from the crates.io RC step.
5. Yank a broken crates.io pre-release if leaving it selectable would harm testers.

## Step 7: Publish an approved release

Send a `[RESULT][VOTE]` message that lists the binding and non-binding vote totals.

Move the exact approved artifacts from `dist/dev` to `dist/release` without rebuilding or renaming them:

```bash
svn ls "${dist_dev_base_url}/${release_candidate_version}/"
if svn ls "${dist_release_base_url}/${release_version}/" >/dev/null 2>&1; then
  echo "release destination already exists" >&2
  exit 1
fi

svn mv \
  "${dist_dev_base_url}/${release_candidate_version}" \
  "${dist_release_base_url}/${release_version}" \
  -m "Release datasketches-rust $release_version"

svn ls "${dist_release_base_url}/${release_version}/"
if svn ls "${dist_dev_base_url}/${release_candidate_version}/" >/dev/null 2>&1; then
  echo "dev candidate directory still exists" >&2
  exit 1
fi
```

Create the final tag from the approved RC commit:

```bash
candidate_commit="$(git rev-parse "${release_candidate_version}^{commit}")"
git switch --detach "$candidate_commit"
test -z "$(git status --porcelain)"

git tag -a "$release_version" "$candidate_commit" \
  -m "Release version $release_version"
git push origin "$release_version"
```

Publish the final crate from that clean tag:

```bash
test -z "$(git status --porcelain)"
cargo publish --dry-run --locked -p datasketches
cargo publish --locked -p datasketches
cargo info "datasketches@$release_version"
```

## Step 8: Update downloads and announce

1. Confirm that the new source archive is available:

   ```bash
   curl --fail --head \
     "https://downloads.apache.org/datasketches/rust/${release_version}/${artifact_name}"
   ```

2. List current releases, then remove `previous_release_version` from `dist/release`. It remains available from the Apache archive:

   ```bash
   svn ls "$dist_release_base_url"
   svn rm "${dist_release_base_url}/${previous_release_version}" \
     -m "Archive old release datasketches-rust $previous_release_version"
   ```

3. Regenerate the website download table only after removing the old release, because the generator includes every Rust version still present in `dist/release`:

   ```bash
   cd /path/to/datasketches-dist-scripts
   ./createDownloadsInclude.sh /absolute/path/to/datasketches-website
   ```

4. Review the generated `_includes/downloadsInclude.txt`, submit the website change, and verify the download, signature, checksum, and `KEYS` links after it is published.
5. Wait at least one hour after the release first appears on `downloads.apache.org` before announcing it.
6. Send a plain-text announcement from an `@apache.org` address to `dev@datasketches.apache.org` and `announce@apache.org`. Include a short project description and links to the project download page, changelog, crates.io, and docs.rs.
7. Submit a post-release pull request that adds the actual release date to the `v${release_version}` changelog heading, then close the release tracking issue.

## Troubleshooting

### Yank a broken pre-release

```bash
cargo yank --vers "$release_candidate_version" datasketches
```

Do not yank a final release merely because a newer version exists; publish a corrective release instead.

### GPG issues

- Confirm that `signing_key_fingerprint` is present in the DataSketches `KEYS` file and identifies an available secret key.
- Pass both the signature and source archive to `gpg --verify`.
- Do not rely on whichever signing key GPG happens to select by default.

### SVN authentication issues

- Do not claim an upload or move succeeded until SVN returns a committed revision and `svn ls` confirms the remote state.
- If a remote `svn mv` hangs on an interactive credential prompt, use a shallow working copy and commit a local `svn move` instead.
- Never put SVN credentials in this document, shell history, issue text, or release notes.

### crates.io publish failures

- Confirm that the package version is not already published.
- Confirm that `cargo login` is current.
- Inspect `cargo package --list -p datasketches`.
- Run final publish commands from a clean checkout of the approved tag.
