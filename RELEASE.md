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

A streamlined release process that relies on CI for validation and focuses on essential manual steps.

## What CI Handles Automatically

CI validates every commit and ensures:

- All tests pass on multiple platforms (Linux, macOS, Windows)
- Code passes linting (`cargo x lint`)
- Documentation builds successfully
- Examples run correctly
- MSRV (Minimum Supported Rust Version) compatibility

## Prerequisites (One-Time Setup)

1. **crates.io access**: `cargo login` and save your API token
2. **GPG setup**: Ensure `gpg-agent` is running and your key is configured
3. **SVN access**: Checkout Apache dist directories:
   ```bash
   mkdir -p ~/apache/dist/{dev,release}/datasketches
   cd ~/apache/dist/dev/datasketches
   svn co https://dist.apache.org/repos/dist/dev/datasketches/ .
   cd ~/apache/dist/release/datasketches
   svn co https://dist.apache.org/repos/dist/release/datasketches/ .
   ```

## Step 1: Create Release Candidate Tag

```bash
# Simply tag the current commit as RC
git checkout main
git tag -a 0.3.0-RC1 -m "Release candidate 1 for 0.3.0"
git push origin 0.3.0-RC1
```

## Step 2: Publish Release Candidate to crates.io

This allows the community to test the actual published crate.

```bash
# Temporarily change version for RC publish (DON'T commit!)
sed -i '' 's/version = ".*"/version = "0.3.0-rc.1"/' datasketches/Cargo.toml

# Verify package contents
cargo package --list -p datasketches

# Dry run
cargo publish --dry-run -p datasketches

# Publish
cargo publish -p datasketches

# Revert the temporary change
git checkout -- datasketches/Cargo.toml Cargo.lock
```

Verify: Visit https://crates.io/crates/datasketches and confirm `0.3.0-rc.1` is published.

## Step 3: Create Signed Source Distribution

```bash
# Navigate to dist scripts (adjust path as needed)
cd ~/apache/dist/dev/datasketches/scripts

# Run the packaging script (requires GPG)
./bashDeployToDist.sh \
  /path/to/datasketches-rust \
  datasketches-rust \
  0.3.0-RC1
```

This script will:

1. Create a source archive from the git tag
2. Generate GPG signature (`.asc`)
3. Generate SHA512 checksum (`.sha512`)
4. Upload to https://dist.apache.org/repos/dist/dev/datasketches/rust/0.3.0-RC1/

Verify the files are accessible at the URL above.

## Step 4: Send [VOTE] Email

Send to: dev@datasketches.apache.org

**Subject:** `[VOTE] Release Apache DataSketches Rust 0.3.0 (RC1)`

**Email template:**

```
Hi everyone,

I propose releasing Apache DataSketches Rust version 0.3.0.

Source distribution:
https://dist.apache.org/repos/dist/dev/datasketches/rust/0.3.0-RC1/

GitHub tag:
https://github.com/apache/datasketches-rust/releases/tag/0.3.0-RC1

Testing (choose one or both):
- crates.io RC: cargo add datasketches@0.3.0-rc.1
- From source: Download, verify signatures, cargo x test

To verify signatures:
  curl -O https://dist.apache.org/repos/dist/dev/datasketches/rust/0.3.0-RC1/datasketches-rust-0.3.0-RC1-src
  curl -O https://dist.apache.org/repos/dist/dev/datasketches/rust/0.3.0-RC1/datasketches-rust-0.3.0-RC1-src.asc
  gpg --verify datasketches-rust-0.3.0-RC1-src.asc

Notable changes: [link to CHANGELOG or summary]

Vote will remain open for at least 72 hours.

[ ] +1 approve
[ ] +0 no opinion
[ ] -1 disapprove (and reason why)
```

**Wait 72+ hours.** Need at least 3 +1 PMC votes and more +1s than -1s.

**If vote fails:**

```bash
# Fix the issues, commit to main, then tag the new RC
git tag -a 0.3.0-RC2 -m "Release candidate 2 for 0.3.0"
git push origin 0.3.0-RC2
# Then repeat from Step 2 (with version 0.3.0-RC2)
```

## Step 5: Close Vote & Publish Release

After successful vote, send [VOTE-RESULT] email summarizing the outcome and proceed to publish the release:

```bash
# Move artifacts from dev to release
cd ~/apache/dist/dev/datasketches/scripts
./moveDevToRelease.sh rust 0.3.0-RC1 0.3.0

# Update Cargo.toml to final release version (this is the only version commit!)
cd /path/to/datasketches-rust
git checkout main
sed -i '' 's/version = ".*"/version = "0.3.0"/' datasketches/Cargo.toml
git add datasketches/Cargo.toml
git commit -m "chore: release 0.3.0"
git push origin main

# Create final release tag
git tag -a 0.3.0 -m "Release version 0.3.0"
git push origin 0.3.0

# Publish final version to crates.io
cargo publish --dry-run -p datasketches
cargo publish -p datasketches
```

## Step 6: Create GitHub Release

Go to https://github.com/apache/datasketches-rust/releases and draft a new release.

## Step 7: Post-Release Housekeeping

1. **Update website**:
   ```bash
   cd ~/apache/dist/dev/datasketches/scripts
   ./createDownloadsInclude.sh /path/to/datasketches-website
   ```

2. **Clean up old releases** from dist (keep only latest):
   ```bash
   cd ~/apache/dist/release/datasketches
   svn rm rust/0.2.0
   svn commit -m "Archive old release 0.2.0"
   ```

3. **Send [ANNOUNCE] email** after 24 hours (allows mirror propagation):
    - To: dev@datasketches.apache.org, announce@apache.org
    - Include links to: GitHub release, crates.io, docs.rs, Apache dist

---

## Troubleshooting

**Need to yank a crate?**

```bash
cargo yank --vers 0.3.0-RC1 datasketches
```

Only for broken pre-releases. For released versions, publish a patch instead.

**GPG issues?**

- Ensure key is in KEYS file and uploaded to public keyservers
- Check `gpg-agent` is running: `ps aux | grep gpg-agent`
- Try: `eval $(gpg-agent --daemon)`

**crates.io publish fails?**

- Verify `cargo login` is current
- Check package size: `cargo package --list -p datasketches`
- Ensure all dependencies are published
