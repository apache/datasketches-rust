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

# Release process for Rust components

**NOTES:**

* This process covers major and minor releases only. Bug-fix releases, which increment the third digit, are performed on
  an A.B.X branch and not on main, but otherwise follow a similar process.
* Some of these operations can be performed either on the Command-Line or in your IDE, whatever you prefer.

## Preparation

### Verify project health

* Confirm correctness for:
    * LICENSE
    * NOTICE -- check for copyright dates
    * README.md
    * .asf.yaml
    * .gitignore
    * Cargo.toml -- ensure workspace metadata is correct

* From Command Line or IDE:
    * Run all checks: `cargo x lint`
    * Run unit tests: `cargo x test`
    * Run `cargo update` to ensure Cargo.lock is updated
    <!-- TODO: * Run code coverage: `cargo llvm-cov --workspace` (target > 90%). -->
    * Run tests on all platforms (see CI workflow)
    * Confirm that documentation builds: `cargo doc --open`
    * Confirm that all **temporary** branches are checked into main and/or deleted, both local and remote
    * Confirm any new bug fixes have corresponding tests
    * Check that all dependencies are at stable versions (no pre-release dependencies in Cargo.toml)

### Verify release tooling

* From Command Line at project root:
    * Confirm GitHub repository is current and git status is clean
    * Ensure you have publishing credentials configured:
        * If not set: `cargo login` and enter your crates.io API token
    * At major version releases, search for deprecated code and remove at **Major Versions** only:
        * `grep -r "deprecated" --include="*.rs" src/`

## Create permanent release branch & release candidate version preparation

* From IDE or Command Line:
    * Create new **Permanent Branch**: "A.B.X"
    * Modify `datasketches/Cargo.toml` version:
        * Change version to `version = "A.B.X"`
    * Commit the changes with message: "chore: prepare A.B.X release"
    * Write down the Git hash: example: 40c6f4f
    * Push Branch "A.B.X" to origin:
        * `git push -u origin A.B.X`
    * Do explicit push of tags on branch "A.B.X" to origin:
        * `git push origin --tags`
    * **DO NOT MERGE THIS PERMANENT BRANCH INTO THE DEFAULT BRANCH**

* From a web browser at origin website: github.com/apache/datasketches-rust
    * Select the A.B.X branch
    * Confirm that the tag: A.B.X-rc.1 exists and that the tag is on the latest commit with the correct Git hash
    * **DO NOT CREATE PR OR MERGE THIS PERMANENT BRANCH INTO THE DEFAULT BRANCH**

## Publish release candidate to crates.io

**IMPORTANT:** This step publishes a pre-release version to crates.io for community testing. The `-rc.N` suffix
indicates this is not a final release.

* Return to release branch A.B.X:
    * `git checkout A.B.X`
* Create Annotated TAG: A.B.X-rc.1
    * `git tag -a A.B.X-rc.1 -m "release candidate 1 for version A.B.X"`
* Modify `datasketches/Cargo.toml` version **temporarily**:
    * Change version to `version = "A.B.X-rc.1"`
* Review the package contents:
    * `cargo package --list -p datasketches`
    * Ensure no unwanted files are included
    * Verify LICENSE, NOTICE, README.md are included
* Run publish in dry-run and verify output:
    * `cargo publish --dry-run -p datasketches`
* Publish to crates.io:
    * `cargo publish -p datasketches`
* Verify the published crate:
    * Visit https://crates.io/crates/datasketches
    * Confirm A.B.X-rc.1 is visible
    * Test installation: `cargo add datasketches@A.B.X-rc.1` in a test project
* Reset all temporary changes with `git checkout -f A.B.X`:
    * `cat datasketches/Cargo.toml | grep version` # should show A.B.X

## Create and/or checkout local *dist/dev* directories on your system

* If you have not already, on your system create the two directory structures that mirror the dist.apache.org/repos/
  directories:
    * `mkdir -p dist/dev/datasketches/`
    * `mkdir -p dist/release/datasketches/`
* Checkout both "dev" and "release" directories:
    * Open a terminal in the dist/dev/datasketches directory and do a checkout:
        * `svn co https://dist.apache.org/repos/dist/dev/datasketches/ .`      #Note the DOT
        * `svn status`    # make sure it is clean
    * Open a terminal in the dist/release/datasketches directory and do a checkout:
        * `svn co https://dist.apache.org/repos/dist/release/datasketches/ .`  #Note the DOT
        * `svn status`    # make sure it is clean

## Create the candidate Apache release distribution on *dist/dev*

### Create primary zip files & signatures

* You will need the following arguments:
    * Absolute path of target project.basedir on your system
    * Artifact name: datasketches-rust
    * GitHub Tag: A.B.X-rc.1 (or rc.N)

* Start a new terminal in the *dist/dev/datasketches/scripts* directory on your system:
    * To confirm *gpg-agent* is running type:
        * `ps -axww | grep gpg`  # you should see something like:
            * *64438 ?? 0:30.33 gpg-agent --homedir /Users/\<name\>/.gnupg --use-standard-socket --daemon*
        * To start GPG if GPG Agent is not running:
            * `eval $(gpg-agent --daemon)`
    * Run the deployment script:
        * `./bashDeployToDist.sh /Users/\<name\>/dev/git/Apache/datasketches-rust datasketches-rust A.B.X-rc.1`
        * Follow the instructions
        * NOTE: if you get the error "gpg: signing failed: No pinentry":
            * open .gnupg/gpg-agent.conf
            * change to: pinentry-program /usr/local/bin/pinentry-tty
            * reload the gpg agent in the terminal: `gpg-connect-agent reloadagent /bye`
            * restart the ./bashDeployToDist script
        * Close the terminal

* Check and grab the web URL: *https://dist.apache.org/repos/dist/dev/datasketches/rust/A.B.X-rc.1/*
    * There should be 3 files: \*-src.zip, \*-src.zip.asc, \*-src.zip.sha512
    * Verify signatures:
        * `gpg --verify datasketches-rust-A.B.X-rc.1-src.zip.asc datasketches-rust-A.B.X-rc.1-src.zip`
        * `sha512sum -c datasketches-rust-A.B.X-rc.1-src.zip.sha512`

## Prepare & Send [VOTE] Letter to dev@

* See VoteTemplates directory for a recent example
* Include in your vote email:
    * Link to the source distribution on dist.apache.org
    * Link to the GitHub tag
    * Link to the crates.io release candidate: https://crates.io/crates/datasketches/A.B.X-rc.1
    * Instructions for testing:
        * How to verify signatures
        * How to test the crates.io release candidate: `cargo add datasketches@A.B.X-rc.1`
        * How to build from source: `cargo build --workspace`
        * How to run tests: `cargo x test`
    * Link to the CHANGELOG or list of notable changes
* Vote must be open for at least 72 hours
* Require at least 3 +1 PMC votes and more +1 than -1 votes
* If vote is not successful:
    * Fix the identified problems
    * Increment RC number: A.B.X-rc.2
    * Publish new release candidate to crates.io: `A.B.X-rc.2`
    * Repeat the process
* After a successful vote, return to **this point** and continue...

## Prepare & Send [VOTE-RESULT] Letter to dev@

* See VoteTemplates directory for a recent example
* Declare that the vote is closed
* Summarize PMC vote results:
    * List all +1 votes (noting PMC members)
    * List any +0 or -1 votes
    * Confirm the vote passed

## Move files from dist/dev to dist/release

* Use dist/dev/datasketches/scripts/moveDevToRelease.sh script to move the approved release candidate to the
  destination:
    * `./moveDevToRelease.sh rust A.B.X-rc.1 A.B.X`
* Verify the move:
    * Check https://dist.apache.org/repos/dist/release/datasketches/rust/A.B.X/
    * Confirm all three files are present and accessible

## Publish final release to crates.io

* Return to release branch A.B.X:
    * `git checkout A.B.X`
* Verify you're on the correct branch and commit:
    * `git describe --tags` # should show A.B.X-rc.1 or the final RC that passed
    * `cat datasketches/Cargo.toml | grep version` # should show A.B.X
* Review the package contents:
    * `cargo package --list -p datasketches`
    * Ensure no unwanted files are included
    * Verify LICENSE, NOTICE, README.md are included
* Run publish in dry-run and verify output:
    * `cargo publish --dry-run -p datasketches`
* Publish the final release:
    * `cargo publish -p datasketches`
* Verify the published crate:
    * Visit https://crates.io/crates/datasketches
    * Confirm A.B.X is now the latest version (no -rc suffix)
    * The previous -rc.N versions will remain visible but not marked as latest

## Create & document release tag on GitHub

* Open your IDE and switch to the recently created release branch A.B.X:
    * `git checkout A.B.X`
* Find the A.B.X-rc.N tag in that branch
* At that same git hash, create a new tag A.B.X (without the rc.N):
    * `git tag -a A.B.X -m "release version A.B.X"`
* From the Command Line: Push the new tag to origin:
    * `git push origin --tags`
* On the GitHub component site document the release:
    * Go to: https://github.com/apache/datasketches-rust/releases
    * Click "Draft a new release"
    * Select tag: A.B.X
    * Release title: "Apache DataSketches Rust A.B.X"
    * Description should include:
        * Overview of major changes
        * Link to detailed CHANGELOG
        * Installation instructions: `cargo add datasketches@A.B.X`
        * Link to documentation
        * List of contributors (use `git shortlog -sn A.B-1.0..A.B.X` to generate)
    * Publish release

## Update website downloads.md "Latest Source Zip Files" table

* This script assumes that the remote .../dist/release/datasketches/... directories are up-to-date with no old releases
* Start a new terminal in the ../dist/dev/datasketches/scripts directory on your system:
* Make sure your local website directory is pointing to main and up-to-date
* Run the following with the argument specifying the location of your local website directory:
    * `./createDownloadsInclude.sh /Users/\<name\>/.../datasketches-website`
* When this is done, commit and push the changes to the website repository

## Cleanup old releases from dist/release

* Per Apache policy, only the latest release of each supported branch should remain on dist.apache.org
* Older releases are automatically archived to archive.apache.org
* From your dist/release/datasketches directory:
    * Remove old release artifacts:
        * `svn rm rust/<old-version>`
        * `svn commit -m "remove old release A.B-1.0 (archived)"`
* Users can still access old releases from:
    * archive.apache.org
    * crates.io (all versions remain available)
    * GitHub tags

## Prepare Announce Letter to dev@

* ASF requests that you wait 24 hours to publish Announce letter to allow propagation to mirrors
* Use recent template from VoteTemplates directory
* Include:
    * Summary of what's new in this release
    * Installation instructions: `cargo add datasketches`
    * Links to:
        * GitHub release: https://github.com/apache/datasketches-rust/releases/tag/A.B.X
        * crates.io: https://crates.io/crates/datasketches
        * Documentation: https://docs.rs/datasketches/A.B.X/
        * Source distribution: https://dist.apache.org/repos/dist/release/datasketches/rust/A.B.X/
    * Vote results summary
    * Thanks to contributors

* Also send announcement to:
    * announce@apache.org (must be sent from @apache.org address)
    * users@datasketches.apache.org

## Troubleshooting

### If signature verification fails:

* Ensure your GPG key is in KEYS file
* Check key is uploaded to public keyservers
* Verify gpg-agent is running correctly

### If there is the need to yank a published crate version:

* `cargo yank --vers A.B.X-rc.N datasketches`
* This doesn't delete the version but marks it as not recommended
* Only do this for broken pre-release versions
* For final releases, prefer publishing a patch version

### If vote fails:

* Address the issues raised
* Publish new RC with incremented number: A.B.X-rc.N+1
* Update git tag: A.B.X-rc.N+1
* Restart vote process
* No need to wait 24 hours between RCs during the vote period

## Update These Instructions

* If you have updated this file or any of the scripts, please:
    * Update this file in the repository
    * Update dist/dev/datasketches scripts if needed
    * Consider contributing improvements back to the community