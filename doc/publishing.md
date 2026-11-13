# Publishing

This project uses sbt's native Sonatype Central Portal support. It does not
use the retired OSSRH staging endpoints or the deprecated `sbt-sonatype`
plugin.

## Prerequisites

- A verified `io.teknek.arizona.spark` namespace in Sonatype Central
- A published GPG key
- A Sonatype Central Portal user token
- sbt 1.12.15, selected by `project/build.properties`

For local publishing, configure the token in
`~/.sbt/sonatype_central_credentials`:

```properties
host=central.sonatype.com
user=YOUR_TOKEN_USERNAME
password=YOUR_TOKEN_PASSWORD
```

Do not commit this file or any token values.

## Validate Before Release

```bash
./sbt/sbt test
./sbt/sbt IntegrationTest/test
./sbt/sbt publishM2
```

The Testcontainers integration tests require the Airlock rootless Docker setup
described in `agents.md`.

## Release

Use a tag for the release version. `sbt-dynver` derives the version from tags,
so there is no Maven-style `versions:set` step.

```bash
git tag v0.0.5
git push origin v0.0.5
./sbt/sbt +publishSigned
./sbt/sbt sonaUpload
```

`sonaUpload` uploads the signed bundle to the Central Portal without releasing
it. Validate the deployment at:

```text
https://central.sonatype.com/publishing/deployments
```

Then use the Portal's **Publish** action. For an automated release after
validation, use `sonaRelease` instead of `sonaUpload`.

## Next Snapshot

After the release, continue development from the branch. `sbt-dynver` will
derive a snapshot version from the last release tag and subsequent commits;
there is no need to create a `-SNAPSHOT` tag:

```bash
./sbt/sbt show version
```

Snapshot publication uses the Central snapshots repository. Release tags are
immutable, so choose the final version before publishing.
