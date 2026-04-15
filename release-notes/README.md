# Release Notes

One file per release, named after the Reckon tag (no `v` prefix): `4.16.2.md`, `4.17.0-beta.1.md`, etc.

The release workflow (`.github/workflows/release.yml`) reads `release-notes/${TAG}.md` and uses it as the body of the GitHub Release when a tag is pushed. If the file is missing, the workflow fails before publishing.

## Flow

1. Create `release-notes/<next-version>.md`
2. Test the build locally at that version:
   ```bash
   ./gradlew clean build :zulia-server:distTar :zulia-tools:distTar \
     -Preckon.scope=minor -Preckon.stage=final
   ./gradlew publishToMavenLocal -Preckon.scope=minor -Preckon.stage=final
   ```
3. Commit the notes file (and any other release prep).
4. Create and push the tag:
   ```bash
    ./gradlew reckonTagCreate -Preckon.scope=patch -Preckon.stage=final
   OR
   ./gradlew reckonTagCreate -Preckon.scope=minor -Preckon.stage=final
   OR 
   ./gradlew reckonTagCreate -Preckon.scope=major -Preckon.stage=final
   # build with tests
   ./gradlew clean
   ./gradlew
   # push tag
   
   ```
5. CI builds, publishes to Maven Central, and creates the GitHub Release with the server/tools tars attached.

## Format

Use categorized sections. The content becomes the GitHub Release body verbatim, so write for users:

```markdown
## New Features
- Short, user-facing description of the feature.

## Improvements
- ...

## Bug Fixes
- ...

## Breaking Changes
- ...

## Dependencies
- Lucene 10.4.0 -> 10.5.0
```
