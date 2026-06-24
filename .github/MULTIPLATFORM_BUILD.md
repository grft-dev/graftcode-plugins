# Multiplatform RabbitMQ Plugin Build

This document describes the GitHub Actions workflow for building the RabbitMQ Plugin across multiple platforms.

## Supported Platforms

### Available by Default (GitHub-hosted runners)

| OS | Architecture | Runner | Compiler |
|---|---|---|---|
| Windows | x86_64 | `windows-latest` | MSVC |
| Linux | x86_64 | `ubuntu-latest` | GCC |
| macOS | x86_64 | `macos-latest` | Apple Clang |
| macOS | arm64 | `macos-14` | Apple Clang |

### Additional Platforms (Requires self-hosted runner)

| OS | Architecture | Runner | Compiler | Status |
|---|---|---|---|---|
| Windows | arm64 | `windows-arm64` (self-hosted) | MSVC | ⏳ To Enable |
| Linux | arm64 | `linux-arm64` (self-hosted) | GCC | ⏳ To Enable |

## Build Commands

```bash
# In the rabbitmq folder
cd rabbitmq
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release
```

## Output

Built files will appear in:
```
rabbitmq/build/RabbitmqPlugin/
```

Artifacts are automatically uploaded to GitHub Actions and can be downloaded from the "Artifacts" section of each workflow run.

## Requirements

### Global Requirements
- CMake >= 3.22
- C++20 compiler

### Linux (Ubuntu)
```bash
sudo apt-get install build-essential cmake git
```

### macOS
```bash
brew install cmake
```

### Windows
- Visual Studio Build Tools or MSVC compiler

## Creating Releases

### Automatic Release Creation

The workflow automatically creates a GitHub Release when you push a tag:

```bash
git tag v1.0.0
git push origin v1.0.0
```

This will:
1. ✅ Build all platforms in parallel
2. ✅ Download all built artifacts
3. ✅ Package each platform's files into a `.tar.gz` archive
4. ✅ Create a GitHub Release with all archives attached
5. ✅ Auto-generate release notes based on commits

### Release Artifacts

Each platform's built files are packaged as:
- `rabbitmq-Windows-x86_64.tar.gz`
- `rabbitmq-Linux-x86_64.tar.gz`
- `rabbitmq-macOS-x86_64.tar.gz`
- `rabbitmq-macOS-arm64.tar.gz`

## Enabling ARM Builds

### For Windows ARM64

1. Configure a [self-hosted runner](https://docs.github.com/en/actions/hosting-your-own-runners/managing-self-hosted-runners/adding-self-hosted-runners) on a Windows ARM64 machine
2. Uncomment in `.github/workflows/rabbitmq-build.yml`:

```yaml
- os: windows-arm64  # Requires self-hosted ARM64 runner
  platform: "Windows"
  arch: "arm64"
  compiler: "msvc"
```

### For Linux ARM64

1. Configure a [self-hosted runner](https://docs.github.com/en/actions/hosting-your-own-runners/managing-self-hosted-runners/adding-self-hosted-runners) on a Linux ARM64 machine
2. Uncomment in `.github/workflows/rabbitmq-build.yml`:

```yaml
- os: linux-arm64    # Requires self-hosted ARM64 runner
  platform: "Linux"
  arch: "arm64"
  compiler: "gcc"
```

## Workflow Triggers

The workflow runs:
- ✅ On push to `main` branch (only if rabbitmq/ changed)
- ✅ On pull request to `main` branch (only if rabbitmq/ changed)
- ✅ On tag push (e.g., `git tag v1.0.0`) - creates a Release

## Troubleshooting

### Build Fails

1. Check that all dependencies are installed
2. Check the logs in GitHub Actions UI
3. Reproduce locally using the same commands:

```bash
cd rabbitmq
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release
```

### Artifacts Not Uploaded

1. Check if `rabbitmq/build/RabbitmqPlugin/` contains files
2. Check workflow logs for "No files found" message
3. Change `if-no-files-found: warn` to `error` for debugging

### Release Creation Failed

1. Ensure you pushed a tag: `git tag v1.0.0 && git push origin v1.0.0`
2. Check GitHub Actions logs for the release job
3. Verify repository has write permissions on the GitHub token

## Project Structure

```
rabbitmq/
├── CMakeLists.txt              (root CMake config)
├── GraftcodePluginsInterfaces/ (shared interfaces)
├── RabbitmqPlugin/             (plugin source)
│   ├── CMakeLists.txt
│   ├── *.cpp, *.h
│   └── ...
└── RabbitmqPluginTest/         (tests)
    ├── CMakeLists.txt
    └── *.cpp
```

## Future Changes

To add new platforms or project folders:

1. Create a new project folder (e.g., `newproject/`)
2. Create a new workflow `.github/workflows/newproject-build.yml` with similar structure
3. Adjust `paths` in the `on` section to your folder
4. Adjust `working-directory` and artifact paths accordingly

