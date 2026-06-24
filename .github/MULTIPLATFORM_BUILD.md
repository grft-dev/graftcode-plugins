# Multiplatform RabbitMQ Plugin Build

This document describes the GitHub Actions workflow for building the RabbitMQ Plugin across multiple platforms.

## Supported Platforms

### Available by Default (GitHub-hosted runners)

| OS | Architecture | Runner | Compiler |
|---|---|---|---|
| Windows | x86_64 | `windows-latest` | MSVC |
| Windows | arm64 | `windows-arm64` | MSVC |
| Linux | x86_64 | `ubuntu-latest` | GCC |
| Linux | arm64 | `ubuntu-arm64` | GCC |
| macOS | x86_64 | `macos-latest` | Apple Clang |
| macOS | arm64 | `macos-14` | Apple Clang |

All platforms are available without requiring self-hosted runners!

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

ARM builds are already enabled! Just push to trigger builds on:
- `windows-arm64` - Windows ARM64
- `ubuntu-arm64` - Linux ARM64

No additional configuration needed. The workflow will automatically build on all 6 platforms.

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

