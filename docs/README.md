# SchemaX Documentation

Welcome to the SchemaX documentation!

## 📚 Documentation Index

### Getting Started

| Document | Description | Audience |
|----------|-------------|----------|
| **[Quickstart Guide](QUICKSTART.md)** | Complete getting started guide with step-by-step instructions | All users |
| **[Testing Guide](../TESTING.md)** | How to test all components | Developers & testers |

### Technical Documentation

| Document | Description | Audience |
|----------|-------------|----------|
| **[Architecture](ARCHITECTURE.md)** | System design, operation log architecture, data models | Developers |
| **[Development Guide](DEVELOPMENT.md)** | Building from source, contributing guidelines | Contributors |

### Package-Specific Documentation

| Package | Documentation | Description |
|---------|--------------|-------------|
| **VS Code Extension** | [README](../packages/vscode-extension/README.md) | Extension-specific docs |
| **Python SDK** | [README](../packages/python-sdk/README.md) | SDK and CLI reference |

## 🚀 Quick Links

### For Users

- **First time?** Start with the [Quickstart Guide](QUICKSTART.md)
- **Testing?** See the [Testing Guide](../TESTING.md)
- **VS Code?** Read [Extension README](../packages/vscode-extension/README.md)
- **Python/CLI?** Read [SDK README](../packages/python-sdk/README.md)

### For Developers

- **Architecture:** [ARCHITECTURE.md](ARCHITECTURE.md)
- **Contributing:** [DEVELOPMENT.md](DEVELOPMENT.md)
- **Contributing Guidelines:** [../CONTRIBUTING.md](../CONTRIBUTING.md)

### For CI/CD

- **Examples:** [../examples/github-actions/](../examples/github-actions/)
- **Python SDK:** [SDK README](../packages/python-sdk/README.md)

## 📖 Documentation by Topic

### Installation & Setup

- Installing the VS Code Extension → [Quickstart: VS Code](QUICKSTART.md#vs-code-extension)
- Installing Python SDK/CLI → [Quickstart: Python](QUICKSTART.md#python-sdk--cli)

### Using SchemaX

- Creating your first schema → [Quickstart: Your First Schema](QUICKSTART.md#your-first-schema)
- Generating SQL migrations → [Quickstart: Generating SQL](QUICKSTART.md#generating-sql)
- CI/CD integration → [Quickstart: CI/CD](QUICKSTART.md#cicd-integration)

### Technical Details

- How operation logs work → [Architecture: Core Concepts](ARCHITECTURE.md)
- File structure → [Architecture: File Structure](ARCHITECTURE.md)
- State management → [Architecture: State Loading](ARCHITECTURE.md)

### Development

- Building from source → [Development: Building](DEVELOPMENT.md)
- Running tests → [Testing Guide](../TESTING.md)
- Contributing code → [Contributing](../CONTRIBUTING.md)

## 🎯 Quick Start Paths

### Path 1: Visual Designer User

1. Read [Quickstart Guide](QUICKSTART.md) - VS Code section
2. Launch extension (Press F5)
3. Follow along with examples
4. Generate SQL when ready

### Path 2: CLI/SDK User

1. Read [Python SDK README](../packages/python-sdk/README.md)
2. Install: `pip install -e packages/python-sdk`
3. Try: `schemax validate`
4. Integrate into your CI/CD

### Path 3: Developer/Contributor

1. Read [Architecture](ARCHITECTURE.md)
2. Read [Development Guide](DEVELOPMENT.md)
3. Review [Contributing Guidelines](../CONTRIBUTING.md)
4. Run tests: `./scripts/smoke-test.sh`

## 📁 Repository Structure

```
schemax/
├── README.md                          # Project overview
├── TESTING.md                         # Testing guide
├── CONTRIBUTING.md                    # How to contribute
│
├── docs/                              # Documentation (you are here)
│   ├── README.md                      # This file
│   ├── QUICKSTART.md                  # Getting started
│   ├── ARCHITECTURE.md                # Technical design
│   └── DEVELOPMENT.md                 # Development guide
│
├── packages/
│   ├── vscode-extension/              # VS Code Extension
│   │   └── README.md                  # Extension docs
│   └── python-sdk/                    # Python SDK & CLI
│       └── README.md                  # SDK/CLI docs
│
└── examples/                          # Working examples
    ├── basic-schema/                  # Sample project
    ├── github-actions/                # CI/CD templates
    └── python-scripts/                # SDK examples
```

## 🔍 Find What You Need

### I want to...

**...get started quickly**
→ [Quickstart Guide](QUICKSTART.md)

**...understand how SchemaX works**
→ [Architecture](ARCHITECTURE.md)

**...test SchemaX**
→ [Testing Guide](../TESTING.md)

**...contribute code**
→ [Development Guide](DEVELOPMENT.md) + [Contributing](../CONTRIBUTING.md)

**...use SchemaX in CI/CD**
→ [Quickstart: CI/CD](QUICKSTART.md#cicd-integration) + [Examples](../examples/github-actions/)

**...use the Python API**
→ [Python SDK README](../packages/python-sdk/README.md)

**...use the VS Code extension**
→ [Extension README](../packages/vscode-extension/README.md)

## 💡 Examples

All examples are in the [examples/](../examples/) directory:

- **Basic Schema**: `examples/basic-schema/` - Complete working example
- **GitHub Actions**: `examples/github-actions/` - CI/CD workflows
- **Python Scripts**: `examples/python-scripts/` - SDK usage examples

## 🆘 Getting Help

### Common Issues

See the Troubleshooting sections in:
- [Quickstart Guide](QUICKSTART.md#troubleshooting)
- [Testing Guide](../TESTING.md#troubleshooting-tests)

### Need More Help?

- **Issues**: [GitHub Issues](https://github.com/vb-dbrks/schemax/issues)
- **Discussions**: [GitHub Discussions](https://github.com/vb-dbrks/schemax/discussions)

## 🎓 Learning Path

**Beginner** → Quickstart → Try examples → Generate SQL

**Intermediate** → Read Architecture → Use Python API → Set up CI/CD

**Advanced** → Read Development → Contribute features → Extend functionality

---

**Start here:** [Quickstart Guide](QUICKSTART.md) 🚀

