# IOCPNetworkEngine
Windows IOCP-based server/client network engine library

# Info
High-performance C++ IOCP-based server/client network engine for Windows
Provides asynchronous server/client networking, session management, scheduling, and memory-pool-based resource handling for scalable network applications.

# Features
- IOCP-based asynchronous network architecture
- Server and client session support
- Session pool and lifecycle management
- Scheduler-based session processing
- Memory pool helpers for efficient allocation
- Modular Core library integration via submodule

# Dependencies
- [Core](./Modules/Core) as a submodule
- Windows WinSock2 / MSWSock
- C++20
- MSVC (Visual Studio 2022)

# Build Environment
- C++20
- MSVC (Visual Studio 2022)
- Windows 10/11 x64

# Project Structure
- `Core/` : IOCP-related core engine components
- `Session/` : session classes and session pools
- `Scheduler/` : ready/client session scheduling logic
- `HandlerTable/` : packet handler table definitions
- `Memory/` : memory pool helpers
- `Modules/Core/` : external Core library submodule

# Notes
- This repository uses `Modules/Core` as a submodule.
- Make sure submodules are initialized before building.
- The solution is configured for Windows-based development with Visual Studio 2022.

# Clone
- Clone with submodules:
```bash
git clone --recurse-submodules https://github.com/KommyButterCream/IOCPNetworkEngine.git
```
- If already cloned without submodules:
- ```bash
git submodule update --init --recursive
```
