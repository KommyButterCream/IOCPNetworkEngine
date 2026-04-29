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
- Modular integration with the shared Core library

# Dependencies
- [Core](../Core)
- Windows WinSock2 / MSWSock
- C++20
- MSVC (Visual Studio 2022)

# Build Environment
- C++20
- MSVC (Visual Studio 2022)
- Windows 10/11 x64

# Project Structure
- `Buffer/` : send/receive packet buffers and packet pool helpers
- `Core/` : IOCP-related core engine components
- `Session/` : session classes and session pools
- `Scheduler/` : ready/client session scheduling logic
- `HandlerTable/` : packet handler table definitions
- `Memory/` : memory pool helpers
- `Network/` : socket options and overlapped I/O helpers
- `Protocol/` : packet header, packet id, and system packet definitions
- `Job/` : session job definitions

# Repository Layout
This project expects `IOCPNetworkEngine` and `Core` to be placed under the same parent directory.

Example:
```text
Module/
+-- Core/
+-- IOCPNetworkEngine/
```

The Visual Studio solution references the shared Core project at `../Core/Core/Core.vcxproj`.

# Notes
- The shared Core library is managed as a sibling repository/project.
- Open `IOCPNetworkEngine.sln` with Visual Studio 2022.
- Build the x64 configuration to produce the IOCPNetworkEngine DLL.
