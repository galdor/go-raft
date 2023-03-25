# go-raft
## Introduction
The go-raft library implements the [Raft](https://raft.github.io/) consensus
algorithm.

## Usage
Refer to the [Go package
documentation](https://pkg.go.dev/github.com/galdor/go-raft) for
information about the API.

See the [`kvstore` program](cmd/kvstore/main.go) for a practical use case.

## Licensing
Go-raft is open source software distributed under the
[ISC](https://opensource.org/licenses/ISC) license.

## Contributions
### Open source, not open contribution
[Similar to SQLite](https://www.sqlite.org/copyright.html), go-raft is open
source but not open contribution for multiple reasons:

- It avoid potential intellectual property and licensing issues.
- It removes the burden of reviewing patches and maintaining the resulting
  code.
- It helps keeping the software focused on a clear vision.

While this might be disappointing to you, this choice helps me continue to
build and maintain go-raft.

### Bug reporting
I am thankful for any bug report. Feel free to open issues and include as much
useful information as possible. I cannot however guarantee that I will fix
every bug.

### Ideas and feature suggestions
Ideas about current systems and suggestions for new ones are welcome, either
on GitHub discussions or by [email](mailto:nicolas@n16f.net).

You can also [hire me](mailto:nicolas@exograd.com) for support or to develop
specific features.
