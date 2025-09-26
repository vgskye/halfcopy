# halfcopy
Copy a file or directory from device A to device B.

## Demo
[![asciicast](https://asciinema.org/a/uf9HuqXsoVmhr2Oz0BQae8Obv.svg)](https://asciinema.org/a/uf9HuqXsoVmhr2Oz0BQae8Obv)

## Install
Find the binaries in [Releases](https://github.com/vgskye/halfcopy/releases/latest), or install from crates.io with:
```sh
cargo install halfcopy
```

## Usage
To send:
```sh
halfcopy send path/to/file/or/directory
```

This will print a "coupon" to stdout.

To receive:
```sh
halfcopy recv coupon-here
```