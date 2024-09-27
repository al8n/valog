#!/bin/bash

set -ex

export ASAN_OPTIONS="detect_odr_violation=0 detect_leaks=0"

# Run address sanitizer
RUSTFLAGS="-Z sanitizer=address --cfg all_tests" \
cargo test --tests --target x86_64-unknown-linux-gnu --all-features

# Run leak sanitizer
RUSTFLAGS="-Z sanitizer=leak --cfg all_tests" \
cargo test --tests --target x86_64-unknown-linux-gnu --all-features

RUSTFLAGS="--cfg all_tests -Zsanitizer=memory -Zsanitizer-memory-track-origins" \
RUSTDOCFLAGS="-Zsanitizer=memory -Zsanitizer-memory-track-origins" \
cargo test -Zbuild-std --release --tests --target x86_64-unknown-linux-gnu --features memmap

# Run thread sanitizer
RUSTFLAGS="-Z sanitizer=thread --cfg all_tests" \
cargo -Zbuild-std test --tests --target x86_64-unknown-linux-gnu --all-features
