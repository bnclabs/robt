#! /usr/bin/env bash

cargo +nightly build
# cargo +stable build // we are still using unstable features

cargo +nightly doc
# cargo +stable doc // we are still using unstable features.

cargo +nightly test
# cargo +stable test // we are still using unstable features.

cargo +nightly bench
# cargo +stable bench // we are still using unstable features.

cargo +nightly clippy --all-targets --all-features

