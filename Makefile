# Package not ready for stable.

build:
	# ... build ...
	cargo +nightly build
	# ... test ...
	cargo +nightly test --no-run
	# ... bench ...
	cargo +nightly bench --no-run
	# ... doc ...
	cargo +nightly doc
	# ... bins ...
	cargo +nightly build --release --bin robt --features=robt
	# ... meta commands ...
	cargo +nightly clippy --all-targets --all-features
flamegraph:
	echo "nothing here yet"
prepare:
	check.sh
	perf.sh
clean:
	cargo clean
	rm -f check.out perf.out flamegraph.svg perf.data perf.data.old
