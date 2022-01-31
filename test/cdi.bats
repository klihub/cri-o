#!/usr/bin/env bats

load helpers

function setup() {
	setup_test
	pod_config="$TESTDATA/sandbox_config.json"
	ctr_config="$TESTDIR/config.json"
	cdidir=$TESTDIR/cdi
	enable_cdi
}

function enable_cdi() {
	cat << EOF > "$CRIO_CONFIG_DIR/zz-cdi.conf"
[crio.runtime]
cdi_spec_dirs = [
    "$cdidir",
]
EOF
}

function teardown() {
	cleanup_test
}

function wait_until_exit() {
	ctr_id=$1
	# Wait for container to exit
	attempt=0
	while [ $attempt -le 100 ]; do
		attempt=$((attempt + 1))
		output=$(crictl inspect -o table "$ctr_id")
		if [[ "$output" == *"State: CONTAINER_EXITED"* ]]; then
			[[ "$output" == *"Exit Code: ${EXPECTED_EXIT_STATUS:-0}"* ]]
			return 0
		fi
		sleep 1
	done
	return 1
}

function write_cdi_spec() {
	mkdir -p "$cdidir"
	cat << EOF > "$cdidir/vendor0.yaml"
cdiVersion: "0.2.0"
kind: "vendor0.com/device"
devices:
  - name: loop8
    containerEdits:
      env:
        - LOOP8=present
      deviceNodes:
        - path: /dev/loop8
          type: b
          major: 7
          minor: 8
          fileMode: 0640
  - name: loop9
    containerEdits:
      env:
        - LOOP9=present
      deviceNodes:
        - path: /dev/loop9
          type: b
          major: 7
          minor: 9
          fileMode: 0644
containerEdits:
  env:
    - VENDOR0=injected
EOF
}

function verify_injected_vendor0() {
	# shellcheck disable=SC2016
	run -0 crictl exec --sync "$1" sh -c 'echo $VENDOR0'
	[ "$output" = "injected" ]
}

function verify_injected_loop8() {
	# shellcheck disable=SC2016
	run -0 crictl exec --sync "$1" sh -c 'echo $LOOP8'
	[ "$output" = "present" ]
	run -0 crictl exec --sync "$1" sh -c 'stat -c %t.%T /dev/loop8'
	[ "$output" = "7.8" ]
	run -0 crictl exec --sync "$1" sh -c 'stat -c %a /dev/loop8'
	[ "$output" = "640" ]
}

function verify_injected_loop9() {
	# shellcheck disable=SC2016
	run -0 crictl exec --sync "$1" sh -c 'echo $LOOP9'
	[ "$output" = "present" ]
	run -0 crictl exec --sync "$1" sh -c 'stat -c %t.%T /dev/loop9'
	[ "$output" = "7.9" ]
	run -0 crictl exec --sync "$1" sh -c 'stat -c %a /dev/loop9'
	[ "$output" = "644" ]
}

function write_invalid_cdi_spec() {
	mkdir -p "$cdidir"
	cat << EOF > "$cdidir/vendor1.yaml"
cdiVersion: "0.2.0"
kind: "vendor1.com/device"
devices:
  invalid data
EOF
}

function prepare_ctr_without_cdidev {
	cp "$TESTDATA/container_config.json" "$ctr_config"
}

function prepare_ctr_with_cdidev {
	jq ".annotations |= . + { \"cdi.k8s.io/test\": \"vendor0.com/device=loop8,vendor0.com/device=loop9\" }" \
		"$TESTDATA/container_sleep.json" > "$ctr_config"
}

function prepare_ctr_with_unknown_cdidev {
	jq ".annotations |= . + { \"cdi.k8s.io/test\": \"vendor0.com/device=loop10\" }" \
		"$TESTDATA/container_sleep.json" > "$ctr_config"
}

@test "no CDI errors, create ctr without CDI devices" {
	write_cdi_spec
	start_crio

	run -0 crictl runp "$pod_config"
	pod_id="$output"

	prepare_ctr_without_cdidev
	run -0 crictl create "$pod_id" "$ctr_config" "$pod_config"
	ctr_id="$output"

	run -0 crictl start "$ctr_id"
	run -0 wait_until_exit "$ctr_id"
}

@test "no CDI errors, create ctr with CDI devices" {
	write_cdi_spec
	start_crio

	run -0 crictl runp "$pod_config"
	pod_id="$output"

	prepare_ctr_with_cdidev
	run -0 crictl create "$pod_id" "$ctr_config" "$pod_config"
	ctr_id="$output"
	run -0 crictl start "$ctr_id"

	verify_injected_vendor0 "$ctr_id"
	verify_injected_loop8 "$ctr_id"
	verify_injected_loop9 "$ctr_id"
}

@test "no CDI errors, fail to create ctr with unresolvable CDI devices" {
	write_cdi_spec
	start_crio

	run -0 crictl runp "$pod_config"
	pod_id="$output"

	prepare_ctr_with_unknown_cdidev
	run ! crictl create "$pod_id" "$ctr_config" "$pod_config"
}

@test "CDI registry refresh" {
	start_crio

	run -0 crictl runp "$pod_config"
	pod_id="$output"

	prepare_ctr_with_cdidev
	run ! crictl create "$pod_id" "$ctr_config" "$pod_config"

	write_cdi_spec
	run -0 crictl create "$pod_id" "$ctr_config" "$pod_config"
	ctr_id="$output"
	run -0 crictl start "$ctr_id"

	verify_injected_vendor0 "$ctr_id"
	verify_injected_loop8 "$ctr_id"
	verify_injected_loop9 "$ctr_id"
}

@test "CDI with errors, create ctr without CDI devices" {
	write_cdi_spec
	write_invalid_cdi_spec
	start_crio

	run -0 crictl runp "$pod_config"
	pod_id="$output"

	prepare_ctr_without_cdidev
	run -0 crictl create "$pod_id" "$ctr_config" "$pod_config"
	ctr_id="$output"

	run -0 crictl start "$ctr_id"
	run -0 wait_until_exit "$ctr_id"
}

@test "CDI with errors, create ctr with (unaffected) CDI devices" {
	write_cdi_spec
	write_invalid_cdi_spec
	start_crio

	run -0 crictl runp "$pod_config"
	pod_id="$output"

	prepare_ctr_with_cdidev
	run -0 crictl create "$pod_id" "$ctr_config" "$pod_config"
	ctr_id="$output"
	run -0 grep "CDI registry has errors" "$CRIO_LOG"
	run -0 crictl start "$ctr_id"

	verify_injected_vendor0 "$ctr_id"
	verify_injected_loop8 "$ctr_id"
	verify_injected_loop9 "$ctr_id"
}
