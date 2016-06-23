#!/bin/bash

# Copyright 2014 The Kubernetes Authors All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# A set of helpers for starting/running consul for tests

CONSUL_VERSION=${CONSUL_VERSION:-0.6.4}

kube::consul::start() {
  local host=${CONSUL_HOST:-127.0.0.1}
  local port=${CONSUL_PORT:-8500}

  which consul >/dev/null || {
    kube::log::usage "consul must be in your PATH"
    exit 1
  }

  if pgrep consul >/dev/null 2>&1; then
    kube::log::usage "consul appears to already be running on this machine (`pgrep -l consul`) (or its a zombie and you need to kill its parent)."
    kube::log::usage "retry after you resolve this consul error."
    exit 1
  fi

  version=$(consul version | head -n 1 | cut -d "v" -f 2   )
  if [[ "${version}" < "${CONSUL_VERSION}" ]]; then
   kube::log::usage "consul version ${CONSUL_VERSION} or greater required."
   kube::log::info "You can use 'hack/install-consul.sh' to install a copy in third_party/."
   exit 1
  fi

  # Start consul
  CONSUL_DIR=$(mktemp -d 2>/dev/null || mktemp -d -t test-consul.XXXXXX)
  # Todo: launch a consul cluster instead
  kube::log::info "consul agent -dev -data-dir=${CONSUL_DIR} -bind=${host} -http-port=${port} >/dev/null 2>/dev/null"
  consul agent -dev -data-dir=${CONSUL_DIR} -bind=${host} -http-port=${port} >/dev/null 2>/dev/null &
  CONSUL_PID=$!

  echo "Waiting for consul to come up."
  kube::util::wait_for_url "http://${host}:${port}/v1/catalog/nodes" "consul: " 0.25 80
  curl -X PUT -d 'test' "http://${host}:${port}/v1/kv/k8s_consul_integration/_test"
}

kube::consul::stop() {
  kill "${CONSUL_PID-}" >/dev/null 2>&1 || :
  wait "${CONSUL_PID-}" >/dev/null 2>&1 || :
}

kube::consul::clean_CONSUL_dir() {
  rm -rf "${CONSUL_DIR-}"
}

kube::consul::cleanup() {
  kube::consul::stop
  kube::consul::clean_CONSUL_dir
}

kube::consul::install() {
  (
    cd "${KUBE_ROOT}/third_party"
    curl -fsSL --retry 3 https://bintray.com/gonzalo-mustwin/must-win-consul/download_file?file_path=linux_amd64.zip | tar xzf -
    ln -fns "consul-v${CONSUL_VERSION}-linux-amd64" consul
    kube::log::info "consul v${CONSUL_VERSION} installed. To use:"
    kube::log::info "export PATH=\${PATH}:$(pwd)/consul")
}
