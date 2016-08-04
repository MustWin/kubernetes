# Consul storage integration
## Enable Consul as storage backend

Start `kube-apiserver` with the following parameters:

-  `--storage-backend="consul"`
- `--consul-servers="http://HOST:PORT"`

## Currently not implemented
- Usage of certificates
- TTL (not supported by Consul)
