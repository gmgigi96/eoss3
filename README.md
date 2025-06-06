# EOS plugin for Versity S3 gateway

This project icontains the EOS backedn plugin for the Versity S3 Gateway. It allos the Versity S3 Gateway to use CERN's EOS as a storage backend.

The plugin acts as a highly efficient translation layer. It converts S3 API calls into corresponding operations on the EOS distributed filesystem by using:
* gRPC: for all metadata operations (listing, stat, delete, etc.).
* HTTP: for all high-throughput data operations (uploading/downloading objects).

## Architecture

The data flow is as follows:
<Insert photo>

1. An S3 request is received by the Versity S3 Gateway
2. The gateway's core engine parses the request and forwards it to the configured EOS backend.
3. The EOS backend inspect the operation type:
    * for _metadata operations_ (e.g. `ListObjects`, `DeleteObject`), the backend makes a call to the MGM using the EOS gRPC interface.
    * for _data operations_ (e.g. `PutObject`, `GetObject`), the backend makes an HTTP call to the MGM and follows the FST redirection.
4. The response from EOS is translated back into an S3 response and sent to the client.

## Prerequisites

Before proceeding, complete the following setup steps:

1. **Install the Versity S3 Gateway**
Run this command to install the required version of the gateway:
```
go install github.com/versity/versitygw/cmd/versitygw@v1.0.14
```

2. **Enable the HTTP Interface on EOS**
Ensure that your EOS instance has the HTTP interface enabled, following [this](TODO) doc.

3. **Configure Authentication Tokens**
Generate and configure the security tokens on your EOS instance. You will need:
```
eos vid enable grpc
eos vid enable https

eos vid set map -grpc key:<key> vuid:<uid> vgid:<gid>
eos vid set map -https key:<key> vuid:<uid> vgid:<gid>

eos vid add gateway <hostname> grpc
eos vid add gateway <hostname> https
```

## Configuration

The EOS S3 plugin is configured using a YAML file. By default, the plugin will look for this file at `/etc/eoss3/plugin.yaml`.

Here is an example configuration file

```yaml
--
grpc_url: "eospilot.cern.ch:50051"
http_url: "https://eospilot.cern.ch:8444"
authkey: "secret"

compute_md5: true

buckets:
  driver: "local"
  folder: "/var/eoss3/s3config"
```

#### Configuration parameters

| Parameter | Description |
| :--- | :--- |
| **`grpc_url`** | The address and port for the EOS gRPC service.|
| **`http_url`** | The full URL for the EOS HTTPS service. |
| **`authkey`** | The authentication key (token) used to authorize requests to both the gRPC and HTTP endpoints. |
| **`compute_md5`** | A boolean (`true` or `false`). If set to `true`, the gateway will compute the MD5 checksum for uploaded objects. |
| **`buckets.driver`** | Specifies how bucket metadata should be stored. `local` uses the local filesystem. |
| **`buckets.folder`** | If `driver` is `local`, this is the absolute path to the directory where bucket configuration files will be stored. |

## Usage

## Contributing

## Licence

