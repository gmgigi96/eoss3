# EOS plugin for Versity S3 gateway

This project contains the EOS backend plugin for the Versity S3 Gateway. It allows the Versity S3 Gateway to use CERN's EOS as a storage backend.

The plugin acts as a highly efficient translation layer. It converts S3 API calls into corresponding operations on the EOS distributed filesystem by using:
* **gRPC**: for all metadata operations (listing, stat, delete, etc.).
* **HTTP**: for all high-throughput data operations (uploading/downloading objects).

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
go install github.com/versity/versitygw/cmd/versitygw@latest
```

2. **Enable the HTTP Interface on EOS**
Ensure that your EOS instance has the HTTP interface enabled, following [this](https://eos-docs.web.cern.ch/diopside/manual/protocols.html?highlight=http) doc.

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

4. **Configure synchronous alternative checksums computation**
```
eos space config default space.altxs=on
```

> [!WARNING]
> Feature supported in EOS 5.4+

## Build
To build the plugin and automatically install all necessary dependencies (including fetching the correct version of `versitygw`), simply run:
```bash
make
```

If you also need to build the CLI for the plugin, run:
```bash
make cli
```

## Configuration

The EOS S3 plugin is configured using a YAML file. By default, the plugin will look for this file at `/etc/eoss3/plugin.yaml`.

Here is an example configuration file

```yaml
--
grpc_url: "eospilot.cern.ch:50051"
http_url: "https://eospilot.cern.ch:8444"
authkey: "secret"
insecure: false

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
| **`insecure`** | If true disables transport security when connecting to EOS. |
| **`buckets.driver`** | Specifies how bucket metadata should be stored. `local` uses the local filesystem. |
| **`buckets.folder`** | If `driver` is `local`, this is the absolute path to the directory where bucket configuration files will be stored. |

## Usage

## Contributing
Contributions are welcome! If you'd like to improve the EOS plugin for Versity S3 gateway, please follow these steps:
  1. Fork the repository.
  2. Create a new branch for your feature or bugfix (`git checkout -b feature/my-new-feature`).
  3. Commit your changes (`git commit -am 'Add some feature'`).
  4. Push to the branch (`git push origin feature/my-new-feature`).
  5. Open a Pull Request.

For major changes, please open an issue first to discuss what you would like to modify.

## Licence
This project is licensed under the Apache License 2.0 - see the [LICENSE](./LICENSE) file for details.
